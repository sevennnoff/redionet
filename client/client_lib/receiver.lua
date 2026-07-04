--[[
    Receiver module
    Handles server communications and audio playback.
]]

local dfpwm = require("cc.audio.dfpwm")

local speaker = peripheral.find("speaker")
local decoder = dfpwm.make_decoder()
local expected_chunk_id = nil
local active_stream_id = nil
local LATE_FLUSH_MS = 120


local dbgmon = function (message) end

local function debug_init()
    settings.load()
    local monitor = peripheral.find("monitor")

    if (monitor and settings.get('redionet.log_level', 3) == 1) then
        local pp = require('cc.pretty')
        monitor.setTextScale(0.5)

        dbgmon = function (message)
            if type(message) == "table" then
                message = pp.render(pp.pretty(message))
            end

            local time_ms = os.epoch("local")
            local time_ms_fmt = ('%s,%03d'):format(os.date("%H:%M:%S", time_ms/1000), time_ms%1000)
            local log_msg = ("[DBG] (%s) %s"):format(time_ms_fmt, message)

            local prev_term = term.redirect(monitor)
            print(log_msg)
            term.redirect(prev_term)
        end
    end
end


local M = {}

local function reset_playback_cursor()
    expected_chunk_id = nil
    active_stream_id = nil
    decoder = dfpwm.make_decoder()
end

function M.update_server_state(blocking)
    if blocking then
        rednet.send(SERVER_ID, {"STATE", nil}, REDIONET_PROTO.SERVER_PLAYER)
        local id, server_state = rednet.receive(REDIONET_PROTO.SERVER_STATE)
        CSTATE.server_state = server_state
        CSTATE.is_authorized = server_state.controller_id == CLIENT_ID
        CSTATE.state_received_epoch_ms = os.epoch("local")
    else
        os.queueEvent('redionet:sync_state')
    end
end

function M.authenticate(password)
    rednet.send(SERVER_ID, {"AUTH", password}, REDIONET_PROTO.SERVER)
    local id, payload = rednet.receive(REDIONET_PROTO.SERVER_REPLY, 2.0)
    local code, ok
    if type(payload) == "table" then code, ok = table.unpack(payload) end

    CSTATE.is_authorized = (code == "AUTH" and ok == true)
    os.queueEvent('redionet:sync_state')
    return CSTATE.is_authorized
end

local function can_control()
    return CSTATE.is_authorized
end

function M.send_server_queue(result, code)
    if not can_control() then return false end
    CSTATE.is_paused = false
    rednet.send(SERVER_ID, {code, result},  REDIONET_PROTO.SERVER_QUEUE)
    os.queueEvent('redionet:sync_state')
    return true
end

function M.send_server_player(code, loop_mode)
    if code ~= "STATE" and not can_control() then return false end
    rednet.send(SERVER_ID, {code, loop_mode},  REDIONET_PROTO.SERVER_PLAYER)
    os.queueEvent('redionet:sync_state')
    return true
end

function M.send_server_volume(volume)
    return M.send_server_player("VOLUME", volume)
end

function M.send_server_sync()
    return M.send_server_player("SYNC")
end

function M.toggle_play_local()
    if CSTATE.is_paused or CSTATE.is_paused == nil then
        CSTATE.is_paused = false
        local status = speaker and 1 or -1
        rednet.send(SERVER_ID, status, REDIONET_PROTO.AUDIO_CONNECTION)
    else
        CSTATE.is_paused = true
        if speaker then
            rednet.send(SERVER_ID, 0, REDIONET_PROTO.AUDIO_CONNECTION)
            os.queueEvent("redionet:playback_stopped")
            speaker.stop()
         end
    end
end

local function wait_until_play_at(play_at_ms, state)
    if not play_at_ms then return true end

    while true do
        local remaining_ms = play_at_ms - os.epoch("local")
        if remaining_ms <= 0 then return true end

        parallel.waitForAny(
            function() os.sleep(remaining_ms / 1000) end,
            function()
                os.pullEvent("redionet:playback_stopped")
                state.active_stream_id = "HALT"
            end
        )

        if CSTATE.is_paused or state.active_stream_id ~= state.song_id then
            return false
        end
    end
end

local function decode_chunk(encoded)
    if type(encoded) == "table" then
        return encoded
    end
    return decoder(encoded)
end

---@return string ack message for server
local function play_audio(encoded, state)
    if not encoded or CSTATE.is_paused or state.active_stream_id ~= state.song_id then
        return "playback_stopped"
    end

    if active_stream_id ~= state.active_stream_id then
        active_stream_id = state.active_stream_id
        expected_chunk_id = state.chunk_id
        decoder = dfpwm.make_decoder()
    end

    if expected_chunk_id == nil then
        expected_chunk_id = state.chunk_id
    end

    if state.chunk_id < expected_chunk_id then
        dbgmon(('DROP duplicate chunk %d (expected %d)'):format(state.chunk_id, expected_chunk_id))
        return "request_next_chunk"
    end

    if state.chunk_id > expected_chunk_id then
        dbgmon(('GAP reset at chunk %d (expected %d)'):format(state.chunk_id, expected_chunk_id))
        speaker.stop()
        decoder = dfpwm.make_decoder()
        expected_chunk_id = state.chunk_id
    end

    local now_ms = os.epoch("local")
    if state.play_at_ms and now_ms > state.play_at_ms + LATE_FLUSH_MS then
        dbgmon(('LATE flush chunk %d (%dms)'):format(state.chunk_id, now_ms - state.play_at_ms))
        speaker.stop()
    end

    if not wait_until_play_at(state.play_at_ms, state) then
        return "playback_stopped"
    end

    local volume = CSTATE.server_state.volume or 1.5
    local buffer = decode_chunk(encoded)
    dbgmon(('- %ds - chunk: %d, song: %s'):format(state.audio_position_sec, state.chunk_id, state.song_id))

    while not speaker.playAudio(buffer, volume) do
        parallel.waitForAny(
            function() os.pullEvent("speaker_audio_empty") end,
            function()
                os.pullEvent("redionet:playback_stopped")
                state.active_stream_id = "HALT"
            end
        )
        if CSTATE.is_paused or state.active_stream_id ~= state.song_id then
            return "playback_stopped"
        end
    end

    expected_chunk_id = state.chunk_id + 1
    return "request_next_chunk"
end


function M.receive_loop()
    if not speaker then
        while true do
            local ev, side = os.pullEvent('peripheral')
            if peripheral.hasType(side, 'speaker') then
                os.queueEvent('redionet:reload')
            end
        end
    end

    debug_init()

    local id, message

    rednet.send(SERVER_ID, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)

    while true do
        parallel.waitForAny(
            function ()
                id, message = rednet.receive(REDIONET_PROTO.AUDIO)

                if CSTATE.is_paused then
                    rednet.send(id, "playback_stopped", REDIONET_PROTO.AUDIO_NEXT)
                else
                    local encoded, sub_state = table.unpack(message)
                    local ack = play_audio(encoded, sub_state)
                    rednet.send(id, (not CSTATE.is_paused) and ack or "playback_stopped", REDIONET_PROTO.AUDIO_NEXT)
                end
            end,

            function ()
                id, message = rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                reset_playback_cursor()
                speaker.stop()
                os.queueEvent("redionet:playback_stopped")
                rednet.send(id, "playback_interrupted", REDIONET_PROTO.AUDIO_NEXT)
            end,

            function ()
                while true do
                    os.pullEvent("redionet:playback_stopped")
                    reset_playback_cursor()
                end
            end,

            function ()
                while true do
                    local sid, _ = rednet.receive(REDIONET_PROTO.AUDIO_STATUS)
                    rednet.send(sid, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)
                end
            end
        )
    end
end

return M
