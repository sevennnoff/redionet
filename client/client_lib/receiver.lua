--[[
    Receiver module
    Handles server communications and audio playback.
]]

local speaker = peripheral.find("speaker")
local REDIONET_VERSION = require("lib.version")

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

function M.reset_stream()
    if speaker then speaker.stop() end
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

function M.send_server_bass(bass)
    return M.send_server_player("BASS", bass)
end

local function send_audio_connection(status)
    rednet.send(SERVER_ID, {status, REDIONET_VERSION}, REDIONET_PROTO.AUDIO_CONNECTION)
end

local function send_chunk_ack(server_id, chunk_id, kind)
    rednet.send(server_id, {chunk_id, kind}, REDIONET_PROTO.AUDIO_NEXT)
end

function M.send_server_sync()
    if not can_control() then return false end
    return M.send_server_player("SYNC")
end

function M.toggle_play_local()
    if CSTATE.is_paused or CSTATE.is_paused == nil then
        CSTATE.is_paused = false
        local status = speaker and 1 or -1
        send_audio_connection(status)
    else
        CSTATE.is_paused = true
        if speaker then
            send_audio_connection(0)
            os.queueEvent("redionet:playback_stopped")
            speaker.stop()
         end
    end
end

local function play_audio(buffer, state)
    if not buffer or CSTATE.is_paused or state.active_stream_id ~= state.song_id then return end

    if state.volume then
        CSTATE.server_state.volume = state.volume
    end
    local volume = state.volume or CSTATE.server_state.volume or 1.5
    dbgmon(('- %0.3fs - chunk: %d, song: %s, vol: %0.2f'):format(
        state.audio_position_sec, state.chunk_id, state.song_id, volume))
    os.queueEvent("redionet:audio_timestamp", state.audio_position_sec)

    while not speaker.playAudio(buffer, volume) do
        dbgmon('SPEAKER FULL')
        parallel.waitForAny(
            function()
                os.pullEvent("speaker_audio_empty")
            end,
            function()
                os.pullEvent("redionet:playback_stopped")
                state.active_stream_id = "HALT"
            end
        )
        if CSTATE.is_paused or state.active_stream_id ~= state.song_id then return end
    end
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

    send_audio_connection(CSTATE.is_paused and 0 or 1)
    os.queueEvent('redionet:sync_state')

    while true do
        parallel.waitForAny(
            function ()
                while true do
                    local id, message = rednet.receive(REDIONET_PROTO.AUDIO)

                    if CSTATE.is_paused then
                        local _, sub_state = table.unpack(message)
                        send_chunk_ack(id, sub_state.chunk_id, "playback_stopped")
                    else
                        local buffer, sub_state = table.unpack(message)
                        play_audio(buffer, sub_state)
                        send_chunk_ack(id, sub_state.chunk_id,
                            (not CSTATE.is_paused) and "request_next_chunk" or "playback_stopped")
                    end
                end
            end,

            function ()
                while true do
                    local id, message = rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                    speaker.stop()
                    os.queueEvent("redionet:playback_stopped")
                    rednet.send(id, "playback_interrupted", REDIONET_PROTO.AUDIO_NEXT)
                end
            end,

            function ()
                while true do
                    local sid, _ = rednet.receive(REDIONET_PROTO.AUDIO_STATUS)
                    rednet.send(sid, {CSTATE.is_paused and 0 or 1, REDIONET_VERSION}, REDIONET_PROTO.AUDIO_CONNECTION)
                end
            end
        )
    end
end

return M
