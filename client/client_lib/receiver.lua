--[[
    Receiver module
    Handles server communications and timeline-synced audio playback.
]]

local dfpwm = require("cc.audio.dfpwm")

local speaker = peripheral.find("speaker")

local MAX_QUEUE = 12
local LATE_TOLERANCE_MS = 1200

local play_queue = {}
local expected_chunk_id = 0
local timeline_stream_id = nil
local timeline_origin_ms = nil
local decoder = dfpwm.make_decoder()

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

local function chunk_play_at_ms(state)
    if state.play_at_ms then
        return state.play_at_ms
    end
    if timeline_origin_ms and state.audio_position_sec then
        return timeline_origin_ms + math.floor(state.audio_position_sec * 1000 + 0.5)
    end
end

local function reset_playback(stream_id, chunk_id, origin_ms)
    speaker.stop()
    play_queue = {}
    expected_chunk_id = chunk_id or 1
    decoder = dfpwm.make_decoder()
    timeline_stream_id = stream_id
    timeline_origin_ms = origin_ms
end

local M = {}

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

local function play_audio(buffer, state)
    if not buffer or CSTATE.is_paused or state.active_stream_id ~= state.song_id then return end

    local volume = CSTATE.server_state.volume or 1.5
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

local function wait_until_play_at(play_at_ms, stream_id)
    if not play_at_ms then return true end

    while true do
        local now_ms = os.epoch("local")
        if now_ms >= play_at_ms then
            return timeline_stream_id == nil or timeline_stream_id == stream_id
        end

        local wait_sec = (play_at_ms - now_ms) / 1000
        local timer_id = os.startTimer(wait_sec)
        local proceed = true
        parallel.waitForAny(
            function()
                repeat
                    local _, tid = os.pullEvent("timer")
                until tid == timer_id
            end,
            function()
                os.pullEvent("redionet:playback_stopped")
                proceed = false
            end,
            function()
                os.pullEvent("redionet:timeline_anchor")
                proceed = false
            end
        )
        os.cancelTimer(timer_id)

        if not proceed or CSTATE.is_paused then
            return false
        end
    end
end

local function enqueue_chunk(server_id, encoded, state)
    if CSTATE.is_paused then
        rednet.send(server_id, "playback_stopped", REDIONET_PROTO.AUDIO_NEXT)
        return
    end

    if state.active_stream_id ~= state.song_id then return end

    if timeline_stream_id and state.active_stream_id ~= timeline_stream_id then
        reset_playback(state.active_stream_id, state.chunk_id, state.timeline_origin_ms)
    end

    if state.timeline_origin_ms then
        timeline_origin_ms = state.timeline_origin_ms
    end

    rednet.send(server_id, "chunk_received", REDIONET_PROTO.AUDIO_NEXT)

    if expected_chunk_id == 0 then
        expected_chunk_id = state.chunk_id
    end

    if state.chunk_id ~= expected_chunk_id then
        dbgmon(('chunk gap: expected %d got %d'):format(expected_chunk_id, state.chunk_id))
        rednet.send(server_id, "playback_desync", REDIONET_PROTO.AUDIO_NEXT)
        return
    end

    local play_at_ms = chunk_play_at_ms(state)
    local recv_ms = os.epoch("local")
    if play_at_ms and recv_ms > play_at_ms + LATE_TOLERANCE_MS then
        dbgmon(('late chunk %d by %dms'):format(state.chunk_id, recv_ms - play_at_ms))
        rednet.send(server_id, "playback_desync", REDIONET_PROTO.AUDIO_NEXT)
        return
    end

    while #play_queue >= MAX_QUEUE do
        os.sleep(0.05)
    end

    table.insert(play_queue, {
        encoded = encoded,
        state = state,
        play_at_ms = play_at_ms,
    })
    expected_chunk_id = state.chunk_id + 1
    os.queueEvent("redionet:audio_queued")
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

    rednet.send(SERVER_ID, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)

    while true do
        parallel.waitForAny(
            function()
                while true do
                    local id, message = rednet.receive(REDIONET_PROTO.AUDIO)
                    local encoded, sub_state = table.unpack(message)
                    enqueue_chunk(id, encoded, sub_state)
                end
            end,

            function()
                while true do
                    local _, anchor_ms, stream_id, chunk_id, origin_ms = os.pullEvent("redionet:timeline_anchor")
                    reset_playback(stream_id, chunk_id, origin_ms)
                    dbgmon(('timeline anchor pos=%0.3fs chunk=%s'):format(
                        (anchor_ms - (origin_ms or anchor_ms)) / 1000, tostring(chunk_id)))
                end
            end,

            function()
                while true do
                    os.pullEvent("redionet:audio_queued")
                    while #play_queue > 0 do
                        local item = play_queue[1]
                        if CSTATE.is_paused or item.state.active_stream_id ~= item.state.song_id then
                            table.remove(play_queue, 1)
                        else
                            if wait_until_play_at(item.play_at_ms, item.state.active_stream_id) then
                                table.remove(play_queue, 1)
                                if not CSTATE.is_paused and item.state.active_stream_id == item.state.song_id
                                    and (timeline_stream_id == nil or timeline_stream_id == item.state.active_stream_id) then
                                    play_audio(decoder(item.encoded), item.state)
                                end
                            else
                                table.remove(play_queue, 1)
                            end
                        end
                    end
                end
            end,

            function()
                local id, message = rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                play_queue = {}
                expected_chunk_id = 0
                timeline_origin_ms = nil
                speaker.stop()
                os.queueEvent("redionet:playback_stopped")
                rednet.send(id, "playback_interrupted", REDIONET_PROTO.AUDIO_NEXT)
            end,

            function()
                while true do
                    local sid, _ = rednet.receive(REDIONET_PROTO.AUDIO_STATUS)
                    rednet.send(sid, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)
                end
            end
        )
    end
end

return M
