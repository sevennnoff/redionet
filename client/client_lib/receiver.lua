--[[
    Receiver module
    Timeline playback: buffer pushed chunks, play at server play_at_ms.
]]

local speaker = peripheral.find("speaker")

local dbgmon = function() end
local WAIT_TICK = 0.05
local LATE_GRACE_MS = 80

local timeline = nil
local pending = {}
local next_play_id = 1
local clock_offset_ms = 0
local play_timer = nil

local function server_now_ms()
    return os.epoch("local") + clock_offset_ms
end

local function sync_clock(server_time_ms)
    if server_time_ms then
        clock_offset_ms = server_time_ms - os.epoch("local")
    end
end

local function debug_init()
    settings.load()
    local monitor = peripheral.find("monitor")

    if monitor and settings.get('redionet.log_level', 3) == 1 then
        local pp = require('cc.pretty')
        monitor.setTextScale(0.5)

        dbgmon = function(message)
            if type(message) == "table" then
                message = pp.render(pp.pretty(message))
            end

            local time_ms = os.epoch("local")
            local time_ms_fmt = ('%s,%03d'):format(os.date("%H:%M:%S", time_ms / 1000), time_ms % 1000)
            local prev_term = term.redirect(monitor)
            print(("[DBG] (%s) %s"):format(time_ms_fmt, message))
            term.redirect(prev_term)
        end
    end
end

local M = {}

function M.server_now_ms()
    return server_now_ms()
end

function M.sync_clock(server_time_ms)
    sync_clock(server_time_ms)
end

function M.on_timeline_flush()
    if not timeline then return end
    speaker.stop()
    pending = {}
    local pos_ms = server_now_ms() - timeline.origin_ms
    next_play_id = math.max(timeline.start_chunk_id or 1, math.floor(pos_ms / timeline.chunk_ms) + 1)
    os.queueEvent("redionet:advance_playback")
end

function M.update_server_state(blocking)
    if blocking then
        rednet.send(SERVER_ID, {"STATE", nil}, REDIONET_PROTO.SERVER_PLAYER)
        local id, server_state = rednet.receive(REDIONET_PROTO.SERVER_STATE)
        CSTATE.server_state = server_state
        CSTATE.is_authorized = server_state.controller_id == CLIENT_ID
        CSTATE.state_received_epoch_ms = os.epoch("local")
        sync_clock(server_state.server_time_ms)
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
    rednet.send(SERVER_ID, {code, result}, REDIONET_PROTO.SERVER_QUEUE)
    os.queueEvent('redionet:sync_state')
    return true
end

function M.send_server_player(code, loop_mode)
    if code ~= "STATE" and not can_control() then return false end
    rednet.send(SERVER_ID, {code, loop_mode}, REDIONET_PROTO.SERVER_PLAYER)
    os.queueEvent('redionet:sync_state')
    return true
end

function M.send_server_volume(volume)
    return M.send_server_player("VOLUME", volume)
end

function M.send_server_bass(bass)
    return M.send_server_player("BASS", bass)
end

function M.send_server_sync()
    if not can_control() then return false end
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
            speaker.stop()
            pending = {}
        end
    end
end

local function reset_playback()
    timeline = nil
    pending = {}
    next_play_id = 1
    if play_timer then
        os.cancelTimer(play_timer)
        play_timer = nil
    end
    if speaker then speaker.stop() end
end

local function apply_timeline(map)
    reset_playback()
    sync_clock(map.server_time_ms)
    timeline = map
    next_play_id = map.start_chunk_id or 1
    dbgmon(("timeline origin in %dms, from chunk %d"):format(
        map.origin_ms - server_now_ms(), next_play_id))
    os.queueEvent("redionet:advance_playback")
end

local function schedule_play_timer(delay_sec)
    if play_timer then os.cancelTimer(play_timer) end
    play_timer = os.startTimer(math.max(WAIT_TICK, delay_sec or WAIT_TICK))
end

local function play_pcm(buffer, volume)
    while not speaker.playAudio(buffer, volume) do
        parallel.waitForAny(
            function() os.pullEvent("speaker_audio_empty") end,
            function() os.pullEvent("redionet:timeline_flush") end,
            function() os.pullEvent("redionet:playback_stopped") end
        )
        if not timeline or CSTATE.is_paused then return false end
    end
    return true
end

local function advance_playback()
    if not timeline or not speaker or CSTATE.is_paused then return end
    if CSTATE.server_state.status ~= 1 then return end

    play_timer = nil

    while true do
        local entry = pending[next_play_id]
        if not entry then
            schedule_play_timer(WAIT_TICK)
            return
        end

        local play_at = timeline.origin_ms + (next_play_id - 1) * timeline.chunk_ms
        local end_at = play_at + timeline.chunk_ms + LATE_GRACE_MS
        local now = server_now_ms()

        if now > end_at then
            dbgmon(("skip late chunk %d by %dms"):format(next_play_id, now - end_at))
            pending[next_play_id] = nil
            next_play_id = next_play_id + 1
        elseif now < play_at then
            schedule_play_timer((play_at - now) / 1000)
            return
        else
            local sub = entry.sub_state
            if sub.volume then CSTATE.server_state.volume = sub.volume end
            if sub.bass_boost ~= nil then CSTATE.server_state.bass_boost = sub.bass_boost end
            local volume = sub.volume or CSTATE.server_state.volume or 1.5

            if play_pcm(entry.buffer, volume) then
                os.queueEvent("redionet:audio_timestamp", (next_play_id - 1) * timeline.chunk_ms / 1000)
            end
            pending[next_play_id] = nil
            next_play_id = next_play_id + 1
        end
    end
end

function M.receive_loop()
    if not speaker then
        while true do
            local _, side = os.pullEvent('peripheral')
            if peripheral.hasType(side, 'speaker') then
                os.queueEvent('redionet:reload')
            end
        end
    end

    debug_init()

    rednet.send(SERVER_ID, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)
    os.queueEvent('redionet:sync_state')

    while true do
        parallel.waitForAny(
            function()
                while true do
                    local _, map = rednet.receive(REDIONET_PROTO.AUDIO_TIMELINE)
                    apply_timeline(map)
                end
            end,

            function()
                while true do
                    local _, message = rednet.receive(REDIONET_PROTO.AUDIO)
                    if CSTATE.is_paused or not timeline then
                        -- drop while paused / no map
                    else
                        local buffer, sub_state = table.unpack(message)
                        if sub_state
                            and sub_state.active_stream_id == timeline.active_stream_id
                            and sub_state.chunk_id then
                            pending[sub_state.chunk_id] = { buffer = buffer, sub_state = sub_state }
                            os.queueEvent("redionet:advance_playback")
                        end
                    end
                end
            end,

            function()
                while true do
                    local ev = os.pullEvent()
                    if ev == "redionet:advance_playback"
                        or ev == "redionet:timeline_flush"
                        or ev == "timer"
                        or ev == "redionet:sync_state" then
                        if ev == "redionet:sync_state" and CSTATE.server_state.server_time_ms then
                            sync_clock(CSTATE.server_state.server_time_ms)
                        end
                        if ev == "redionet:timeline_flush" then
                            M.on_timeline_flush()
                        else
                            advance_playback()
                        end
                    elseif ev == "redionet:playback_stopped" then
                        reset_playback()
                    end
                end
            end,

            function()
                while true do
                    rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                    reset_playback()
                    os.queueEvent("redionet:playback_stopped")
                end
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
