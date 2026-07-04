--[[
    Audio module
    Timeline conductor — clients download and play locally from the API.
]]

local chat = require('server_lib.chat')

local AUDIO_CHUNK_SEC = 2.70
local TICK = 0.050
local START_LEAD_BASE_MS = 2000
local START_LEAD_PER_CLIENT_MS = 700
local RESYNC_LEAD_MS = 900
local TIMELINE_SETTLE_SEC = 0.05
local STATE_BROADCAST_TICK = 0.20
local BUFFER_READY_BASE_MS = 15000
local BUFFER_READY_PER_CLIENT_MS = 5000
local SYNC_RETRY_SEC = 0.40

local M = {}

M.state = {
    receiver_stats = {},
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
    timeline_origin_ms = nil,
    clients_ready = {},
    clients_failed = {},
    waiting_buffer = false,
    playback_token = 0,
    session_id = 0,
}

local function song_duration_sec(meta)
    if not meta or not meta.duration then return 0 end
    local d = meta.duration
    return d.H * 3600 + d.M * 60 + d.S
end

local function active_listener_count()
    local n = 0
    for _, status in pairs(M.state.receiver_stats) do
        if status == 1 then n = n + 1 end
    end
    return n
end

local function ready_count()
    local ready = 0
    for id, status in pairs(M.state.receiver_stats) do
        if status == 1 and M.state.clients_ready[id] then
            ready = ready + 1
        end
    end
    return ready
end

local function all_active_clients_ready()
    local expected = active_listener_count()
    if expected == 0 then return true end
    return ready_count() >= expected
end

local function start_lead_ms(extra_ms)
    local n = math.max(1, active_listener_count())
    return START_LEAD_BASE_MS + n * START_LEAD_PER_CLIENT_MS + (extra_ms or 0)
end

local function buffer_ready_timeout_ms()
    local n = math.max(1, active_listener_count())
    return BUFFER_READY_BASE_MS + n * BUFFER_READY_PER_CLIENT_MS
end

local function broadcast_prepare()
    rednet.broadcast({
        kind = "prepare",
        stream_id = STATE.active_stream_id,
        song_id = STATE.active_stream_id,
        session_id = M.state.session_id,
    }, REDIONET_PROTO.CLIENT_SYNC)
end

---@return boolean ready_enough
---@return number missing_clients
local function wait_for_client_buffers(token)
    M.state.waiting_buffer = true
    M.state.clients_ready = {}
    M.state.clients_failed = {}
    broadcast_prepare()

    local settle = os.startTimer(0.15)
    repeat
        local _, tid = os.pullEvent("timer")
    until tid == settle
    os.cancelTimer(settle)

    os.queueEvent('redionet:broadcast_state', 'await buffer_ready')

    local deadline = os.epoch("local") + buffer_ready_timeout_ms()
    while os.epoch("local") < deadline do
        if STATE.data.status ~= 1 or M.state.playback_token ~= token then
            M.state.waiting_buffer = false
            return false, active_listener_count()
        end
        if all_active_clients_ready() then
            M.state.waiting_buffer = false
            return true, 0
        end
        os.sleep(TICK)
    end

    M.state.waiting_buffer = false
    local expected = active_listener_count()
    local missing = math.max(0, expected - ready_count())
    chat.log_message(('buffer_ready timeout (%d/%d ready, %d failed)'):format(
        ready_count(), expected, (function()
            local n = 0
            for _ in pairs(M.state.clients_failed) do n = n + 1 end
            return n
        end)()), 'WARN')
    return ready_count() > 0, missing
end

function M.arm_timeline(is_new_stream, chunk_id, audio_position_sec, extra_lead_ms)
    local lead_ms = (is_new_stream and start_lead_ms(extra_lead_ms) or RESYNC_LEAD_MS)
    local position_ms = math.floor((audio_position_sec or 0) * 1000 + 0.5)
    local start_at_ms = os.epoch("local") + lead_ms
    M.state.timeline_origin_ms = start_at_ms - position_ms
    STATE.data.timeline_origin_ms = M.state.timeline_origin_ms
    STATE.data.audio_position_sec = audio_position_sec or 0
    STATE.audio_position_epoch_ms = os.epoch("local")
    STATE.data.audio_position_epoch_ms = STATE.audio_position_epoch_ms
    STATE.data.server_time_ms = os.epoch("local")

    chat.log_message(('Timeline s%d: %d clients, start in %dms'):format(
        M.state.session_id, active_listener_count(), lead_ms), 'INFO')

    rednet.broadcast({
        kind = "timeline",
        anchor_ms = start_at_ms,
        start_at_ms = start_at_ms,
        timeline_origin_ms = M.state.timeline_origin_ms,
        stream_id = STATE.active_stream_id,
        session_id = M.state.session_id,
        chunk_id = chunk_id or 1,
        audio_position_sec = audio_position_sec or 0,
        server_time_ms = STATE.data.server_time_ms,
    }, REDIONET_PROTO.CLIENT_SYNC)

    local settle_timer = os.startTimer(TIMELINE_SETTLE_SEC)
    repeat
        local _, tid = os.pullEvent("timer")
    until tid == settle_timer
    os.cancelTimer(settle_timer)
end

local function update_timeline_position()
    if M.state.timeline_origin_ms and STATE.data.status == 1 then
        STATE.data.audio_position_sec = math.max(0, (os.epoch("local") - M.state.timeline_origin_ms) / 1000)
        STATE.audio_position_epoch_ms = os.epoch("local")
        STATE.data.audio_position_epoch_ms = STATE.audio_position_epoch_ms
        STATE.data.server_time_ms = os.epoch("local")
    end
end

local function try_arm_timeline(is_new_stream, token)
    local ready, missing = wait_for_client_buffers(token)
    if not ready or STATE.data.status ~= 1 or M.state.playback_token ~= token then
        return false
    end

    local pos = STATE.data.audio_position_sec or 0
    local chunk_id = math.max(1, math.floor(pos / AUDIO_CHUNK_SEC) + 1)
    local extra = missing * 800
    M.arm_timeline(is_new_stream, chunk_id, pos, extra)
    M.state.clients_ready = {}
    os.queueEvent('redionet:broadcast_state', 'arm_timeline')
    return true
end

local function run_timeline_playback(song_meta)
    M.state.playback_token = M.state.playback_token + 1
    M.state.session_id = M.state.session_id + 1
    local token = M.state.playback_token

    STATE.active_stream_id = song_meta.id
    STATE.data.audio_position_sec = 0
    STATE.data.timeline_origin_ms = nil
    M.state.timeline_origin_ms = nil
    M.state.need_sync = true
    M.state.clients_ready = {}
    M.state.clients_failed = {}

    local duration = song_duration_sec(song_meta)
    chat.announce_song(song_meta.artist, song_meta.name)

    local last_broadcast = 0
    local synced_once = false

    while STATE.data.status == 1
        and STATE.active_stream_id == song_meta.id
        and M.state.playback_token == token do

        if M.state.need_sync then
            if try_arm_timeline(not synced_once, token) then
                synced_once = true
                M.state.need_sync = false
            else
                os.sleep(SYNC_RETRY_SEC)
            end
        end

        update_timeline_position()

        local now = os.epoch("local")
        if now - last_broadcast >= STATE_BROADCAST_TICK * 1000 then
            os.queueEvent('redionet:broadcast_state', 'timeline tick')
            last_broadcast = now
        end

        if STATE.data.audio_position_sec >= duration then
            break
        end

        os.sleep(TICK)
    end

    local completed = STATE.data.status == 1 and M.state.playback_token == token
    if M.state.playback_token == token then
        STATE.active_stream_id = nil
    end
    return completed
end

local function set_state_queue_empty()
    if STATE.data.status ~= 0 then
        STATE.data.status = -1
    end

    STATE.data.active_song_meta = nil
    STATE.data.audio_position_sec = 0
    STATE.data.timeline_origin_ms = nil
    STATE.audio_position_epoch_ms = nil
    STATE.data.audio_position_epoch_ms = nil
    STATE.data.server_time_ms = nil

    STATE.data.is_loading = false
    STATE.data.error_status = false
    STATE.active_stream_id = nil
end

local function advance_queue()
    if STATE.data.loop_mode > 0 and STATE.data.active_song_meta then
        if STATE.data.loop_mode == 1 then
            table.insert(STATE.data.queue, STATE.data.active_song_meta)
        elseif STATE.data.loop_mode == 2 then
            table.insert(STATE.data.queue, 1, STATE.data.active_song_meta)
        end
    end

    local up_next

    if #STATE.data.queue > 0 then
        up_next = STATE.data.queue[1]
        table.remove(STATE.data.queue, 1)
    else
        set_state_queue_empty()
    end

    return up_next
end

function M.play_song(song_meta)
    M.state.playback_token = M.state.playback_token + 1

    if song_meta and song_meta.id then
        if STATE.active_stream_id and STATE.active_stream_id ~= song_meta.id then
            M.stop_song()
        end

        STATE.data.active_song_meta = song_meta
        STATE.data.audio_position_sec = 0
        STATE.audio_position_epoch_ms = nil
        STATE.data.audio_position_epoch_ms = nil
        STATE.data.timeline_origin_ms = nil
        M.state.timeline_origin_ms = nil
    end

    STATE.data.status = 1
    os.queueEvent("redionet:fetch_audio")
end

function M.stop_song()
    M.state.playback_token = M.state.playback_token + 1
    M.state.need_sync = false
    M.state.waiting_buffer = false
    M.state.clients_ready = {}
    M.state.clients_failed = {}
    rednet.broadcast({ kind = "stop", session_id = M.state.session_id }, REDIONET_PROTO.CLIENT_SYNC)
    rednet.broadcast("audio.stop_song", REDIONET_PROTO.AUDIO_HALT)
    os.queueEvent("redionet:playback_stopped")
    STATE.active_stream_id = nil
    STATE.data.status = 0
    STATE.data.audio_position_sec = 0
    STATE.data.timeline_origin_ms = nil
    M.state.timeline_origin_ms = nil
    STATE.audio_position_epoch_ms = os.epoch("local")
    STATE.data.audio_position_epoch_ms = STATE.audio_position_epoch_ms
    STATE.data.server_time_ms = os.epoch("local")
    os.queueEvent('redionet:broadcast_state', 'stop_song')
end

function M.skip_song()
    M.state.playback_token = M.state.playback_token + 1
    local up_next_meta = advance_queue()
    M.play_song(up_next_meta)
end

function M.toggle_play_pause()
    if STATE.data.status < 1 then
        M.play_song()
    else
        M.stop_song()
    end
end

function M.audio_loop()
    parallel.waitForAny(
        function()
            while true do
                local id, status = rednet.receive(REDIONET_PROTO.AUDIO_CONNECTION)

                if status == -1 then
                    M.state.need_sync = true
                else
                    if not M.state.receiver_stats[id] then
                        M.state.n_receivers = M.state.n_receivers + 1
                    end

                    if M.state.receiver_stats[id] ~= status then
                        if status == 1 then
                            M.state.need_sync = true
                            M.state.clients_ready[id] = nil
                            M.state.clients_failed[id] = nil
                            M.state.num_active = M.state.num_active + 1
                        else
                            M.state.clients_ready[id] = nil
                            M.state.clients_failed[id] = nil
                            M.state.num_active = M.state.num_active - 1
                        end
                        M.state.receiver_stats[id] = status
                    end
                end
            end
        end,

        function()
            while true do
                local id, msg = rednet.receive(REDIONET_PROTO.AUDIO_NEXT)
                if msg == "buffer_ready" then
                    M.state.clients_ready[id] = true
                    M.state.clients_failed[id] = nil
                elseif msg == "buffer_failed" then
                    M.state.clients_failed[id] = true
                    chat.log_message(('Client #%d download failed'):format(id), 'WARN')
                    if STATE.data.status == 1 and not M.state.waiting_buffer then
                        M.state.need_sync = true
                    end
                elseif msg == "playback_stalled" then
                    chat.log_message(('Client #%d playback stalled'):format(id), 'WARN')
                    if STATE.data.status == 1 and not M.state.waiting_buffer then
                        M.state.need_sync = true
                    end
                end
            end
        end,

        function()
            local event_filter = {
                ["redionet:fetch_audio"] = true,
                ["redionet:playback_stopped"] = true,
            }
            while true do
                local eventData = { os.pullEvent() }
                local event = eventData[1]

                if event_filter[event] then
                    if STATE.data.active_song_meta == nil then
                        STATE.data.active_song_meta = advance_queue()
                    end

                    local can_play = STATE.data.active_song_meta ~= nil
                    local should_play = STATE.data.status ~= 0

                    if not can_play then
                        event = "redionet:event_cancelled"
                    end

                    os.queueEvent('redionet:broadcast_state', "audio_loop - ".. event)

                    if event == "redionet:fetch_audio" then
                        if should_play and STATE.data.active_song_meta then
                            local song_meta = STATE.data.active_song_meta
                            local song_completed = run_timeline_playback(song_meta)
                            if song_completed then
                                STATE.data.active_song_meta = advance_queue()
                            end
                            os.queueEvent('redionet:fetch_audio')
                        end

                    elseif event == "redionet:playback_stopped" then
                        STATE.active_stream_id = nil
                        STATE.data.is_loading = false
                        STATE.data.error_status = eventData[2] or false
                        if STATE.data.error_status then
                            chat.log_message(("%s: %s"):format(STATE.data.error_status, eventData[3] or "Unknown"), "ERROR")
                        end
                    end
                end
            end
        end
    )
end

return M
