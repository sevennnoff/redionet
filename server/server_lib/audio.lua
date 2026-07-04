--[[
    Audio module
    Timeline conductor — clients download and play locally from the API.
]]

local chat = require('server_lib.chat')

local AUDIO_CHUNK_SEC = 2.70
local TICK = 0.050
local START_LEAD_MS = 1200
local RESYNC_LEAD_MS = 600
local TIMELINE_SETTLE_SEC = 0.05
local STATE_BROADCAST_TICK = 0.25

local M = {}

M.state = {
    receiver_stats = {},
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
    timeline_origin_ms = nil,
}

local function song_duration_sec(meta)
    if not meta or not meta.duration then return 0 end
    local d = meta.duration
    return d.H * 3600 + d.M * 60 + d.S
end

--- Broadcast timeline anchor; clients seek to chunk_id / audio_position_sec.
function M.arm_timeline(is_new_stream, chunk_id, audio_position_sec)
    local lead_ms = is_new_stream and START_LEAD_MS or RESYNC_LEAD_MS
    local position_ms = math.floor((audio_position_sec or 0) * 1000 + 0.5)
    M.state.timeline_origin_ms = os.epoch("local") + lead_ms - position_ms
    STATE.data.timeline_origin_ms = M.state.timeline_origin_ms
    STATE.data.audio_position_sec = audio_position_sec or 0
    STATE.audio_position_epoch_ms = os.epoch("local")
    STATE.data.audio_position_epoch_ms = STATE.audio_position_epoch_ms

    rednet.broadcast({
        kind = "timeline",
        anchor_ms = M.state.timeline_origin_ms + position_ms,
        timeline_origin_ms = M.state.timeline_origin_ms,
        stream_id = STATE.active_stream_id,
        chunk_id = chunk_id or 1,
        audio_position_sec = audio_position_sec or 0,
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
    end
end

local function run_timeline_playback(song_meta)
    STATE.active_stream_id = song_meta.id
    STATE.data.audio_position_sec = 0
    STATE.data.timeline_origin_ms = nil
    M.state.timeline_origin_ms = nil
    M.state.need_sync = true

    local duration = song_duration_sec(song_meta)
    chat.announce_song(song_meta.artist, song_meta.name)

    local last_broadcast = 0

    while STATE.data.status == 1 and STATE.active_stream_id == song_meta.id do
        if M.state.need_sync then
            local pos = STATE.data.audio_position_sec or 0
            local chunk_id = math.max(1, math.floor(pos / AUDIO_CHUNK_SEC) + 1)
            M.arm_timeline(pos < 0.01, chunk_id, pos)
            M.state.need_sync = false
            os.queueEvent('redionet:broadcast_state', 'arm_timeline')
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

    local completed = STATE.data.status == 1
    STATE.active_stream_id = nil
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
    if song_meta and song_meta.id then
        if STATE.active_stream_id and STATE.active_stream_id ~= song_meta.id then
            M.stop_song()
        end

        STATE.data.active_song_meta = song_meta
        STATE.data.audio_position_sec = 0
        STATE.audio_position_epoch_ms = nil
        STATE.data.audio_position_epoch_ms = nil
    end

    STATE.data.status = 1
    os.queueEvent("redionet:fetch_audio")
end

function M.stop_song()
    rednet.broadcast("audio.stop_song", REDIONET_PROTO.AUDIO_HALT)
    os.queueEvent("redionet:playback_stopped")
    STATE.active_stream_id = nil
    STATE.data.status = 0
    STATE.data.audio_position_sec = 0
    STATE.data.timeline_origin_ms = nil
    STATE.audio_position_epoch_ms = nil
    STATE.data.audio_position_epoch_ms = nil
end

function M.skip_song()
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
                            M.state.num_active = M.state.num_active + 1
                        else
                            M.state.num_active = M.state.num_active - 1
                        end
                        M.state.receiver_stats[id] = status
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
