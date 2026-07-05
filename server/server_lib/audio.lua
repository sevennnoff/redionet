--[[
    Audio module
    Server timeline sync: broadcast chunk map at start, push PCM with play_at_ms.
]]

local dfpwm = require("cc.audio.dfpwm")

local network = require("server_lib.network")
local chat = require('server_lib.chat')
local bass_boost = require("server_lib.bass_boost")

local AUDIO_CHUNK_SEC = 2.70
local TICK = 0.050
local TIMELINE_LEAD_MS = 1200 -- prefetch before first audible chunk

local M = {}

M.state = {
    receiver_stats = {},
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
}

local function chunk_ms()
    return math.floor(AUDIO_CHUNK_SEC * 1000 + 0.5)
end

---@class Buffer
local Buffer = {}

function Buffer.new(handle, song_meta)
    local self = {
        handle = handle,
        song_meta = song_meta,
        song_id = song_meta.id or "INVALID",
        max_buffer_length = 8,
        chunk_size = (AUDIO_CHUNK_SEC * 48000) / 8,
        total_read = { bytes = 0, chunks = 0 },
        total_write = { bytes = 0, chunks = 0 },
        done_read = false,
        done_write = false,
        destroyed = false,
    }

    self.buffer = {}
    self.decoder = dfpwm.make_decoder()
    self.audio_total_sec = self.song_meta.duration.H * 3600 + self.song_meta.duration.M * 60 + self.song_meta.duration.S
    self.audio_total_chunks = math.ceil(self.audio_total_sec / AUDIO_CHUNK_SEC)

    function self:next()
        if self.done_write then return end

        if #self.buffer == 0 then
            self:read()
        end

        if self.done_read and #self.buffer == 0 then
            self.done_write = true
            self.song_id = "NULL"
            return
        end

        local next_chunk = self.buffer[1]
        table.remove(self.buffer, 1)

        self.total_write.chunks = self.total_write.chunks + 1
        self.total_write.bytes = self.total_write.bytes + #next_chunk

        chat.log_message(
            string.format('<%02d|%03d/%03d> [\25%0.1f\24%0.1f] KiB',
                #self.buffer, self.total_write.chunks, self.audio_total_chunks,
                self.total_read.bytes / 1024, self.total_write.bytes / 1024),
            "DEBUG")

        return next_chunk
    end

    function self:read()
        if self.done_read or #self.buffer > self.max_buffer_length then
            return
        end

        local ok, data = pcall(self.handle.read, self.chunk_size)
        if not ok or data == nil and self.total_read.chunks > 0 then
            self.done_read = true
            pcall(self.handle.close)
            return
        end

        table.insert(self.buffer, self.decoder(data))
        self.total_read.chunks = self.total_read.chunks + 1
        self.total_read.bytes = self.total_read.bytes + #data
    end

    function self:read_n(n)
        for _ = 1, n do self:read() end
    end

    function self:destroy()
        self.destroyed = true
        self.done_read = true
        pcall(self.handle.close)
        self.done_write = true
        self.song_id = "NULL"
        self.buffer = nil
        return nil
    end

    function self:stream_complete()
        return not self.destroyed and self.done_write and self.done_read
    end

    return self
end

local function flush_clients()
    os.queueEvent('redionet:sync')
    local sync_timer = os.startTimer(TICK * 2)
    repeat
        local _, tid = os.pullEvent('timer')
    until tid == sync_timer
    os.cancelTimer(sync_timer)
end

local function build_timeline_map(data_buffer, start_chunk_id)
    start_chunk_id = start_chunk_id or 1
    local now = os.epoch("local")
    local cms = chunk_ms()
    local origin_ms = now + TIMELINE_LEAD_MS - (start_chunk_id - 1) * cms

    return {
        stream_id = data_buffer.song_id,
        active_stream_id = STATE.active_stream_id,
        origin_ms = origin_ms,
        chunk_ms = cms,
        chunk_sec = AUDIO_CHUNK_SEC,
        start_chunk_id = start_chunk_id,
        total_chunks = data_buffer.audio_total_chunks,
        server_time_ms = now,
        volume = STATE.data.volume,
        bass_boost = STATE.data.bass_boost,
    }
end

local function broadcast_timeline(map)
    rednet.broadcast(map, REDIONET_PROTO.AUDIO_TIMELINE)
    STATE.data.timeline_origin_ms = map.origin_ms
    STATE.data.audio_position_sec = math.max(0, (map.start_chunk_id - 1) * AUDIO_CHUNK_SEC)
    STATE.audio_position_epoch_ms = os.epoch("local")
    STATE.data.audio_position_epoch_ms = STATE.audio_position_epoch_ms
    STATE.data.server_time_ms = map.server_time_ms
    os.queueEvent('redionet:broadcast_state', 'timeline map')
    chat.log_message(
        ('Timeline: origin +%dms, chunk %d/%d, lead %dms'):format(
            map.origin_ms - os.epoch("local"), map.start_chunk_id, map.total_chunks, TIMELINE_LEAD_MS),
        "INFO")
end

local function push_chunk(data_buffer, map, chunk_id, audio_chunk)
    bass_boost.process(audio_chunk, data_buffer.song_id, STATE.data.bass_boost)

    local play_at_ms = map.origin_ms + (chunk_id - 1) * map.chunk_ms
    local sub_state = {
        active_stream_id = STATE.active_stream_id,
        stream_id = data_buffer.song_id,
        chunk_id = chunk_id,
        play_at_ms = play_at_ms,
        total_chunks = map.total_chunks,
        volume = STATE.data.volume,
        bass_boost = STATE.data.bass_boost,
    }

    rednet.broadcast({ audio_chunk, sub_state }, REDIONET_PROTO.AUDIO)

    STATE.data.audio_position_sec = (chunk_id - 1) * AUDIO_CHUNK_SEC
    STATE.audio_position_epoch_ms = os.epoch("local")
    STATE.data.audio_position_epoch_ms = STATE.audio_position_epoch_ms
    STATE.data.server_time_ms = os.epoch("local")
end

local function process_audio_data(data_buffer)
    bass_boost.clear()
    M.state.need_sync = true

    local map
    local chunk_id = 0

    while STATE.active_stream_id == data_buffer.song_id and STATE.data.status == 1 do
        if M.state.need_sync or map == nil then
            flush_clients()
            local start_id = chunk_id + 1
            if chunk_id == 0 then start_id = 1 end
            map = build_timeline_map(data_buffer, start_id)
            broadcast_timeline(map)
            M.state.need_sync = false
        end

        if M.state.n_receivers == 0 then
            chat.log_message('No visible client connections... Stopping', 'WARN')
            return M.stop_song()
        end

        local audio_chunk = data_buffer:next()
        if not audio_chunk then break end

        chunk_id = chunk_id + 1
        push_chunk(data_buffer, map, chunk_id, audio_chunk)

        parallel.waitForAll(
            function() data_buffer:read_n(2) end,
            function() os.sleep(TICK) end
        )
    end

    STATE.data.timeline_origin_ms = nil
    return data_buffer:stream_complete()
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
        STATE.data.timeline_origin_ms = nil
        STATE.audio_position_epoch_ms = nil
    end

    STATE.data.status = 1
    os.queueEvent("redionet:fetch_audio")
end

function M.stop_song()
    bass_boost.clear()
    rednet.broadcast("audio.stop_song", REDIONET_PROTO.AUDIO_HALT)
    os.queueEvent("redionet:playback_stopped")
    STATE.active_stream_id = nil
    STATE.data.status = 0
    STATE.data.audio_position_sec = 0
    STATE.data.timeline_origin_ms = nil
    STATE.audio_position_epoch_ms = nil
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
                ["redionet:audio_ready"] = true,
                ["redionet:playback_stopped"] = true,
            }
            local dbuffer
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

                    os.queueEvent('redionet:broadcast_state', "audio_loop - " .. event)

                    if event == "redionet:fetch_audio" then
                        local has_data_stream = (STATE.active_stream_id ~= nil)
                        local has_correct_stream = has_data_stream and (STATE.active_stream_id == STATE.data.active_song_meta.id)

                        if should_play and not has_correct_stream then
                            rednet.broadcast('status', REDIONET_PROTO.AUDIO_STATUS)
                            network.download_song(STATE.data.active_song_meta.id)
                        end

                    elseif event == "redionet:audio_ready" then
                        local handle = STATE.response_handle
                        if not handle then error('bad state: read handle is nil', 0) end

                        if should_play then
                            chat.announce_song(STATE.data.active_song_meta.artist, STATE.data.active_song_meta.name)
                            os.queueEvent('redionet:broadcast_state', 'track start')
                            if dbuffer then
                                dbuffer = dbuffer:destroy()
                            end
                            dbuffer = Buffer.new(handle, STATE.data.active_song_meta)

                            local ok, err = pcall(process_audio_data, dbuffer)
                            if not ok then
                                os.queueEvent("redionet:playback_stopped", "PLAYBACK_ERROR", err)
                            elseif dbuffer:stream_complete() then
                                STATE.data.active_song_meta = advance_queue()
                                dbuffer = nil
                            end
                            STATE.active_stream_id = nil
                            os.queueEvent('redionet:fetch_audio')
                        end
                    elseif event == "redionet:playback_stopped" then
                        STATE.active_stream_id = nil
                        STATE.data.timeline_origin_ms = nil
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

function M.get_listener_snapshot()
    local clients = {}
    for id, status in pairs(M.state.receiver_stats) do
        table.insert(clients, { id = id, active = status == 1 })
    end
    table.sort(clients, function(a, b) return a.id < b.id end)
    return {
        total = M.state.n_receivers,
        active = M.state.num_active,
        clients = clients,
    }
end

return M
