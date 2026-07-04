--[[
    Audio module
    Manages audio decoding, transmission, and song queue.
]]

local dfpwm = require("cc.audio.dfpwm")

local network = require("server_lib.network")
local chat = require('server_lib.chat')

local AUDIO_CHUNK_SEC = 2.70 -- maximum tick multiple under 2.730666.. [(2^7 * 2^10) samples / 48000kHz]
local TICK = 0.050
local START_LEAD_MS = 1200
local RESYNC_LEAD_MS = 600
local PREFETCH_MS = 12000 -- max encoded audio scheduled ahead of wall clock
local TIMELINE_SETTLE_SEC = 0.05

local M = {}

M.state = {
    receiver_stats = {}, -- {id: (-1|1)}
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
    prefill_end = true,
    timeline_origin_ms = nil, -- play_at_ms = origin + audio_position_ms
}

local previous = {
    time_audio_sent = nil,
    audio_position_sec = 0,
}

---@class Buffer
local Buffer = {}

--- Creates a new Buffer instance.
---@class ReadHandle
---@param handle ReadHandle read handle returned by http.request
---@param song_meta table expected song metadata
---@return Buffer instance
function Buffer.new(handle, song_meta)
    local self = {
        handle = handle,
        index = 0,
        song_meta = song_meta,
        song_id = song_meta.id or "INVALID",
        max_buffer_length = 12,
        chunk_size = (AUDIO_CHUNK_SEC * 48000) / 8, --16 * 1024,
        total_read = { bytes = 0, chunks = 0 },
        total_write = { bytes = 0, chunks = 0 },
        done_read = false,
        done_write = false,
        destroyed = false,
    }

    self.buffer = {}
    self.decoder = dfpwm.make_decoder()
    self.audio_total_sec = self.song_meta.duration.H*3600 + self.song_meta.duration.M*60 + self.song_meta.duration.S
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

        local next = self.buffer[1]
        table.remove(self.buffer, 1)

        self.total_write.chunks = self.total_write.chunks + 1
        self.total_write.bytes = self.total_write.bytes + #next

        chat.log_message(
            string.format('<%02d|%03d/%03d> [\25%0.1f\24%0.1f] KiB',
                #self.buffer, self.total_write.chunks, self.audio_total_chunks,
                self.total_read.bytes / 1024, self.total_write.bytes / 1024),
            "DEBUG")

        return next
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

        local dsz = #data
        table.insert(self.buffer, data)

        self.total_read.chunks = self.total_read.chunks + 1
        self.total_read.bytes = self.total_read.bytes + dsz
    end

    function self:read_n(n)
        for i = 1, n do self:read() end
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

local function chunk_duration_ms(data)
    return (#data * 8 * 1000) / 48000
end

local function position_ms_from_sec(audio_position_sec)
    return math.floor(audio_position_sec * 1000 + 0.5)
end

--- Broadcast timeline anchor; origin maps wall clock ms -> song position ms.
---@param is_new_stream boolean
---@param chunk_id number|nil
---@param audio_position_sec number|nil
function M.arm_timeline(is_new_stream, chunk_id, audio_position_sec)
    local lead_ms = is_new_stream and START_LEAD_MS or RESYNC_LEAD_MS
    local position_ms = position_ms_from_sec(audio_position_sec or 0)
    M.state.timeline_origin_ms = os.epoch("local") + lead_ms - position_ms

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

local function wait_for_pipeline_room(play_at_ms, chunk_dur_ms)
    local end_ms = play_at_ms + chunk_dur_ms
    while end_ms - os.epoch("local") > PREFETCH_MS do
        os.sleep(TICK)
    end
end

local function transmit_audio(data_buffer)
    local audio_chunk = data_buffer:next()
    if not audio_chunk then
        os.queueEvent("redionet:request_next_chunk")
        return
    end

    local chunk_dur_ms = chunk_duration_ms(audio_chunk)
    local audio_dur_sec = chunk_dur_ms / 1000

    local sub_state = {
        active_stream_id = STATE.active_stream_id,
        song_id = data_buffer.song_id,
        chunk_id = data_buffer.total_write.chunks,
        audio_position_sec = previous.audio_position_sec,
    }
    previous.audio_position_sec = previous.audio_position_sec + audio_dur_sec
    STATE.data.audio_position_sec = sub_state.audio_position_sec
    STATE.audio_position_epoch_ms = os.epoch("local")

    if M.state.n_receivers == 0 then
        chat.log_message('No visible client connections... Stopping', 'WARN')
        return M.stop_song()
    end

    if M.state.need_sync then
        M.arm_timeline(sub_state.chunk_id == 1, sub_state.chunk_id, sub_state.audio_position_sec)
        M.state.need_sync = false
        chat.log_message(('Audio sync. Listening: %d/%d'):format(M.state.num_active, M.state.n_receivers), "INFO")
    end

    if not M.state.timeline_origin_ms then
        M.state.timeline_origin_ms = os.epoch("local") + START_LEAD_MS
    end

    local position_ms = position_ms_from_sec(sub_state.audio_position_sec)
    sub_state.sent_ms = os.epoch("local")
    sub_state.play_at_ms = M.state.timeline_origin_ms + position_ms
    sub_state.timeline_origin_ms = M.state.timeline_origin_ms

    wait_for_pipeline_room(sub_state.play_at_ms, chunk_dur_ms)

    rednet.broadcast({ audio_chunk, sub_state }, REDIONET_PROTO.AUDIO)
    local time_audio_sent = os.epoch("local")

    if previous.time_audio_sent then
        chat.log_message(('Send elapsed: %0.3fs, pos: %0.3fs, play_at: %0.3f'):format(
            (time_audio_sent - previous.time_audio_sent) / 1000,
            sub_state.audio_position_sec,
            sub_state.play_at_ms / 1000), "DEBUG")
    end
    previous.time_audio_sent = time_audio_sent

    if M.state.prefill_end then
        data_buffer:read_n(2)
    end

    os.queueEvent("redionet:request_next_chunk")
end

---@param data_buffer Buffer holds data
local function process_audio_data(data_buffer)
    M.state.need_sync = true
    M.state.prefill_end = true
    M.state.timeline_origin_ms = nil

    previous = {
        time_audio_sent = nil,
        audio_position_sec = 0
    }

    while STATE.active_stream_id == data_buffer.song_id and STATE.data.status == 1 do
        transmit_audio(data_buffer)
        parallel.waitForAny(
            function() os.pullEvent("redionet:request_next_chunk") end,
            function() os.pullEvent("redionet:playback_stopped") end
        )
        if STATE.data.status < 1 or STATE.active_stream_id == nil then break end
    end

    return data_buffer:stream_complete()
end

local function set_state_queue_empty()
    if STATE.data.status ~= 0 then
        STATE.data.status = -1
    end

    STATE.data.active_song_meta = nil
    STATE.data.audio_position_sec = 0
    STATE.audio_position_epoch_ms = nil

    STATE.data.is_loading = false
    STATE.data.error_status = false
    STATE.active_stream_id = nil
end

---Moves the queue forward 1 song. Accounts for loop_mode state.
---@return table? song_meta_data meta data of next queued song or nil if queue empty
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
            while true do
                local id, msg = rednet.receive(REDIONET_PROTO.AUDIO_NEXT)
                if msg == "playback_desync" then
                    M.state.need_sync = true
                    chat.log_message(('Client #%d requested resync'):format(id), "WARN")
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

                    os.queueEvent('redionet:broadcast_state', "audio_loop - ".. event)

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
                            if dbuffer then
                                dbuffer = dbuffer:destroy()
                            end
                            dbuffer = Buffer.new(handle, STATE.data.active_song_meta)

                            local song_completed = process_audio_data(dbuffer)
                            if song_completed then
                                STATE.data.active_song_meta = advance_queue()
                                dbuffer = nil
                            end
                            STATE.active_stream_id = nil

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
