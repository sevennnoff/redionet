--[[
    Audio module
    Manages audio decoding, transmission, and song queue.
]]

local dfpwm = require("cc.audio.dfpwm")

local network = require("server_lib.network")
local chat = require('server_lib.chat')

local AUDIO_CHUNK_SEC = 2.70 -- maximum tick multiple under 2.730666.. [(2^7 * 2^10) samples / 48000kHz]
local TICK = 0.050
local START_LEAD_MS = 1500
local RESYNC_LEAD_MS = 800
local CHUNK_LEAD_MS = 100
local PREFETCH_CHUNKS = 3
local MAX_PIPELINE_MS = math.floor(PREFETCH_CHUNKS * AUDIO_CHUNK_SEC * 1000)
local ACK_COLLECT_SEC = 0.12
local TIMELINE_SETTLE_SEC = 0.15

local M = {}

M.state = {
    receiver_stats = {}, -- {id: (-1|1)}
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
    prefill_end = true,
    next_play_at_ms = nil,
}

local previous = {
    req_chunk_times = {},
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
        max_buffer_length = 8,
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
    -- for i = 1, self.size, self.chunk_size do
    --     table.insert(self.buffer, string.sub(data, i, i + (self.chunk_size-1)))
    -- end

    function self:next()
        if self.done_write then return end

        if #self.buffer == 0 then -- first call occurs before parallel, will need to read
            self:read()
        end

        if self.done_read and #self.buffer == 0 then
            self.done_write = true
            self.song_id = "NULL" -- avoid setting nil because of the nil == nil behavior
            return
        end


        local next = self.buffer[1]
        table.remove(self.buffer, 1)

        self.total_write.chunks = self.total_write.chunks + 1
        self.total_write.bytes = self.total_write.bytes + #next -- decoded length

        chat.log_message(
        -- os.queueEvent("redionet:log_message",
            string.format('<%02d|%03d/%03d> [\25%0.1f\24%0.1f] KiB',
                -- #self.buffer, self.total_read.chunks, self.total_write.chunks,
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

        local dsz = #data -- encoded length
        table.insert(self.buffer, data)
        
        self.total_read.chunks = self.total_read.chunks + 1
        self.total_read.bytes = self.total_read.bytes + dsz
        
    end

    function self:read_n(n)
        for i=1,n do self:read() end
    end
    
    function self:destroy()
        self.destroyed = true
        self.done_read = true
        pcall(self.handle.close)
        self.done_write = true
        self.song_id = "NULL" -- avoid setting nil because of the nil == nil behavior
        self.buffer = nil
        return nil
    end

    function self:stream_complete()
        return not self.destroyed and self.done_write and self.done_read
    end

    return self
end


--- Broadcast timeline anchor so all clients flush and align on local wall clock.
---@param is_new_stream boolean longer lead before first chunk of a new song
function M.arm_timeline(is_new_stream)
    local lead_ms = is_new_stream and START_LEAD_MS or RESYNC_LEAD_MS
    M.state.next_play_at_ms = os.epoch("local") + lead_ms
    rednet.broadcast({
        kind = "timeline",
        anchor_ms = M.state.next_play_at_ms,
        stream_id = STATE.active_stream_id,
    }, REDIONET_PROTO.CLIENT_SYNC)

    local settle_timer = os.startTimer(TIMELINE_SETTLE_SEC)
    repeat
        local _, tid = os.pullEvent("timer")
    until tid == settle_timer
    os.cancelTimer(settle_timer)
end

local function wait_for_pipeline_room()
    while M.state.next_play_at_ms do
        local ahead_ms = M.state.next_play_at_ms - os.epoch("local")
        if ahead_ms <= MAX_PIPELINE_MS then
            break
        end
        os.sleep(TICK)
    end
end

--  broadcasts encoded audio chunks with play_at_ms over the audio protocol
local function transmit_audio(data_buffer)
    wait_for_pipeline_room()

    local audio_chunk = data_buffer:next()
    if not audio_chunk then
        os.queueEvent("redionet:request_next_chunk")
        return
    end

    local audio_dur_sec = (#audio_chunk * 8) / 48000

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
        M.arm_timeline(sub_state.chunk_id == 1)
        M.state.need_sync = false
        chat.log_message(('Audio sync. Listening: %d/%d'):format(M.state.num_active, M.state.n_receivers), "INFO")
    end

    local reply = {ids = {}, times = {}}
    local play_state = {
        receiver_stats = {},
        n_receivers = 0,
        num_active = 0,
    }
    local time_audio_sent

    local function timed_play_task()
        local istate = {
            n_receivers = M.state.n_receivers,
            num_active = M.state.num_active,
            receiver_stats = {},
        }
        for id, status in pairs(M.state.receiver_stats) do
            istate.receiver_stats[id] = status
        end

        local now_ms = os.epoch("local")
        sub_state.play_at_ms = math.max(M.state.next_play_at_ms or now_ms, now_ms + CHUNK_LEAD_MS)
        M.state.next_play_at_ms = sub_state.play_at_ms + math.floor(audio_dur_sec * 1000)

        local function all_receivers_replied()
            for id, _ in pairs(istate.receiver_stats) do
                if play_state.receiver_stats[id] == nil then
                    return false
                end
            end
            return true
        end

        local collect_timer = os.startTimer(ACK_COLLECT_SEC)
        parallel.waitForAny(
            function()
                rednet.broadcast({audio_chunk, sub_state}, REDIONET_PROTO.AUDIO)
                time_audio_sent = os.epoch("local")
                while true do os.pullEvent() end
            end,
            function()
                repeat
                    local id, msg = rednet.receive(REDIONET_PROTO.AUDIO_NEXT)
                    if play_state.receiver_stats[id] ~= nil then
                        -- duplicate ack
                    elseif msg == "chunk_received" or msg == "request_next_chunk" then
                        play_state.n_receivers = play_state.n_receivers + 1
                        local timestamp_ms = os.epoch("local")
                        play_state.receiver_stats[id] = 1
                        play_state.num_active = play_state.num_active + 1
                        table.insert(reply.ids, id)
                        table.insert(reply.times, timestamp_ms)
                        chat.log_message(string.format('#%d (%s, recv) | n=%d/%d', id,
                            ("%0.3f"):format(timestamp_ms / 1000):sub(-8),
                            play_state.num_active, play_state.n_receivers), "DEBUG")
                    elseif msg == "playback_stopped" then
                        play_state.n_receivers = play_state.n_receivers + 1
                        play_state.receiver_stats[id] = -1
                    elseif msg == "playback_interrupted" then
                        break
                    end
                until all_receivers_replied()
            end,
            function()
                repeat
                    local _, tid = os.pullEvent("timer")
                until tid == collect_timer
            end
        )
        os.cancelTimer(collect_timer)
    end

    local ok, err = pcall(parallel.waitForAll, timed_play_task, function()
        if not M.state.prefill_end then
            data_buffer:read_n(2)
        end
    end)

    if #reply.ids == 0 and STATE.active_stream_id ~= nil then
        chat.log_message('No chunk_received acks', 'WARN')
    end

    if #reply.times > 1 then
        local desync_ms = math.max(table.unpack(reply.times)) - math.min(table.unpack(reply.times))
        chat.log_message(string.format('max recv desync: %dms | n=%d/%d', desync_ms, #reply.times, play_state.n_receivers), "INFO")

        if desync_ms > 1000 then
            chat.log_message('Detected client desync. Forcing sync..', "WARN")
            M.state.need_sync = true
        end
    end

    if previous.time_audio_sent then
        local send_elapsed = (time_audio_sent - previous.time_audio_sent)
        chat.log_message(('Send elapsed: %0.3fs, play_at: %0.3f'):format(
            send_elapsed / 1000, (sub_state.play_at_ms or 0) / 1000), "DEBUG")
    end
    previous.time_audio_sent = time_audio_sent

    if M.state.prefill_end then
        data_buffer:read_n(2)
    end

    if ok then
        os.queueEvent("redionet:request_next_chunk")
    else
        os.queueEvent("redionet:playback_stopped", "PLAYBACK_ERROR", err)
    end
end

---@param data_buffer Buffer holds data
local function process_audio_data(data_buffer)
    M.state.need_sync = true
    M.state.prefill_end = true
    M.state.next_play_at_ms = nil

    previous = {
        req_chunk_times = {},
        time_audio_sent = nil,
        audio_position_sec = 0
    }
    

    while STATE.active_stream_id == data_buffer.song_id and STATE.data.status==1 do
        transmit_audio(data_buffer)
        parallel.waitForAny(
            function() os.pullEvent("redionet:request_next_chunk") end,
            function() os.pullEvent("redionet:playback_stopped") end
        )
        if STATE.data.status<1 or STATE.active_stream_id==nil then break end
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
        if STATE.data.loop_mode == 1 then     -- Loop Queue
            table.insert(STATE.data.queue, STATE.data.active_song_meta)
        elseif STATE.data.loop_mode == 2 then -- Loop song
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
            M.stop_song() -- if different song currently streaming, stop
        end

        STATE.data.active_song_meta = song_meta -- overwrite current meta (may be identical)
        STATE.data.audio_position_sec = 0
        STATE.audio_position_epoch_ms = nil
    end

    STATE.data.status = 1 -- needs to be at end to overwrite stop_song()

    os.queueEvent("redionet:fetch_audio")
end

function M.stop_song()
    rednet.broadcast("audio.stop_song", REDIONET_PROTO.AUDIO_HALT)
    os.queueEvent("redionet:playback_stopped") -- pulled by process_audio_data
    STATE.active_stream_id = nil
    STATE.data.status = 0
    STATE.data.audio_position_sec = 0
    STATE.audio_position_epoch_ms = nil
end

function M.skip_song()
    -- cannot rely on nil/fetch_audio behaviour because of looping
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
        function ()
            while true do
                local id, status = rednet.receive(REDIONET_PROTO.AUDIO_CONNECTION)

                if status == -1 then -- special case for speakerless device. Allows sync on toggle Quit/Join, but doesn't add to known receivers.
                    M.state.need_sync = true
                else
                    if not M.state.receiver_stats[id] then -- new client connection
                        M.state.n_receivers = M.state.n_receivers + 1
                    end

                    if M.state.receiver_stats[id] ~= status then -- only update on status change
                        if status == 1 then
                            M.state.need_sync = true -- NOTE: do not false when ~= 1
                            M.state.num_active = M.state.num_active + 1
                        else
                            M.state.num_active = M.state.num_active - 1
                        end
                        M.state.receiver_stats[id] = status
                    end
                end
            end
        end,
        function ()
            local event_filter = {
                ["redionet:fetch_audio"] = true,
                ["redionet:audio_ready"] = true,
                ["redionet:playback_stopped"] = true,
            }
            local dbuffer -- data buffer, need out of loop body to properly destroy in the event of early termination
            while true do
                local eventData = { os.pullEvent() }
                local event = eventData[1]

                if event_filter[event] then
                    if STATE.data.active_song_meta == nil then
                        STATE.data.active_song_meta = advance_queue()
                    end

                    local can_play = STATE.data.active_song_meta ~= nil -- if still nil after advance_queue, then queue empty, nothing to play
                    local should_play = STATE.data.status ~= 0 -- if it's -1 or +1, play as soon as data is available
                    
                    if not can_play then
                        event = "redionet:event_cancelled" -- skip the event handling below
                    end
                    
                    -- may trigger more than strictly necessary, but centeralizing eliminates need for a patchwork of calls elsewhere
                    os.queueEvent('redionet:broadcast_state', "audio_loop - ".. event)

                    if event == "redionet:fetch_audio" then
                        local has_data_stream    = (STATE.active_stream_id ~= nil)
                        local has_correct_stream = has_data_stream and (STATE.active_stream_id == STATE.data.active_song_meta.id)
                        
                        -- debug.debug()
                        -- This will always execute if queued properly and should_play==true, but keep as safety check to avoid re-downloading an actively streaming song
                        if should_play and not has_correct_stream then
                            rednet.broadcast('status', REDIONET_PROTO.AUDIO_STATUS) -- trigger status update
                            network.download_song(STATE.data.active_song_meta.id)
                        end
                        
                    elseif event == "redionet:audio_ready" then
                        local handle = STATE.response_handle
                        if not handle then error('bad state: read handle is nil', 0) end -- appease the linter (state should be unreachable)

                        if should_play then
                            -- announce here, last moment before audio actually plays
                            chat.announce_song(STATE.data.active_song_meta.artist, STATE.data.active_song_meta.name)
                            if dbuffer then
                                dbuffer = dbuffer:destroy() -- if it still exists, the song didn't complete. cannot guarantee clean state
                            end
                            dbuffer = Buffer.new(handle, STATE.data.active_song_meta)

                            local song_completed = process_audio_data(dbuffer)
                            if song_completed then
                                -- EDGE CASE?: click skip song just as a song ends => skips over a song
                                STATE.data.active_song_meta = advance_queue() -- can't set active_song_meta = nil in case of looping
                                dbuffer = nil -- if completed, don't need to destroy, file handle will have already been closed
                            end
                            STATE.active_stream_id = nil -- (re)download on next play, regardless of if finished
                            
                            os.queueEvent('redionet:fetch_audio') -- needed to auto play next song
                            
                        end
                    elseif event == "redionet:playback_stopped" then
                        STATE.active_stream_id = nil
                        STATE.data.is_loading = false
                        STATE.data.error_status = eventData[2] or false -- PLAYBACK_ERROR or false
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
