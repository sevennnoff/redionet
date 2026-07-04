--[[
    Audio module
    Manages audio decoding, transmission, and song queue.
]]

local dfpwm = require("cc.audio.dfpwm")

local network = require("server_lib.network")
local chat = require('server_lib.chat')

local AUDIO_CHUNK_SEC = 2.70 -- maximum tick multiple under 2.730666.. [(2^7 * 2^10) samples / 48000kHz]
local TICK = 0.050
-- local Gms = 72 -- game milliseconds. 72ms : 1ms

local M = {}

M.state = {
    receiver_stats = {}, -- {id: (-1|1)}
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
    speaker_cache = 0.0, -- seconds
    prefill_end = true,
}

local speaker_cache_target = AUDIO_CHUNK_SEC/2 -- sec of audio stored in speakers at anytime. Higher = latency desync protection. Lower = reduced resync audio gaps
local speaker_cache_max = speaker_cache_target * 2
local first_response_timeout = AUDIO_CHUNK_SEC
local AUTO_RESYNC_MS = 5000 -- manual `rn sync` still available; auto flush caused collective silence

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

        -- table.insert(self.buffer, data)
        table.insert(self.buffer, self.decoder(data))
        --[[
        Preliminary testing shows desynchronization issues worsen when decoding is done
        by the client. Server decode, cache, transmit seems to be the best approach.
        For posterity, it's worth noting the main downside is larger rednet transmissions.
        The decoded message is a table of 131k ints compared to encoded 16k chars. 
        ]]
        
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


local function round_tick_sec(seconds)
    if seconds <= 0 then return 0 end
    return math.ceil(math.max(seconds, TICK) * 20) * TICK -- 20 tick/sec
end

local function flush_speaker_state(reason)
    M.state.speaker_cache = 0
    M.state.prefill_end = true
    chat.log_message(('Speaker flush: %s'):format(reason or 'sync'), 'INFO')
end

local function wait_speakers(max_wait, eps)
    max_wait = max_wait or 1.000

    local timer_max =  os.startTimer(max_wait - (eps or 0.00))

    local tid
    repeat _,tid = os.pullEvent('timer')
    until tid == timer_max
    os.cancelTimer(timer_max)
end


--  broadcasts the decoded audio buffer data over the audio protocol
local function transmit_audio(data_buffer)
     -- NOTE: logging via os.queueEvent("redionet:log_message") saturates event queue, keep sync
    local audio_chunk = data_buffer:next()
    if not audio_chunk then
        os.queueEvent("redionet:request_next_chunk")
        return
    end

    local audio_dur_sec = (#audio_chunk/48000)

    local sub_state = {
        active_stream_id = STATE.active_stream_id, -- this is the only place we give clients access to active_stream_id
        song_id = data_buffer.song_id, -- add in local song_id for interrupts
        chunk_id = data_buffer.total_write.chunks,
        -- audio_dur_sec = audio_dur_sec,
        audio_position_sec = previous.audio_position_sec,
    }
    previous.audio_position_sec = previous.audio_position_sec + audio_dur_sec
    STATE.data.audio_position_sec = sub_state.audio_position_sec
    STATE.audio_position_epoch_ms = os.epoch("local")

    if M.state.n_receivers == 0 then
        chat.log_message('No visible client connections... Stopping', 'WARN')
        return M.stop_song()
    end
    

    local reply = {ids = {}, times = {}}
    
    local play_state = {
        receiver_stats = {},
        n_receivers = 0,
        num_active = 0,
    }

    local time_audio_sent

    

    local function timed_play_task()
        -- copy initial, pre-broadcast state or changes while yielding may cause unexpected behavior
        local istate = {
            n_receivers = M.state.n_receivers,
            num_active = M.state.num_active,
            receiver_stats = {},
        }
        for id,status in pairs(M.state.receiver_stats) do istate.receiver_stats[id] = status end
        
        local timeout = speaker_cache_target
        local timer, fallback_timer, tid

        local function all_receivers_replied()
            for id, _ in pairs(istate.receiver_stats) do
                if play_state.receiver_stats[id] == nil then
                    return false
                end
            end
            return true
        end

        parallel.waitForAny(
            function ()
                rednet.broadcast({audio_chunk, sub_state}, REDIONET_PROTO.AUDIO)
                time_audio_sent = os.epoch("local")
                fallback_timer = os.startTimer(first_response_timeout)
                while true do os.pullEvent() end
            end,
            function ()
                repeat
                    local id,msg = rednet.receive(REDIONET_PROTO.AUDIO_NEXT)
                    if not timer then
                        local first_responce_sec = (os.epoch('local') - time_audio_sent )/1000
                        if fallback_timer then os.cancelTimer(fallback_timer) end
                        timer = os.startTimer(timeout)
                        chat.log_message(('First responce: %0.3fs'):format(first_responce_sec), "DEBUG")
                    end

                    if play_state.receiver_stats[id] ~= nil then
                        -- duplicate ack
                    elseif msg == "request_next_chunk" then
                        play_state.n_receivers = play_state.n_receivers + 1
                        local timestamp_ms = os.epoch("local")
                        play_state.receiver_stats[id] = 1
                        play_state.num_active = play_state.num_active + 1
                        table.insert(reply.ids, id)
                        table.insert(reply.times, timestamp_ms)
                        local play_duration = timestamp_ms - (previous.req_chunk_times[id] or timestamp_ms)
                        chat.log_message(string.format('#%d (%s, %dms) | n=%d/%d', id,
                            ("%0.3f"):format(timestamp_ms/1000):sub(-8), play_duration,
                            play_state.num_active, play_state.n_receivers ), "DEBUG")
                        previous.req_chunk_times[id] = timestamp_ms
                    elseif msg == "playback_stopped" then
                        play_state.n_receivers = play_state.n_receivers + 1
                        play_state.receiver_stats[id] = -1
                    elseif msg == "playback_interrupted" then
                        break
                    end
                until all_receivers_replied()
            end,

            function ()
                repeat _,tid = os.pullEvent('timer') until (timer and tid == timer) or (fallback_timer and tid == fallback_timer)
                for id, status in pairs(istate.receiver_stats) do
                    if not play_state.receiver_stats[id] then
                        M.state.receiver_stats[id] = nil
                        M.state.n_receivers = M.state.n_receivers - 1
                        M.state.num_active = M.state.num_active - (status == 1 and 1 or 0)
                        chat.log_message(('Client #%d timed out'):format(id), 'WARN')
                    end
                end
            end
        )

        if timer then os.cancelTimer(timer) end
        if fallback_timer then os.cancelTimer(fallback_timer) end

    end

    -- print(textutils.serialize(M.state, {compact = true, allow_repetitions = true}))
    if M.state.need_sync then
        local sync_wait = TICK

        if sub_state.chunk_id == 1 then
            sync_wait = 2 * sync_wait
            flush_speaker_state('track start')
            os.queueEvent('redionet:sync')
        else
            -- mid-play: reset pacing only; full CLIENT_SYNC flush desyncs speaker buffers
            flush_speaker_state('mid-play resync')
        end

        chat.log_message(('Audio sync. Listening: %d/%d'):format(M.state.num_active, M.state.n_receivers), "INFO")

        local sync_timer,tid = os.startTimer(sync_wait), nil
        repeat _,tid = os.pullEvent('timer') until tid == sync_timer
        os.cancelTimer(sync_timer)

        M.state.need_sync = false
    end

    local ok, err = pcall(parallel.waitForAll, timed_play_task, function () data_buffer:read_n(2) end)

    -- PROTO_AUDIO_HALT makes all clients not request_next_chunk, thus #rep_ids=0. Only warn if server has active song. 
    -- Noteably, audio.stop_song broadcasts halt. stop_song is also called when a song is skipped, or play now clicked.
    if #reply.ids == 0 and STATE.active_stream_id ~= nil then
        chat.log_message('No remaining listeners... Stopping', 'WARN')
        return M.stop_song()
    end

    if #reply.times > 1 then
        local desync_ms = (math.max(table.unpack(reply.times)) - math.min(table.unpack(reply.times)))--/Gms
        chat.log_message(string.format('max client desync: %dms | n=%d/%d', desync_ms, #reply.times, play_state.n_receivers), "INFO")

        if desync_ms > AUTO_RESYNC_MS then
            chat.log_message(('Client desync %dms (auto resync at %dms)'):format(desync_ms, AUTO_RESYNC_MS), "WARN")
            flush_speaker_state('desync')
            os.queueEvent('redionet:sync')
        end
    end


    if previous.time_audio_sent then
        local send_elapsed = (time_audio_sent - previous.time_audio_sent)
        -- chat.log_message(('Send elapsed: %0.3fs, SpkCache: %0.3fs'):format(send_elapsed/(Gms*1000), M.state.speaker_cache), "DEBUG")
        chat.log_message(('Send elapsed: %0.3fs, SpkCache: %0.3fs'):format(send_elapsed/1000, M.state.speaker_cache), "DEBUG")
    end

    previous.time_audio_sent = time_audio_sent

    local elapsed_sec = (os.epoch('local') - time_audio_sent)/1000
    -- local elapsed_sec = (os.epoch("ingame") - time_audio_sent)/(Gms*1000)
    local free_sec = audio_dur_sec - elapsed_sec

    M.state.speaker_cache = math.min(M.state.speaker_cache + free_sec, speaker_cache_max)
    local wait_seconds = round_tick_sec(M.state.speaker_cache - speaker_cache_target - 0.005)
    wait_seconds = math.min(wait_seconds, speaker_cache_target)

    chat.log_message(('elap: %0.3fs, free: %0.3fs, audio: %0.3fs\n'..'SpkCache: %0.3fs, total_wait: %0.3fs'):format(
        elapsed_sec, free_sec, audio_dur_sec,  M.state.speaker_cache, wait_seconds), "DEBUG")

    if wait_seconds > 0 then
        parallel.waitForAll(
            function () wait_speakers(wait_seconds) end,
            function () data_buffer:read_n(2) end
        )
    else
        data_buffer:read_n(2)
    end

    M.state.speaker_cache = math.max(0, M.state.speaker_cache - wait_seconds)
    -- if speaker buffers overfill, the majority of wait time will be on timed_play_task instead of wait_speakers. prefill_end determines when to read
    M.state.prefill_end = wait_seconds > elapsed_sec

    if ok then
        os.queueEvent("redionet:request_next_chunk")
    else
        os.queueEvent("redionet:playback_stopped", "PLAYBACK_ERROR", err)
    end
end

---@param data_buffer Buffer holds data
local function process_audio_data(data_buffer)
    M.state.speaker_cache = 0
    M.state.need_sync = true -- always sync on new song
    M.state.prefill_end = true

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
