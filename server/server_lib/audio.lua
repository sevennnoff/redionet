--[[
    Audio module
    Manages audio decoding, transmission, and song queue.
]]

local dfpwm = require("cc.audio.dfpwm")

local network = require("server_lib.network")
local chat = require('server_lib.chat')
local REDIONET_VERSION = require("lib.version")

local AUDIO_CHUNK_SEC = 2.70 -- maximum tick multiple under 2.730666.. [(2^7 * 2^10) samples / 48000kHz]
local TICK = 0.050
-- local Gms = 72 -- game milliseconds. 72ms : 1ms

local M = {}

M.state = {
    receiver_stats = {}, -- {id: (-1|1)}
    num_active = 0,
    n_receivers = 0,
    need_sync = false,
    pending_resync = false,
    last_maintain_sync_ms = nil,
    last_soft_resync_ms = nil,
    last_desync_ms = 0,
    speaker_cache = 0.0, -- seconds
    prefill_end = true,
}

local SOFT_DESYNC_MS = 450
local HARD_DESYNC_MS = 1500
local RESYNC_COOLDOWN_MS = 45000
local MAINTAIN_SYNC_SEC = 90

local function get_speaker_cache_target()
    local level = STATE.data.sync_buffer
    if level == nil then level = 1.5 end
    local scale = 0.55 + (level / 3) * 1.0
    return (AUDIO_CHUNK_SEC / 2) * scale
end

local function first_response_timeout_sec()
    return AUDIO_CHUNK_SEC + get_speaker_cache_target()
end

local function maintain_periodic_sync()
    if STATE.data.status ~= 1 then return end
    local now = os.epoch("local")
    if not M.state.last_maintain_sync_ms then
        M.state.last_maintain_sync_ms = now
        return
    end
    if now - M.state.last_maintain_sync_ms < MAINTAIN_SYNC_SEC * 1000 then return end
    M.state.last_maintain_sync_ms = now
    if M.state.last_desync_ms > SOFT_DESYNC_MS then
        M.state.pending_resync = true
        chat.log_message(('Periodic maintain (last desync %dms)'):format(M.state.last_desync_ms), 'INFO')
    end
end

local function apply_pending_resync()
    if not M.state.pending_resync then return end
    M.state.pending_resync = false

    local now = os.epoch("local")
    if M.state.last_soft_resync_ms and (now - M.state.last_soft_resync_ms) < RESYNC_COOLDOWN_MS then
        return
    end

    chat.log_message('Soft resync at chunk boundary', 'INFO')
    os.queueEvent('redionet:sync')
    local sync_timer = os.startTimer(TICK * 2)
    repeat _, tid = os.pullEvent('timer') until tid == sync_timer
    os.cancelTimer(sync_timer)
    M.state.speaker_cache = 0
    M.state.last_soft_resync_ms = now
end

local function parse_connection_payload(payload)
    if type(payload) == "table" then
        return payload[1], payload[2]
    end
    return payload, nil
end

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

local function wait_speakers(max_wait, eps)
    max_wait = max_wait or 1.000

    local timer_max =  os.startTimer(max_wait - (eps or 0.00))

    local tid
    repeat _,tid = os.pullEvent('timer')
    until tid == timer_max
    os.cancelTimer(timer_max)
end


local function parse_ack_payload(msg, expected_chunk_id)
    local ack_kind, ack_chunk_id
    if type(msg) == "table" then
        ack_chunk_id, ack_kind = msg[1], msg[2]
    else
        ack_kind = msg
        ack_chunk_id = nil
    end

    if ack_kind == "request_next_chunk" then
        if ack_chunk_id and ack_chunk_id ~= expected_chunk_id then
            return nil
        end
        return "request_next_chunk"
    end
    if ack_kind == "playback_stopped" then
        return "playback_stopped"
    end
    if ack_kind == "playback_interrupted" then
        return "playback_interrupted"
    end
    return nil
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
        audio_position_sec = previous.audio_position_sec,
        volume = STATE.data.volume,
    }

    if M.state.n_receivers == 0 then
        chat.log_message('No visible client connections... Stopping', 'WARN')
        return M.stop_song()
    end

    maintain_periodic_sync()

    local time_audio_sent
    local ok, err

    if M.state.need_sync then
        local sync_wait = TICK

        if sub_state.chunk_id == 1 then
            sync_wait = 2*sync_wait
            os.queueEvent('redionet:sync')
        end
        -- no mid-track CLIENT_SYNC — stops all speakers and causes doubling

        chat.log_message(('Audio sync. Listening: %d/%d'):format(M.state.num_active, M.state.n_receivers), "INFO")

        local sync_timer,tid = os.startTimer(sync_wait), nil
        repeat _,tid = os.pullEvent('timer') until tid == sync_timer
        os.cancelTimer(sync_timer)

        M.state.need_sync = false
    end

    apply_pending_resync()

    local reply = {ids = {}, times = {}}

    local play_state = {
        receiver_stats = {},
        n_receivers = 0,
        num_active = 0,
    }

    for attempt = 1, 3 do
        reply = {ids = {}, times = {}}
        play_state = {
            receiver_stats = {},
            n_receivers = 0,
            num_active = 0,
        }
        time_audio_sent = nil

        local function timed_play_task()
        local istate = {
            n_receivers = M.state.n_receivers,
            num_active = M.state.num_active,
            receiver_stats = {},
        }
        for id, status in pairs(M.state.receiver_stats) do istate.receiver_stats[id] = status end

        local timeout = get_speaker_cache_target()
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
                fallback_timer = os.startTimer(first_response_timeout_sec())
                while true do os.pullEvent() end
            end,
            function ()
                repeat
                    local id, msg = rednet.receive(REDIONET_PROTO.AUDIO_NEXT)
                    local ack_kind = parse_ack_payload(msg, sub_state.chunk_id)
                    if ack_kind and not timer then
                        local first_responce_sec = (os.epoch('local') - time_audio_sent )/1000
                        if fallback_timer then os.cancelTimer(fallback_timer) end
                        timer = os.startTimer(timeout)
                        chat.log_message(('First responce: %0.3fs'):format(first_responce_sec), "DEBUG")
                    end

                    if play_state.receiver_stats[id] ~= nil then
                        -- duplicate ack
                    elseif ack_kind == "request_next_chunk" then
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
                    elseif ack_kind == "playback_stopped" then
                        play_state.n_receivers = play_state.n_receivers + 1
                        play_state.receiver_stats[id] = -1
                    elseif ack_kind == "playback_interrupted" then
                        break
                    end
                until all_receivers_replied()
            end,

            function ()
                repeat _,tid = os.pullEvent('timer') until (timer and tid == timer) or (fallback_timer and tid == fallback_timer)
                local slow_ids = {}
                for id, status in pairs(istate.receiver_stats) do
                    if not play_state.receiver_stats[id] then
                        table.insert(slow_ids, {id = id, status = status})
                    end
                end
                if #slow_ids > 0 then
                    local n_expected = 0
                    for _ in pairs(istate.receiver_stats) do n_expected = n_expected + 1 end
                    if #slow_ids >= n_expected then
                        chat.log_message(('All clients missed chunk #%d ack (attempt %d/3)'):format(
                            sub_state.chunk_id, attempt), 'WARN')
                    else
                        for _, client in ipairs(slow_ids) do
                            rednet.send(client.id, "audio.stop_song", REDIONET_PROTO.AUDIO_HALT)
                            M.state.receiver_stats[client.id] = nil
                            M.state.n_receivers = M.state.n_receivers - 1
                            M.state.num_active = M.state.num_active - (client.status == 1 and 1 or 0)
                            chat.log_message(('Client #%d timed out, halted'):format(client.id), 'WARN')
                        end
                    end
                end
            end
        )

        if timer then os.cancelTimer(timer) end
        if fallback_timer then os.cancelTimer(fallback_timer) end
        end

        ok, err = pcall(parallel.waitForAll, timed_play_task, function () if not M.state.prefill_end then data_buffer:read_n(2) end end)
        if not ok then break end
        if #reply.ids > 0 then break end
        if attempt < 3 then
            chat.log_message(('Retrying chunk #%d broadcast'):format(sub_state.chunk_id), 'WARN')
        end
    end

    if #reply.ids == 0 and STATE.active_stream_id ~= nil then
        chat.log_message('No remaining listeners... Stopping', 'WARN')
        return M.stop_song()
    end

    previous.audio_position_sec = previous.audio_position_sec + audio_dur_sec
    STATE.data.audio_position_sec = sub_state.audio_position_sec
    STATE.audio_position_epoch_ms = os.epoch("local")

    if #reply.times > 1 then
        local desync_ms = (math.max(table.unpack(reply.times)) - math.min(table.unpack(reply.times)))--/Gms
        M.state.last_desync_ms = desync_ms
        chat.log_message(string.format('max client desync: %dms | n=%d/%d', desync_ms, #reply.times, play_state.n_receivers),
            desync_ms > SOFT_DESYNC_MS and "INFO" or "DEBUG")

        if desync_ms > HARD_DESYNC_MS then
            M.state.pending_resync = true
            chat.log_message(('High desync %dms — soft resync queued'):format(desync_ms), 'WARN')
        elseif desync_ms > SOFT_DESYNC_MS then
            local brake_sec = round_tick_sec((desync_ms - SOFT_DESYNC_MS) / 1000 * 0.5)
            M.state.speaker_cache = M.state.speaker_cache + brake_sec
            chat.log_message(('Soft brake +%0.2fs (desync %dms)'):format(brake_sec, desync_ms), 'INFO')
        end
    end


    if previous.time_audio_sent then
        local send_elapsed = (time_audio_sent - previous.time_audio_sent)
        -- chat.log_message(('Send elapsed: %0.3fs, SpkCache: %0.3fs'):format(send_elapsed/(Gms*1000), M.state.speaker_cache), "DEBUG")
        chat.log_message(('Send elapsed: %0.3fs, SpkCache: %0.3fs'):format(send_elapsed/1000, M.state.speaker_cache), "DEBUG")
    end

    previous.time_audio_sent = time_audio_sent

    local elapsed_sec
    if #reply.times > 0 then
        elapsed_sec = (math.max(table.unpack(reply.times)) - time_audio_sent) / 1000
    else
        elapsed_sec = (os.epoch('local') - time_audio_sent) / 1000
    end
    local free_sec = audio_dur_sec - elapsed_sec

    M.state.speaker_cache = M.state.speaker_cache + free_sec
    local wait_seconds = round_tick_sec(M.state.speaker_cache - get_speaker_cache_target() - 0.005)

    chat.log_message(('elap: %0.3fs, free: %0.3fs, audio: %0.3fs\n'..'SpkCache: %0.3fs, total_wait: %0.3fs'):format(
        elapsed_sec, free_sec, audio_dur_sec,  M.state.speaker_cache, wait_seconds), "DEBUG")

    parallel.waitForAll(
        function () wait_speakers(wait_seconds) end,
        function () if M.state.prefill_end and wait_seconds > 1.000 then data_buffer:read_n(2) end end
    )

    M.state.speaker_cache = M.state.speaker_cache - wait_seconds
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
    M.state.pending_resync = false
    M.state.last_maintain_sync_ms = nil
    M.state.last_desync_ms = 0

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
                local id, payload = rednet.receive(REDIONET_PROTO.AUDIO_CONNECTION)
                local status, client_version = parse_connection_payload(payload)

                if client_version and client_version ~= REDIONET_VERSION then
                    chat.log_message(('Client #%d version mismatch (%s != %s). Run rn update on all devices.'):format(
                        id, tostring(client_version), REDIONET_VERSION), 'WARN')
                elseif not client_version and status ~= -1 then
                    chat.log_message(('Client #%d has no version tag — likely outdated. Run rn update.'):format(id), 'WARN')
                end

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
                            chat.announce_song(STATE.data.active_song_meta.artist, STATE.data.active_song_meta.name)
                            os.queueEvent('redionet:broadcast_state', 'track start')
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
