--[[
    Client local audio
    Each speaker downloads from the API and plays on the server timeline.
]]

local net = require("client_lib.net")
local dfpwm = require("cc.audio.dfpwm")

local speaker = peripheral.find("speaker")

local AUDIO_CHUNK_SEC = 2.70
local CHUNK_BYTES = (AUDIO_CHUNK_SEC * 48000) / 8
local MIN_BUFFER_CHUNKS = 2
local DRIFT_RESYNC_MS = 800
local DRIFT_CHECK_SEC = 0.20
local DOWNLOAD_RETRIES = 3

local chunks = {}
local song_id = nil
local session_id = nil
local http_handle = nil
local read_done = false
local read_start_ms = 0
local buffer_ready_sent = false
local buffer_ready_sent_at = nil
local download_attempts = 0
local timeline_origin_ms = nil
local timeline_stream_id = nil
local next_chunk_id = 1
local decoder = dfpwm.make_decoder()
local clock_offset_ms = 0

local dbgmon = function() end

local function chunk_duration_ms(data)
    return (#data * 8 * 1000) / 48000
end

local function server_now_ms()
    return os.epoch("local") + clock_offset_ms
end

local function debug_init()
    settings.load()
    local monitor = peripheral.find("monitor")
    if monitor and settings.get('redionet.log_level', 3) == 1 then
        monitor.setTextScale(0.5)
        dbgmon = function(msg)
            local t = os.epoch("local")
            local prev = term.redirect(monitor)
            print(("[DBG] (%s) %s"):format(os.date("%H:%M:%S", t / 1000), msg))
            term.redirect(prev)
        end
    end
end

local function sync_clock(server_time_ms)
    if server_time_ms then
        clock_offset_ms = server_time_ms - os.epoch("local")
    end
end

local function hard_reset_local()
    if speaker then speaker.stop() end
    if http_handle then pcall(http_handle.close) end
    chunks = {}
    http_handle = nil
    read_done = false
    read_start_ms = 0
    buffer_ready_sent = false
    buffer_ready_sent_at = nil
    download_attempts = 0
    next_chunk_id = 1
    timeline_origin_ms = nil
    timeline_stream_id = nil
    decoder = dfpwm.make_decoder()
end

local function reset_playback(stream_id, chunk_id, origin_ms, new_session_id)
    if new_session_id and session_id and new_session_id ~= session_id then
        return
    end
    if new_session_id then session_id = new_session_id end
    if speaker then speaker.stop() end
    decoder = dfpwm.make_decoder()
    timeline_stream_id = stream_id
    timeline_origin_ms = origin_ms
    next_chunk_id = chunk_id or 1
end

local function target_position_sec()
    if not timeline_origin_ms then
        return CSTATE.server_state.audio_position_sec or 0
    end
    return math.max(0, (server_now_ms() - timeline_origin_ms) / 1000)
end

local function chunk_id_for_position(sec)
    if #chunks == 0 then return 1 end
    for i, entry in ipairs(chunks) do
        local end_sec = (entry.start_ms + chunk_duration_ms(entry.data)) / 1000
        if sec < end_sec then return i end
    end
    return #chunks
end

local function start_download(id, force)
    if not id then return end
    if not force and song_id == id and (http_handle or #chunks > 0 or loading()) then return end

    if http_handle then pcall(http_handle.close) end
    song_id = id
    chunks = {}
    http_handle = nil
    read_done = false
    read_start_ms = 0
    buffer_ready_sent = false
    buffer_ready_sent_at = nil
    next_chunk_id = 1
    decoder = dfpwm.make_decoder()
    download_attempts = download_attempts + 1
    http.request({ url = net.format_download_url(id), binary = true })
    dbgmon(("download %s try %d"):format(tostring(id), download_attempts))
end

function loading()
    return song_id ~= nil and not read_done and #chunks == 0 and http_handle == nil and download_attempts > 0
end

local function read_one_chunk()
    if read_done or not http_handle then return false end

    local ok, data = pcall(http_handle.read, CHUNK_BYTES)
    if not ok or not data or #data == 0 then
        read_done = true
        pcall(http_handle.close)
        http_handle = nil
        return false
    end

    table.insert(chunks, { data = data, start_ms = read_start_ms })
    read_start_ms = read_start_ms + chunk_duration_ms(data)

    if not buffer_ready_sent and #chunks >= MIN_BUFFER_CHUNKS then
        buffer_ready_sent = true
        buffer_ready_sent_at = os.epoch("local")
        rednet.send(SERVER_ID, "buffer_ready", REDIONET_PROTO.AUDIO_NEXT)
        dbgmon(("buffer_ready (%d chunks)"):format(#chunks))
    end

    return true
end

local function catch_up_decoder(target_chunk_id)
    for i = 1, target_chunk_id - 1 do
        local entry = chunks[i]
        if not entry then return false end
        decoder(entry.data)
    end
    return true
end

local function seek_to_position(sec)
    local target_chunk = chunk_id_for_position(sec)
    if not chunks[target_chunk] then return false end
    decoder = dfpwm.make_decoder()
    if target_chunk > 1 and not catch_up_decoder(target_chunk) then
        return false
    end
    next_chunk_id = target_chunk
    return true
end

local function play_audio(buffer, volume)
    if not buffer or not speaker or CSTATE.is_paused then return end
    while not speaker.playAudio(buffer, volume) do
        parallel.waitForAny(
            function() os.pullEvent("speaker_audio_empty") end,
            function() os.pullEvent("redionet:playback_stopped") end,
            function() os.pullEvent("redionet:local_stop") end
        )
        if CSTATE.is_paused then return end
    end
end

local function wait_for_play_at(play_at_ms)
    while true do
        local now_ms = server_now_ms()
        if now_ms >= play_at_ms then return now_ms end
        local timer_id = os.startTimer((play_at_ms - now_ms) / 1000)
        local proceed = true
        parallel.waitForAny(
            function()
                repeat
                    local _, tid = os.pullEvent("timer")
                until tid == timer_id
            end,
            function()
                os.pullEvent("redionet:timeline_anchor")
                proceed = false
            end,
            function()
                os.pullEvent("redionet:playback_stopped")
                proceed = false
            end,
            function()
                os.pullEvent("redionet:local_stop")
                proceed = false
            end
        )
        os.cancelTimer(timer_id)
        if not proceed or CSTATE.is_paused then return nil end
    end
end

local function schedule_play()
    if timeline_origin_ms and CSTATE.server_state.status == 1 and not CSTATE.is_paused then
        os.queueEvent("redionet:local_audio_ready")
    end
end

local function play_next_chunk()
    if not speaker or CSTATE.is_paused or not timeline_origin_ms then return end
    if CSTATE.server_state.status ~= 1 then return end

    if next_chunk_id > #chunks then
        if not read_done then
            os.sleep(0.05)
            schedule_play()
        end
        return
    end

    local entry = chunks[next_chunk_id]
    if not entry then
        schedule_play()
        return
    end

    local play_at_ms = timeline_origin_ms + entry.start_ms
    local now_ms = wait_for_play_at(play_at_ms)
    if not now_ms or CSTATE.is_paused or CSTATE.server_state.status ~= 1 then return end

    local target_sec = target_position_sec()
    local chunk_end_sec = (entry.start_ms + chunk_duration_ms(entry.data)) / 1000
    if target_sec > chunk_end_sec + (DRIFT_RESYNC_MS / 1000) then
        dbgmon(("drift seek %0.2fs chunk %d"):format(target_sec, next_chunk_id))
        if seek_to_position(target_sec) then
            schedule_play()
        end
        return
    end

    local buffer = decoder(entry.data)
    play_audio(buffer, CSTATE.server_state.volume or 1.5)
    os.queueEvent("redionet:audio_timestamp", entry.start_ms / 1000)
    next_chunk_id = next_chunk_id + 1
    schedule_play()
end

local M = {}

function M.loop()
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

    while true do
        parallel.waitForAny(
            function()
                while true do
                    local ev = { os.pullEvent() }
                    if not song_id or not ev[1]:find("http") or ev[2] ~= net.format_download_url(song_id) then
                        -- continue
                    elseif ev[1] == "http_success" then
                        http_handle = ev[3]
                        read_done = false
                        download_attempts = 0
                        os.queueEvent("redionet:local_read_chunk")
                    elseif ev[1] == "http_failure" then
                        dbgmon("download failed")
                        http_handle = nil
                        if download_attempts < DOWNLOAD_RETRIES then
                            os.sleep(0.5)
                            start_download(song_id, true)
                        else
                            rednet.send(SERVER_ID, "buffer_failed", REDIONET_PROTO.AUDIO_NEXT)
                        end
                    end
                end
            end,

            function()
                while true do
                    local _, prep_song_id, prep_session_id = os.pullEvent("redionet:prepare_stream")
                    if prep_session_id then session_id = prep_session_id end
                    hard_reset_local()
                    start_download(prep_song_id or (CSTATE.server_state.active_song_meta and CSTATE.server_state.active_song_meta.id), true)
                end
            end,

            function()
                while true do
                    os.pullEvent("redionet:local_stop")
                    hard_reset_local()
                    song_id = nil
                    session_id = nil
                end
            end,

            function()
                while true do
                    os.pullEvent("redionet:local_read_chunk")
                    if read_one_chunk() and not read_done then
                        os.queueEvent("redionet:local_read_chunk")
                    end
                    schedule_play()
                end
            end,

            function()
                while true do
                    local _, anchor_ms, stream_id, chunk_id, origin_ms, server_time_ms, new_session_id =
                        os.pullEvent("redionet:timeline_anchor")
                    sync_clock(server_time_ms)
                    reset_playback(stream_id, chunk_id, origin_ms, new_session_id)
                    if chunk_id and chunk_id > 1 then
                        catch_up_decoder(chunk_id)
                    end
                    dbgmon(("anchor s%s chunk %s"):format(tostring(session_id), tostring(chunk_id)))
                    schedule_play()
                end
            end,

            function()
                while true do
                    os.pullEvent("redionet:local_audio_ready")
                    play_next_chunk()
                end
            end,

            function()
                while true do
                    local state = CSTATE.server_state
                    sync_clock(state.server_time_ms)

                    local meta = state.active_song_meta
                    if state.status == 1 and meta and not CSTATE.is_paused then
                        if meta.id ~= song_id then
                            start_download(meta.id, true)
                        end

                        if state.timeline_origin_ms
                            and timeline_origin_ms ~= state.timeline_origin_ms
                            and #chunks >= MIN_BUFFER_CHUNKS then
                            local chunk_id = chunk_id_for_position(state.audio_position_sec or 0)
                            reset_playback(meta.id, chunk_id, state.timeline_origin_ms, session_id)
                            if chunk_id > 1 then catch_up_decoder(chunk_id) end
                            schedule_play()
                        elseif timeline_origin_ms and #chunks > 0 then
                            local target = target_position_sec()
                            local local_sec = chunks[next_chunk_id] and (chunks[next_chunk_id].start_ms / 1000) or 0
                            if math.abs(target - local_sec) * 1000 > DRIFT_RESYNC_MS then
                                if seek_to_position(target) then
                                    dbgmon(("drift fix %0.2fs"):format(target))
                                    schedule_play()
                                end
                            elseif next_chunk_id > #chunks and not read_done then
                                schedule_play()
                            end
                        elseif #chunks >= MIN_BUFFER_CHUNKS and not timeline_origin_ms and buffer_ready_sent_at then
                            if os.epoch("local") - buffer_ready_sent_at > 3000 then
                                buffer_ready_sent_at = os.epoch("local")
                                rednet.send(SERVER_ID, "buffer_ready", REDIONET_PROTO.AUDIO_NEXT)
                                dbgmon("re-send buffer_ready")
                            end
                        end
                    elseif state.status == 0 then
                        hard_reset_local()
                        song_id = nil
                    end
                    os.sleep(DRIFT_CHECK_SEC)
                end
            end,

            function()
                local id = rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                hard_reset_local()
                song_id = nil
                os.queueEvent("redionet:local_stop")
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
