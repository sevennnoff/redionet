--[[
    Client local audio
    Each speaker downloads from the API and plays on the server timeline.
]]

local net = require("client_lib.net")
local dfpwm = require("cc.audio.dfpwm")

local speaker = peripheral.find("speaker")

local AUDIO_CHUNK_SEC = 2.70
local CHUNK_BYTES = (AUDIO_CHUNK_SEC * 48000) / 8
local LATE_TOLERANCE_MS = 1500

local chunks = {}
local song_id = nil
local loading = false
local timeline_origin_ms = nil
local timeline_stream_id = nil
local next_chunk_id = 1
local decoder = dfpwm.make_decoder()

local dbgmon = function() end

local function chunk_duration_ms(data)
    return (#data * 8 * 1000) / 48000
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

local function reset_playback(stream_id, chunk_id, origin_ms)
    if speaker then speaker.stop() end
    decoder = dfpwm.make_decoder()
    timeline_stream_id = stream_id
    timeline_origin_ms = origin_ms
    next_chunk_id = chunk_id or 1
end

local function start_download(id)
    if not id or loading then return end
    if song_id == id and #chunks > 0 then return end
    song_id = id
    chunks = {}
    loading = true
    next_chunk_id = 1
    decoder = dfpwm.make_decoder()
    http.request({ url = net.format_download_url(id), binary = true })
    dbgmon("download " .. tostring(id))
end

local function play_audio(buffer, volume)
    if not buffer or not speaker or CSTATE.is_paused then return end
    while not speaker.playAudio(buffer, volume) do
        parallel.waitForAny(
            function() os.pullEvent("speaker_audio_empty") end,
            function() os.pullEvent("redionet:playback_stopped") end
        )
        if CSTATE.is_paused then return end
    end
end

local function catch_up_decoder(target_chunk_id)
    for i = 1, target_chunk_id - 1 do
        local entry = chunks[i]
        if not entry then return false end
        decoder(entry.data)
    end
    return true
end

local function wait_for_play_at(play_at_ms)
    while true do
        local now_ms = os.epoch("local")
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
            end
        )
        os.cancelTimer(timer_id)
        if not proceed or CSTATE.is_paused then return nil end
    end
end

local function play_next_chunk()
    if not speaker or CSTATE.is_paused or not timeline_origin_ms then return end
    if CSTATE.server_state.status ~= 1 then return end
    if next_chunk_id > #chunks then return end

    local entry = chunks[next_chunk_id]
    if not entry then return end

    local play_at_ms = timeline_origin_ms + entry.start_ms
    local now_ms = wait_for_play_at(play_at_ms)
    if not now_ms or CSTATE.is_paused or CSTATE.server_state.status ~= 1 then return end

    if now_ms > play_at_ms + LATE_TOLERANCE_MS then
        dbgmon(("late chunk %d by %dms, catch-up decode"):format(next_chunk_id, now_ms - play_at_ms))
        if not catch_up_decoder(next_chunk_id) then return end
    end

    local buffer = decoder(entry.data)
    play_audio(buffer, CSTATE.server_state.volume or 1.5)
    os.queueEvent("redionet:audio_timestamp", entry.start_ms / 1000)
    next_chunk_id = next_chunk_id + 1

    if next_chunk_id <= #chunks and CSTATE.server_state.status == 1 and not CSTATE.is_paused then
        os.queueEvent("redionet:local_audio_ready")
    end
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
                    if ev[1]:find("http") and song_id and ev[2] == net.format_download_url(song_id) then
                        loading = false
                        if ev[1] == "http_success" then
                            local handle = ev[3]
                            local start_ms = 0
                            chunks = {}
                            while true do
                                local ok, data = pcall(handle.read, CHUNK_BYTES)
                                if not ok or not data or #data == 0 then break end
                                table.insert(chunks, { data = data, start_ms = start_ms })
                                start_ms = start_ms + chunk_duration_ms(data)
                            end
                            pcall(handle.close)
                            dbgmon(("buffered %d chunks"):format(#chunks))
                            os.queueEvent("redionet:local_audio_ready")
                        else
                            dbgmon("download failed")
                        end
                    end
                end
            end,

            function()
                while true do
                    local _, anchor_ms, stream_id, chunk_id, origin_ms = os.pullEvent("redionet:timeline_anchor")
                    reset_playback(stream_id, chunk_id, origin_ms)
                    if chunk_id and chunk_id > 1 then
                        catch_up_decoder(chunk_id)
                    end
                    dbgmon(("anchor chunk=%s origin=%s"):format(tostring(chunk_id), tostring(origin_ms)))
                    os.queueEvent("redionet:local_audio_ready")
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
                    if state.status == 1 and state.active_song_meta and not CSTATE.is_paused then
                        start_download(state.active_song_meta.id)
                        if state.timeline_origin_ms and state.timeline_origin_ms ~= timeline_origin_ms then
                            local chunk_id = math.max(1, math.floor((state.audio_position_sec or 0) / AUDIO_CHUNK_SEC) + 1)
                            reset_playback(state.active_song_meta.id, chunk_id, state.timeline_origin_ms)
                            if chunk_id > 1 then catch_up_decoder(chunk_id) end
                            os.queueEvent("redionet:local_audio_ready")
                        end
                    elseif state.status == 0 and speaker then
                        speaker.stop()
                        next_chunk_id = 1
                    end
                    os.sleep(0.25)
                end
            end,

            function()
                local id = rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                if speaker then speaker.stop() end
                chunks = {}
                song_id = nil
                loading = false
                next_chunk_id = 1
                timeline_origin_ms = nil
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
