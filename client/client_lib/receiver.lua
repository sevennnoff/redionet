--[[
    Receiver module
    Handles server communications and audio playback.
]]

local speaker = peripheral.find("speaker")


local dbgmon = function (message) end -- debugging func is no op unless conditions are met

local function debug_init()
    settings.load() -- lazy to allow client to inherit from server config as needed
    local monitor = peripheral.find("monitor")

    -- dbgmon redefine conditions: monitor available and log level == debug
    if (monitor and settings.get('redionet.log_level', 3) == 1) then
        local pp = require('cc.pretty')
        monitor.setTextScale(0.5)

        dbgmon = function (message)
            if type(message) == "table" then
                message = pp.render(pp.pretty(message))
            end

            local time_ms = os.epoch("local")
            local time_ms_fmt = ('%s,%03d'):format(os.date("%H:%M:%S", time_ms/1000), time_ms%1000)
            local log_msg = ("[DBG] (%s) %s"):format(time_ms_fmt, message)

            local prev_term = term.redirect(monitor)
            print(log_msg)
            term.redirect(prev_term)
        end
    end
end


local M = {}

---Update local server state cache
---@param blocking? boolean if true, force an update before proceeding, otherwise queue event 
function M.update_server_state(blocking)
    if blocking then
        -- get current server state on join
        rednet.send(SERVER_ID, {"STATE", nil}, REDIONET_PROTO.SERVER_PLAYER)
        local id, server_state = rednet.receive(REDIONET_PROTO.SERVER_STATE)
        CSTATE.server_state = server_state
        CSTATE.is_authorized = server_state.controller_id == CLIENT_ID
        CSTATE.state_received_epoch_ms = os.epoch("local")

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

---@param result table metadata for song or playlist
---@param code string [NOW, NEXT, ADD]
function M.send_server_queue(result, code)
    if not can_control() then return false end
    CSTATE.is_paused = false -- queue manipulation = join session if not already
    rednet.send(SERVER_ID, {code, result},  REDIONET_PROTO.SERVER_QUEUE)
    os.queueEvent('redionet:sync_state')
    return true
end

---@param code string [TOGGLE, SKIP, LOOP, VOLUME, SYNC, STATE]
---@param loop_mode? number loop mode [0,1,2] for server playback (only applicable for code=LOOP)
function M.send_server_player(code, loop_mode)
    if code ~= "STATE" and not can_control() then return false end
    rednet.send(SERVER_ID, {code, loop_mode},  REDIONET_PROTO.SERVER_PLAYER)
    os.queueEvent('redionet:sync_state')
    return true
end

function M.send_server_volume(volume)
    return M.send_server_player("VOLUME", volume)
end

function M.send_server_sync()
    return M.send_server_player("SYNC")
end

function M.toggle_play_local()
    if CSTATE.is_paused or CSTATE.is_paused == nil then -- first click nil
        CSTATE.is_paused = false
        local status = speaker and 1 or -1 -- speakerless = special case: -1. Syncs but doesn't start receiving
        rednet.send(SERVER_ID, status, REDIONET_PROTO.AUDIO_CONNECTION)
    else
        CSTATE.is_paused = true
        if speaker then
            rednet.send(SERVER_ID, 0, REDIONET_PROTO.AUDIO_CONNECTION)
            os.queueEvent("redionet:playback_stopped")
            speaker.stop()
         end
    end
end

local function wait_until_play_at(play_at_ms, state)
    if not play_at_ms then return true end

    while true do
        local remaining_ms = play_at_ms - os.epoch("local")
        if remaining_ms <= 0 then return true end

        parallel.waitForAny(
            function() os.sleep(remaining_ms / 1000) end,
            function()
                os.pullEvent("redionet:playback_stopped")
                state.active_stream_id = "HALT"
            end
        )

        if CSTATE.is_paused or state.active_stream_id ~= state.song_id then
            return false
        end
    end
end

local function play_audio(buffer, state)
    if not buffer or CSTATE.is_paused or state.active_stream_id ~= state.song_id then return end

    if not wait_until_play_at(state.play_at_ms, state) then return end

    local volume = CSTATE.server_state.volume or 1.5
    dbgmon(('- %ds - chunk: %d, song: %s, vol: %0.2f, play_at: %s'):format(
        state.audio_position_sec, state.chunk_id, state.song_id, volume,
        state.play_at_ms and tostring(state.play_at_ms) or "now"))
    os.queueEvent("redionet:audio_timestamp", state.audio_position_sec)

    while not speaker.playAudio(buffer, volume) do
        local t_full = os.epoch('ingame')
        dbgmon('SPEAKER FULL')
        parallel.waitForAny(
            function()
                os.pullEvent("speaker_audio_empty")
                dbgmon(('>>> SPEAKER EMPTY (%sms)'):format((os.epoch('ingame')-t_full)/72))
            end,
            function()
                os.pullEvent("redionet:playback_stopped")
                state.active_stream_id="HALT"
            end
        )
        if CSTATE.is_paused or state.active_stream_id ~= state.song_id then return end
    end
end

local MAX_CLIENT_QUEUE = 8

local function process_chunk(item)
    local buffer, sub_state = table.unpack(item.message)
    play_audio(buffer, sub_state)
end


function M.receive_loop()
    -- prevent audio loop entry if no speaker attached 
    if not speaker then
        while true do
            local ev, side = os.pullEvent('peripheral')
            if peripheral.hasType(side, 'speaker') then
                os.queueEvent('redionet:reload')
            end
        end
    end

    debug_init()

    local chunk_queue = {}

    rednet.send(SERVER_ID, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)

    while true do
        parallel.waitForAny(
            function ()
                while true do
                    local id, message = rednet.receive(REDIONET_PROTO.AUDIO)
                    if CSTATE.is_paused then
                        rednet.send(id, "playback_stopped", REDIONET_PROTO.AUDIO_NEXT)
                    else
                        if #chunk_queue >= MAX_CLIENT_QUEUE then
                            table.remove(chunk_queue, 1)
                            dbgmon('CLIENT QUEUE FULL - dropped oldest chunk')
                        end
                        table.insert(chunk_queue, {id = id, message = message})
                        rednet.send(id, "chunk_received", REDIONET_PROTO.AUDIO_NEXT)
                        os.queueEvent("redionet:audio_chunk_queued")
                    end
                end
            end,

            function ()
                while true do
                    os.pullEvent("redionet:audio_chunk_queued")
                    local item = table.remove(chunk_queue, 1)
                    if item then
                        process_chunk(item)
                    end
                end
            end,
            
            function ()
                while true do
                    os.pullEvent("redionet:playback_stopped")
                    chunk_queue = {}
                end
            end,

            function ()
                while true do
                    local id, message = rednet.receive(REDIONET_PROTO.AUDIO_HALT)
                    chunk_queue = {}
                    speaker.stop()
                    os.queueEvent("redionet:playback_stopped")
                    rednet.send(id, "playback_interrupted", REDIONET_PROTO.AUDIO_NEXT)
                end
            end,

            function ()
                while true do
                    local id, message = rednet.receive(REDIONET_PROTO.AUDIO_STATUS)
                    rednet.send(id, CSTATE.is_paused and 0 or 1, REDIONET_PROTO.AUDIO_CONNECTION)
                end
            end
        )
    end
end

return M
