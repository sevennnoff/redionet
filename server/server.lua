--[[
    Main application file for the music server.
    This file loads all the necessary modules and starts the main server application loops.
]]

peripheral.find("modem", rednet.open)
if not rednet.isOpen() then error("Failed to establish rednet connection. Attach a modem to continue.", 0) end

SERVER_ID = os.getComputerID()
-- note: _could_ support multi-server where clients choose "station" via ID..but seems more trouble than it's worth
REDIONET_PROTO = {
    SERVER = 'RDN:SERVER:v5',
    SERVER_REPLY = 'RDN:SERVER_REPLY:v5',
    SERVER_STATE = 'RDN:SERVER_STATE:v5',
    SERVER_QUEUE = 'RDN:SERVER_QUEUE:v5',
    SERVER_PLAYER = 'RDN:SERVER_PLAYER:v5',
    SERVER_UPDATED = 'RDN:SERVER_UPDATED:v5',
    AUDIO = 'RDN:AUDIO:v5',
    AUDIO_CONNECTION = 'RDN:AUDIO_CONNECTION:v5',
    AUDIO_NEXT = 'RDN:AUDIO_NEXT:v5',
    AUDIO_HALT = 'RDN:AUDIO_HALT:v5',
    AUDIO_STATUS = 'RDN:AUDIO_STATUS:v5',
    CLIENT_SYNC = 'RDN:CLIENT_SYNC:v5',
    COMMAND = 'RDN:COMMAND:v5',
    CHATBOX = 'RDN:CHATBOX:v5',
}
rednet.host(REDIONET_PROTO.SERVER, 'server')

local original_term = term.current() -- chat module will redirect term to designated windows. Store it now for reset on reload


local chat = require('server_lib.chat')
local audio = require("server_lib.audio")
local network = require("server_lib.network")
local auth = require("server_lib.auth")

chat.authorize_rednet_chatbox = function(client_id)
    return auth.is_authorized(client_id)
end

chat.authorize_chat_command = function(user)
    local client_id = tonumber(tostring(user):match("^computer_#(%d+)$"))
    return client_id and auth.is_authorized(client_id)
end


--[[ Global Server State ]]

STATE = {
    -- Audio Network State
    active_stream_id = nil,     -- The MOST important server state value. active_stream_id ~= nil <=> active audio transmission. Excluded from STATE.data to avoid clients ever having stale copy
    response_handle = nil,      -- ReadHandle from http.request containing binary song data. Non-serializable object
    audio_position_epoch_ms = nil,

    -- state that clients receive
    data = {
        -- Playback State
        status = -1,            -- -1=cannot_play/empty/waiting, 0=stopped, 1=streaming 
        queue = {},             -- song queue, list of objects like active_song_meta
        active_song_meta = nil, -- Metadata for the song in the player {id=str, name=str, artist=str, duration={H=int, M=int, S=int}}
        audio_position_sec = 0, -- current server playback position
        loop_mode = 0,          -- 0: Off, 1: Queue/List, 2: Song
        volume = 1.5,           -- server-wide volume, value between 0 and 3
        controller_id = nil,    -- client id that entered the control password

        -- Network Status Info
        is_loading = false,     -- set in `network`, get in client.ui
        error_status = false,   -- PLAYBACK_ERROR, DOWNLOAD_ERROR, false
    }
}


local function extrapolate_audio_position()
    if STATE.data.status == 1 and STATE.audio_position_epoch_ms then
        local elapsed_sec = (os.epoch("local") - STATE.audio_position_epoch_ms) / 1000
        STATE.data.audio_position_sec = math.max(0, STATE.data.audio_position_sec + elapsed_sec)
        STATE.audio_position_epoch_ms = os.epoch("local")
    end
end

---broadcast server state data over the server state protocol
---@param caller_info? string origin debugging info to log
local function broadcast_state(caller_info)
    chat.log_message(('broadcast_state: %s'):format(caller_info), 'DEBUG')
    extrapolate_audio_position()
    -- event data is always copied, client-side mutability not a concern
    rednet.broadcast(STATE.data, REDIONET_PROTO.SERVER_STATE)
end

local function broadcast_legacy_shutdown(reason)
    chat.log_message(("legacy shutdown pulse: %s"):format(reason or "unknown"), "WARN")
    for _ = 1, 3 do
        rednet.broadcast("audio.stop_song", 'PROTO_AUDIO_HALT')
        rednet.broadcast("reboot", 'PROTO_COMMAND')
        os.sleep(0.35)
    end
end

local function dump_state(filename)
    filename = filename or '.redionet.state'
    local state_str = textutils.serialize(STATE.data, {allow_repetitions = true})
    local handle, err = fs.open(filename, 'w')
    handle.write(state_str)
    handle.close()
end

local function restore_state(filename)
    filename = filename or '.redionet.state'
    local handle, err = fs.open(filename, 'r')
    if handle then
        local state_data = textutils.unserialize(handle.readAll())
        handle.close()

        -- stop if was playing. Need time for clients to re-connect
        if state_data.status == 1 then state_data.status = 0 end

        STATE.data.status           = state_data.status
        STATE.data.queue            = state_data.queue
        STATE.data.active_song_meta = state_data.active_song_meta
        STATE.data.audio_position_sec = 0
        STATE.data.loop_mode        = state_data.loop_mode
        STATE.data.volume           = state_data.volume or STATE.data.volume
        -- network status info ignored, irrelevant after reset

        pcall(function() fs.delete(filename) end) -- allow fail without compromising restore
    end
end

--[[ Server Loops ]]
local function print_header_info()
    local repo_url = "github.com/sevennnoff/redionet"
    term.setTextColor(colors.purple)
    print("\n\15 Redionet")
    term.setTextColor(colors.lightGray)
    print(("\161 %s\n"):format(repo_url))
    term.setTextColor(colors.gray)
    print("Help support hosting expenses:\n \x10 patreon.com/exclamatory\n")
end

local function server_loop()
    auth.init()
    print_header_info()
    term.setTextColor(colors.white)
    chat.writeto(('[READY] Server ID: %d\n'):format(os.getComputerID()))
    if auth.state.generated_password then
        chat.writeto(('[AUTH] Control password: %s\n'):format(auth.state.generated_password))
    end
    local initial_clients = { rednet.lookup(REDIONET_PROTO.AUDIO) }
    if #initial_clients > 0 then
        chat.writeto(('Known client IDs: <%s>\n'):format(table.concat(initial_clients, ',\t')))
    end

    broadcast_legacy_shutdown("startup")

    settings.load()
    local rn_config = { -- redionet settings to pass to clients
        ['redionet.log_level'] = settings.get('redionet.log_level', 3),
    }

    pcall(restore_state) -- restore pre-reboot/reload state, if any

    local id, message

    while true do
        parallel.waitForAny(
            function()
                while true do
                    id, message = rednet.receive(REDIONET_PROTO.SERVER) -- General utilities
                    local code, payload
                    if type(message) == "table" then
                        code, payload = table.unpack(message)
                    else
                        code = message
                    end

                    if code == "CONFIG" then
                        rednet.send(id, {code, rn_config}, REDIONET_PROTO.SERVER_REPLY)
                    elseif code == "AUTH" then
                        local ok = auth.verify(id, payload)
                        STATE.data.controller_id = auth.get_controller()
                        rednet.send(id, {code, ok}, REDIONET_PROTO.SERVER_REPLY)
                        os.queueEvent('redionet:broadcast_state', "SERVER: AUTH")
                        chat.log_message(("Client #%d %s control auth"):format(id, ok and "passed" or "failed"), ok and "INFO" or "WARN")
                    elseif code == "PING" then
                        rednet.send(id, {code, "PONG"}, REDIONET_PROTO.SERVER_REPLY)
                    elseif code == "LOG" then
                        chat.log_message(payload, "INFO")
                    end
                end
            end,
            -- TODO: queue / player paths can still race under heavy load.
            function()
                id, message = rednet.receive(REDIONET_PROTO.SERVER_QUEUE) -- Song queue management
                local code, payload = table.unpack(message)

                if not auth.is_authorized(id) then
                    chat.log_message(("Rejected queue command from unauthorized client #%d"):format(id), "WARN")
                    rednet.send(id, {"AUTH_REQUIRED", false}, REDIONET_PROTO.SERVER_REPLY)
                    return
                end

                if code == "ADD" then
                    if payload.type == "playlist" then
                        for _, item in ipairs(payload.playlist_items) do table.insert(STATE.data.queue, item) end
                    else
                        table.insert(STATE.data.queue, payload)
                    end
                elseif code == "NEXT" then
                    if payload.type == "playlist" then
                        for i = #payload.playlist_items, 1, -1 do table.insert(STATE.data.queue, 1, payload.playlist_items[i]) end
                    else
                        table.insert(STATE.data.queue, 1, payload)
                    end
                elseif code == "NOW" then
                    if payload.type == "playlist" then
                        -- play the first song on the list now, enqueue the rest for up next
                        for i = #payload.playlist_items, 2, -1 do table.insert(STATE.data.queue, 1, payload.playlist_items[i]) end
                        audio.play_song(payload.playlist_items[1])
                    else
                        audio.play_song(payload)
                    end
                end

                -- always auto play on Queue update unless stopped
                if STATE.data.status == -1 then
                    STATE.data.status = 1
                    os.queueEvent('redionet:fetch_audio') -- TODO: monitor for interaction with Play Now
                end

                os.queueEvent('redionet:broadcast_state', "SERVER_QUEUE: " .. tostring(code))
            end,

            function()
                id, message = rednet.receive(REDIONET_PROTO.SERVER_PLAYER) -- server playback state management 
                local code, payload = table.unpack(message)
                
                if code then
                    if code == "STATE" then
                        -- Unicast only to the requester; broadcasting on poll floods rednet during playback.
                        extrapolate_audio_position()
                        rednet.send(id, STATE.data, REDIONET_PROTO.SERVER_STATE)
                    elseif not auth.is_authorized(id) then
                        chat.log_message(("Rejected player command from unauthorized client #%d"):format(id), "WARN")
                        rednet.send(id, {"AUTH_REQUIRED", false}, REDIONET_PROTO.SERVER_REPLY)
                    elseif code == "TOGGLE" then
                        audio.toggle_play_pause()
                    elseif code == "SKIP" then
                        audio.skip_song()
                    elseif code == "LOOP" then
                        STATE.data.loop_mode = payload
                        os.queueEvent('redionet:broadcast_state', "SERVER_PLAYER: LOOP")
                    elseif code == "VOLUME" then
                        STATE.data.volume = math.max(0, math.min(3, tonumber(payload) or STATE.data.volume))
                        os.queueEvent('redionet:broadcast_state', "SERVER_PLAYER: VOLUME")
                    elseif code == "SYNC" then
                        audio.state.need_sync = true
                        audio.state.speaker_cache = 0
                        os.queueEvent('redionet:sync')
                        os.queueEvent('redionet:broadcast_state', "SERVER_PLAYER: SYNC")
                    end
                end
            end,
            
            -- Misc Client Communication
            function ()
                local cid, client_file_changes = rednet.receive(REDIONET_PROTO.SERVER_UPDATED)
                local prev_color = term.getTextColor()

                if client_file_changes then
                    term.setTextColor(colors.lime)
                    print(('Client #%d: Updated'):format(cid))
                else
                    term.setTextColor(colors.lightGray)
                    print(('Client #%d: Already up to date'):format(cid))
                end
                term.setTextColor(prev_color)
            end
        )
    end
end

local function server_event_loop()
    while true do
        parallel.waitForAny(
            function ()
                while true do
                    local ev, origin = os.pullEvent('redionet:broadcast_state')
                    broadcast_state(origin)
                end
            end,

            function()
                local ev, cmd = os.pullEvent('redionet:issue_command')

                if cmd == 'help' then
                    -- TODO: bypass issue_command, keep all help display logic in chat module 
                    chat.show_help()
                elseif cmd == 'sync' then
                    audio.state.need_sync = true
                    audio.state.speaker_cache = 0
                    os.queueEvent('redionet:sync')
                elseif cmd == 'killlegacy' then
                    os.queueEvent('redionet:killlegacy')
                else
                    rednet.broadcast(cmd, REDIONET_PROTO.COMMAND)
                    os.queueEvent(('redionet:%s'):format(cmd))
                end
            end,

            function()
                os.pullEvent('redionet:sync')
                audio.state.speaker_cache = 0
                rednet.broadcast('sync', REDIONET_PROTO.CLIENT_SYNC)
            end,

            function()
                os.pullEvent('redionet:killlegacy')
                broadcast_legacy_shutdown("manual")
            end,

            function ()
                os.pullEvent('redionet:update') -- Queued by command `rn update`

                print('Updating...')
                local install_url = "https://raw.githubusercontent.com/sevennnoff/redionet/refs/heads/main/install.lua"
                local tabid = shell.openTab('wget run ' .. install_url)
                shell.switchTab(tabid)
            end,

            function ()
                local _, file_changes = os.pullEvent('redionet:update_complete') -- Queued by install script
                local prev_color = term.getTextColor()

                if file_changes then
                    term.setTextColor(colors.lime)
                    print('Server: Updated')

                    os.queueEvent('redionet:reload')
                else
                    term.setTextColor(colors.lightGray)
                    print('Server: Already up to date')
                    term.setTextColor(prev_color)
                end
            end
        )
    end
end

local on_exit
local function system_stop_event()
    -- The only events that should allow the program to terminate
    parallel.waitForAny(
        function ()
            os.pullEvent('redionet:reload')
            term.redirect(original_term) -- reset so term.current() points to root term
            on_exit = 'reload'
        end,
        function ()
            os.pullEvent('redionet:reboot')
            on_exit = 'reboot'
        end
    )
    pcall(dump_state) -- write state to disk for auto restore
end

-- Start the main loops
parallel.waitForAny(
    system_stop_event,
    server_loop,
    server_event_loop,
    audio.audio_loop,
    chat.chat_loop,
    network.handle_http_download
)

if     on_exit == 'reload' then shell.run('server')
elseif on_exit == 'reboot' then os.reboot()
end
