--[[
    Main application file for the music client.
    This file loads all the necessary modules and starts the main client application loops.
]]

peripheral.find("modem", rednet.open)
if not rednet.isOpen() then error("Failed to establish rednet connection. Attach a modem to continue.", 0) end

settings.load()

SERVER_ID = nil     -- set in setup_server_connection

CLIENT_ID = os.getComputerID()
HOST_NAME = 'client_'..CLIENT_ID
DEVICE_TYPE = settings.get('redionet.device_type', 'client')
IS_CONTROLLER = DEVICE_TYPE == 'controller'
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
}

local ui = require("client_lib.ui")
local receiver = require("client_lib.receiver")
local net = require('client_lib.net')


--[[ Global Client State]]
CSTATE = {
    last_search_query = nil,    -- set in `net`, used in `net` and `ui`  
    search_results = nil,       -- list of at most 21 song_meta tables
    is_paused = false,          -- if true, client stops processing music data transmissions
    is_authorized = false,      -- true after this client enters the server control password
    is_controller = IS_CONTROLLER,
    error_status = false,       -- SEARCH_ERROR, false
    server_state = {
        active_song_meta = nil,
        audio_position_sec = 0,
        queue = {},
        is_loading = false,
        loop_mode = 0,
        volume = 1.5,
        controller_id = nil,
        status = -1,
        error_status = false,
    }
}




local speaker = peripheral.find("speaker")
local monitor = peripheral.find("monitor")

local function format_time(seconds)
    seconds = math.floor(seconds or 0)
    return ("%d:%02d"):format(math.floor(seconds / 60), seconds % 60)
end

local function write_line(y, label, value, color)
    term.setCursorPos(1, y)
    term.clearLine()
    term.setTextColor(colors.gray)
    term.write(label)
    term.setTextColor(color or colors.white)
    term.write(value)
end

local function draw_player_status()
    if IS_CONTROLLER then return end

    term.setBackgroundColor(colors.black)
    term.clear()

    term.setCursorPos(1, 1)
    term.setTextColor(colors.purple)
    term.write("\15 Redionet")

    term.setCursorPos(1, 2)
    term.setTextColor(colors.lime)
    term.write("Connection established")

    write_line(4, "Client: ", ("#%d player"):format(CLIENT_ID))
    write_line(5, "Server: ", SERVER_ID and ("#%d"):format(SERVER_ID) or "unknown")
    write_line(6, "Speaker: ", peripheral.find("speaker") and "connected" or "missing", peripheral.find("speaker") and colors.lime or colors.orange)

    local status = CSTATE.server_state.status
    local status_text = status == 1 and "streaming" or status == 0 and "stopped" or "waiting"
    local status_color = status == 1 and colors.lime or status == 0 and colors.red or colors.lightGray
    write_line(8, "Server: ", status_text, status_color)
    write_line(9, "Volume: ", ("%d%%"):format(math.floor(100 * ((CSTATE.server_state.volume or 0) / 3) + 0.5)))

    local song = CSTATE.server_state.active_song_meta
    if song then
        write_line(11, "Track: ", song.name or "unknown")
        write_line(12, "Artist: ", song.artist or "unknown", colors.lightGray)
        write_line(13, "Time: ", format_time(CSTATE.server_state.audio_position_sec))
    else
        write_line(11, "Track: ", "none", colors.lightGray)
    end

    local err = CSTATE.server_state.error_status or CSTATE.error_status
    if CSTATE.server_state.is_loading then
        write_line(15, "State: ", "loading", colors.lightGray)
    elseif err then
        write_line(15, "Error: ", err, colors.red)
    end
end


local function warn_speaker()
    -- Recent CC:tweaked versions may support two peripherals on pocket 
    -- https://github.com/cc-tweaked/CC-Tweaked/commit/0a0c80d
    local no_warn = pocket and not pocket.equipBottom
    if no_warn then
        print('Pocket Client (no audio)')
    else
        local prev_color = term.getTextColor()
        term.setTextColor(colors.orange)
        print('WARN: No speaker attached. To receive audio on this device, attach speaker and reboot.')
        term.setTextColor(prev_color)
    end
end

local function setup_server_connection()
    write('Waiting for server connection')
    local id, server_settings

    local function wait_server_reply(timeout)
        local timer = os.startTimer(timeout or 1)

        while true do
            local event, p1, p2, p3 = os.pullEvent()
            if event == "timer" and p1 == timer then
                return nil
            elseif event == "rednet_message" and p3 == REDIONET_PROTO.SERVER_REPLY and type(p2) == "table" then
                return p1, p2
            end
        end
    end

    local payload, code
    repeat
        write(".")
        rednet.broadcast("CONFIG", REDIONET_PROTO.SERVER)

        id, payload = wait_server_reply(1)
        if payload then
            code, server_settings = table.unpack(payload)
            if code ~= "CONFIG" then
                server_settings = nil
            end
        end
    until code == "CONFIG"

    write("\n")

    SERVER_ID = id

    return server_settings
end

if not IS_CONTROLLER then
    if speaker then rednet.host(REDIONET_PROTO.AUDIO, HOST_NAME) else warn_speaker() end
end
-- check speaker before connect to server to extend time warning visible
local server_settings = setup_server_connection()
draw_player_status()


-- inherit client log level from server unless set locally
if not settings.get('redionet.log_level') then
    settings.set('redionet.log_level', server_settings['redionet.log_level'])
    settings.save()
end


--[[ Client Loops ]]

local function client_loop()
    speaker = peripheral.find("speaker")
    while true do
        parallel.waitForAny(
            --[[
                Client Event
            ]]
            function ()
                os.pullEvent('peripheral_detach')
                -- speaker or modem detached
                if (speaker and not peripheral.find('speaker')) or not peripheral.find("modem") then
                    os.queueEvent('redionet:reload')
                end
            end,

            --[[
                Client Event -> Server Message 
            ]]
            function ()
                os.pullEvent('redionet:sync_state')
                rednet.send(SERVER_ID, {"STATE", nil}, REDIONET_PROTO.SERVER_PLAYER)
            end,
            --[[
                Server Message -> Client Event
            ]]
            function ()
                local id, server_state = rednet.receive(REDIONET_PROTO.SERVER_STATE)
                CSTATE.server_state = server_state
                CSTATE.is_authorized = server_state.controller_id == CLIENT_ID
                if IS_CONTROLLER then
                    os.queueEvent('redionet:redraw_screen')
                else
                    draw_player_status()
                end
            end,
            
            function ()
                local id, command = rednet.receive(REDIONET_PROTO.COMMAND)

                if command == 'sync' then
                    -- no op, 
                    -- server broadcasts CLIENT_SYNC
                
                elseif command == 'reboot' then
                    if monitor then monitor.clear() end
                    os.queueEvent('redionet:reboot')
                
                elseif command == 'reload' then
                    os.queueEvent('redionet:reload')
                
                elseif command == 'update' then
                    local install_url = "https://raw.githubusercontent.com/sevennnoff/redionet/refs/heads/main/install.lua"
                    local tabid = shell.openTab('wget run ' .. install_url)
                    shell.switchTab(tabid)

                    local _, file_changes = os.pullEvent('redionet:update_complete') -- Queued by install script
                    rednet.send(SERVER_ID, file_changes, REDIONET_PROTO.SERVER_UPDATED)
                    
                    if file_changes then
                        os.queueEvent('redionet:reload')
                    end
                end
            end,

            --[[
                (Peer|Server) Message -> Client Event 
            ]]
            function ()
                -- flush the other speaker buffers whenever a client resumes play
                -- this forces all clients to remain in sync
                local id = rednet.receive(REDIONET_PROTO.CLIENT_SYNC)
                if speaker then
                    speaker.stop()
                    os.queueEvent("redionet:playback_stopped")
                end
            end,

            function ()
                if IS_CONTROLLER then
                    while true do os.pullEvent("redionet:player_status_tick") end
                end

                while true do
                    os.sleep(1)
                    os.queueEvent('redionet:sync_state')
                end
            end
        )

    end

end

receiver.update_server_state(true) -- get initial server state before proceeding

local on_exit
local function system_stop_event()
    -- The only events that should allow the program to terminate
    parallel.waitForAny(
        function ()
            os.pullEvent('redionet:reload')
            on_exit = 'reload'
            term.setCursorPos(1, 1)
            term.setBackgroundColor(colors.black)
            term.clear()
        end,
        function ()
            os.pullEvent('redionet:reboot')
            on_exit = 'reboot'
        end
    )
end

-- Start main client loops
if IS_CONTROLLER then
    parallel.waitForAny(
        system_stop_event,
        client_loop,
        ui.ui_loop,
        net.http_search_loop
    )
else
    parallel.waitForAny(
        system_stop_event,
        client_loop,
        receiver.receive_loop
    )
end

if     on_exit == 'reload' then shell.run('client')
elseif on_exit == 'reboot' then os.reboot()
end
