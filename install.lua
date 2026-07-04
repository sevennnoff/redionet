--[[
_ _  _ ____ ___ ____ _    _    ____ ____
| |\ | [__   |  |__| |    |    |___ |__/
| | \| ___]  |  |  | |___ |___ |___ |  \

Github Repository: https://github.com/sevennnoff/redionet

]]
-- Install script based on: https://github.com/CC-YouCube/installer/blob/main/src/installer.lua
-- License: GPL-3.0
-- OpenInstaller v1.0.0 (based on wget)
local prog_args = { ... }


local BASE_URL = "https://raw.githubusercontent.com/sevennnoff/redionet/refs/heads/main/"
local INSTALL_VERSION = "2026-07-04-play-at-sync"

local filemap = {}

filemap["server"] = {
    ["./server.lua"] = BASE_URL ..              "server/server.lua",
    ["./server_lib/audio.lua"] = BASE_URL ..    "server/server_lib/audio.lua",
    ["./server_lib/auth.lua"] = BASE_URL ..     "server/server_lib/auth.lua",
    ["./server_lib/chat.lua"] = BASE_URL ..     "server/server_lib/chat.lua",
    ["./server_lib/network.lua"] = BASE_URL ..  "server/server_lib/network.lua",
}

filemap["client"] = {
    ["./client.lua"] = BASE_URL ..              "client/client.lua",
    ["./client_lib/net.lua"] = BASE_URL ..      "client/client_lib/net.lua",
    ["./client_lib/receiver.lua"] = BASE_URL .. "client/client_lib/receiver.lua",
    ["./client_lib/ui.lua"] = BASE_URL ..       "client/client_lib/ui.lua",
}

filemap["controller"] = {
    ["./client.lua"] = BASE_URL ..              "client/client.lua",
    ["./client_lib/net.lua"] = BASE_URL ..      "client/client_lib/net.lua",
    ["./client_lib/receiver.lua"] = BASE_URL .. "client/client_lib/receiver.lua",
    ["./client_lib/ui.lua"] = BASE_URL ..       "client/client_lib/ui.lua",
}

local function load_settings(verbose)
    settings.define("redionet.device_type", {
        description = "Designation for this computer. 'client', 'controller', or 'server'",
        type = "string",
    })
    settings.define("redionet.run_on_boot", {
        description = "Whether to autorun on computer startup",
        type = "boolean",
    })
    settings.define("redionet.log_level", {
        description = "Minimum severity to show in server console. 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR. (default=3)",
        default = 3,
        type = "number",
    })
    -- *very* important to load before calling settings.save
    -- save overwrites the file, deleting anything not defined. 
    settings.load()

    if not verbose then return end

    -- print config if verbose
    local key_values = {}

    for _,option in ipairs(settings.getNames()) do
        local i_end = select(2, string.find(option, 'redionet'))
        if i_end then
            table.insert(key_values, {(" %s"):format(option:sub(i_end+1)), ("= %s"):format(settings.get(option))})
        end
    end
    if #key_values > 0 then
        term.setTextColor(colors.cyan)
        print('Redionet Settings') -- \149 

        term.setTextColor(colors.lightGray)
        print('redionet')
        textutils.tabulate(table.unpack(key_values))
        
        term.setTextColor(colors.white)
        write('press any key to continue..')
        term.setCursorBlink(true)
        os.pullEvent('key')
        write('\n')
        term.setCursorBlink(false)
    end
end


local function tableContains(_table, element)
    for _, value in pairs(_table) do
        if value == element then
            return true
        end
    end
    return false
end

local function writeColoured(text, colour)
    term.setTextColour(colour)
    write(text)
end

local function write_wrapped(text, x, width)
    x = x or 1
    width = width or select(1, term.getSize()) - x + 1
    local line = ""

    for word in tostring(text):gmatch("%S+") do
        if #line == 0 then
            line = word
        elseif #line + #word + 1 <= width then
            line = line .. " " .. word
        else
            term.setCursorPos(x, select(2, term.getCursorPos()))
            print(line)
            line = word
        end
    end

    if #line > 0 then
        term.setCursorPos(x, select(2, term.getCursorPos()))
        print(line)
    end
end

local function tf_question(message)
    local previous_colour = term.getTextColour()

    writeColoured(message .. " ", colors.cyan)
    term.blit("[Y/n] ", "050e0f", "ffffff") -- 0-white, 5-lime, e-red; f-black
    -- Reset colour
    term.setTextColour(colors.white)
    local c_x, c_y = term.getCursorPos()
    local input_char = read():sub(1, 1):lower()
    
    local accept_chars = { "o", "k", "y", "" }
    if input_char=="" then -- show the default
        term.setCursorPos(c_x, c_y)
        term.blit("Y", "5", "f")
        term.setCursorPos(1, c_y+1)
    end
    term.setTextColour(previous_colour)

    return tableContains(accept_chars, input_char)
end

local function mc_question(prompt_text, options, active_idx, details)
    active_idx = active_idx or 1
    local x,y = term.getCursorPos()
    local w,h = term.getSize()

    term.setBackgroundColor(colors.black)
    term.setTextColor(colors.cyan)
    term.clearLine()
    write_wrapped(prompt_text, 1, w)
    
    for i,opt in ipairs(options) do
        term.setCursorPos(2, y+i)
        if i == active_idx then
            term.setBackgroundColor(colors.white)
            term.setTextColour(colors.gray)
        else
            term.setBackgroundColor(colors.black)
            term.setTextColour(colors.white)
        end
        term.clearLine()
        write(opt)
    end

    local detail_y = y + #options + 1
    for row = detail_y, h - 1 do
        term.setCursorPos(1, row)
        term.setBackgroundColor(colors.black)
        term.clearLine()
    end

    if details and details[active_idx] then
        term.setCursorPos(1, detail_y)
        term.setBackgroundColor(colors.black)
        term.setTextColor(colors.lightGray)
        write_wrapped(details[active_idx], 1, w)
    end

    local key_name
    repeat
        local ev, key, is_held = os.pullEvent("key")
        key_name = keys.getName(key)
        if key_name == "up" then
            active_idx = active_idx > 1 and active_idx-1 or #options
            term.setCursorPos(1, y)
            return mc_question(prompt_text, options, active_idx, details)
        elseif key_name == "down" then
            active_idx = 1 + (active_idx % #options)
            term.setCursorPos(1, y)
            return mc_question(prompt_text, options, active_idx, details)
        end
    until key_name == "enter"

    term.setBackgroundColor(colors.black)
    term.setCursorPos(1, h)

    return active_idx
end

local function choose_device_type()
    local options = { "Client", "Controller", "Server" }
    local device_types = { "client", "controller", "server" }
    local details = {
        "Client: passive speaker node. It only receives synchronized audio from the server and does not provide search or playback controls.",
        "Controller: remote control UI. It can search, manage queue, playback, loop, and server-wide volume after password auth. It does not play audio.",
        "Server: central Redionet host. Install only one server per world.",
    }

    while true do
        local choice_idx = mc_question("Assign this computer as", options, 1, details)
        local device_type = device_types[choice_idx]

        term.clear()
        term.setCursorPos(1, 1)
        term.setTextColor(colors.cyan)
        write("Selected: ")
        term.setTextColor(colors.white)
        write_wrapped(details[choice_idx], 1, select(1, term.getSize()))

        if tf_question("Confirm selection?") then
            return device_type
        end

        term.clear()
        term.setCursorPos(1, 1)
    end
end

local function check_requirements()
    if not http then
        printError("OpenInstaller requires the http API")
        printError("Set http.enabled to true in the ComputerCraft config")
        error("http disabled.", 0)
    end

    local ok, dfpwm = pcall(require, "cc.audio.dfpwm")
    if not ok then
        printError("DFPWM required (CC version: 0.100.0 and later)")
        printError("Version found: ".._HOST)

        if not tf_question("Download anyway?") then
            error("Aborted.", 0)
        end
    end
    
end

local function http_get(url)
    local url_sep = url:find("?", 1, true) and "&" or "?"
    url = url .. url_sep .. "rn_v=" .. textutils.urlEncode(INSTALL_VERSION)

    local valid_url, error_message = http.checkURL(url)
    if not valid_url then
        printError(('"%s" %s.'):format(url, error_message or "Invalid URL"))
        return
    end

    local response, http_error_message = http.get(url, nil, true)
    if not response then
        printError(('Failed to download "%s" (%s).'):format(url, http_error_message or "Unknown error"))
        return
    end

    local response_body = response.readAll()
    response.close()

    if not response_body then
        printError(('Failed to download "%s" (Empty response).'):format(url))
    end

    return response_body
end

local function write_file(response_body, resolved_path)
    local parent = fs.getDir(resolved_path)
    if parent and #parent > 0 and not fs.exists(parent) then
        fs.makeDir(parent)
    end

    local file, file_open_error_message = fs.open(resolved_path, "wb")
    if not file then
        error(('Failed to save "%s" (%s).'):format(resolved_path, file_open_error_message or "Unknown error"), 0)
    end

    file.write(response_body)
    file.close()
end

local function all_install_paths()
    local paths = {
        "./startup/init.lua",
        "./.redionet.state",
        "./.redionet.auth",
        "./.logs/server.log",
    }
    local seen = {}

    for _, path in ipairs(paths) do seen[path] = true end

    for _, files in pairs(filemap) do
        for path in pairs(files) do
            if not seen[path] then
                table.insert(paths, path)
                seen[path] = true
            end
        end
    end

    return paths
end

local function clean_previous_install()
    term.setTextColor(colors.yellow)
    print("Cleaning previous Redionet files..")

    for _, path in ipairs(all_install_paths()) do
        local resolved_path = shell.resolve(path)
        if fs.exists(resolved_path) then
            fs.delete(resolved_path)
            writeColoured(('Deleted "%s"\n'):format(path), colors.lightGray)
        end
    end
end

local function to_hex(data)
    return (data:gsub(".", function(c) return ("%02x"):format(string.byte(c)) end))
end

local function bxor(a, b)
    if bit32 and bit32.bxor then return bit32.bxor(a, b) end

    local result = 0
    local bit = 1
    while a > 0 or b > 0 do
        local abit = a % 2
        local bbit = b % 2
        if abit ~= bbit then result = result + bit end
        a = math.floor(a / 2)
        b = math.floor(b / 2)
        bit = bit * 2
    end
    return result
end

local function xor_crypt(data, key)
    local out = {}
    for i = 1, #data do
        local kb = string.byte(key, ((i - 1) % #key) + 1)
        out[i] = string.char(bxor(string.byte(data, i), kb))
    end
    return table.concat(out)
end

local function random_key(length)
    local out = {}
    math.randomseed(os.epoch("local") + os.getComputerID())
    for i = 1, length do
        out[i] = string.char(math.random(33, 126))
    end
    return table.concat(out)
end

local function write_auth_file(password)
    local key = random_key(32)
    local resolved_path = shell.resolve("./.redionet.auth")
    write_file(textutils.serialize({
        key = to_hex(key),
        password = to_hex(xor_crypt(password, key)),
    }), resolved_path)
end

local function setup_server_password()
    while true do
        term.setTextColor(colors.cyan)
        write("Control password: ")
        term.setTextColor(colors.white)
        local password = read("*")

        if not password or #password == 0 then
            term.setTextColor(colors.red)
            print("Password cannot be empty.")
        else
            term.setTextColor(colors.cyan)
            write("Confirm password: ")
            term.setTextColor(colors.white)
            local confirm = read("*")

            if password == confirm then
                write_auth_file(password)
                term.setTextColor(colors.lime)
                print("Control password saved.")
                return
            end

            term.setTextColor(colors.red)
            print("Passwords do not match.")
        end
    end
end


local function check_peripherals(device_type)
    -- https://www.reddit.com/r/ComputerCraft/comments/1cc2y94/cc_character_cheat_sheet/#lightbox
    local function locate(peripheral_type)
        if peripheral.find(peripheral_type) then
            writeColoured(('ok - %s: Detected\n'):format(peripheral_type), colors.lime) --\215 )\16
            return true
        end
        return false
    end

    if not locate("modem") then
        writeColoured(('\19 - %s: Missing. Attach before running!\n'):format("modem"), colors.red)
    end

    if device_type == 'server' then
        if not locate("chatBox") then
            writeColoured(('\186 - %s: Missing (optional)\n'):format("chatBox"), colors.lightBlue)
            -- Attach for song announcements. (requires Advanced Peripherals mod) \21
        end
        if not locate("playerDetector") then
            writeColoured(('\186 - %s: Missing (optional)\n'):format("playerDetector"), colors.lightBlue)
            -- Attach for fancy song announcements. (requires Advanced Peripherals mod) \177
        end
    elseif device_type == 'client' then
        local pocket_client = pocket and not pocket.equipBottom
        if pocket_client then
            writeColoured(('Pocket Client (no audio)\n'), colors.green)
        elseif not locate("speaker") then
            writeColoured(('\15 - %s: Missing. Attach to play music.\n'):format("speaker"), colors.orange)
        end
    elseif device_type == 'controller' then
        writeColoured(('Controller mode: speaker not required.\n'), colors.green)
    end
end



local function fresh_install()
    term.clear()
    term.setCursorPos(1, 1)
    check_requirements()
    local repo_url = "github.com/sevennnoff/redionet"
    writeColoured("\15 Redionet\n", colors.purple)
    writeColoured(("\161 %s\n"):format(repo_url), colors.lightGray)
    term.setTextColor(colors.white)

    local device_type = choose_device_type()
    settings.set('redionet.device_type', device_type)

    clean_previous_install()
    
    local files = filemap[device_type]
    
    local run_on_boot = tf_question('Run on startup?')
    settings.set('redionet.run_on_boot', run_on_boot)

    if run_on_boot then
        local startup_type = device_type == "controller" and "controller" or device_type
        files["./startup/init.lua"] = BASE_URL ..  startup_type ..  "/startup/init.lua"
    end

    for path, download_url in pairs(files) do
        local resolved_path = shell.resolve(path)
        local response_body = http_get(download_url)

        write_file(response_body, resolved_path)

        term.setTextColour(colors.lime)
        print(('Downloaded "%s"'):format(path))
    end

    if device_type == "server" then
        setup_server_password()
    end

    term.setTextColor(colors.white)
    print("Done! Checking peripherals..")
    check_peripherals(device_type)

    term.setTextColor(colors.lightGray)
    local program_name = device_type == "server" and "server" or "client"
    print('\n' .. 'To execute program: ' .. (run_on_boot and "Reboot computer now" or ("Run `%s` in terminal"):format(program_name)))

    settings.save()
end

local function update(device_type)
    local files = filemap[device_type]
    
    local run_on_boot = settings.get('redionet.run_on_boot', fs.exists(shell.resolve("./startup/init.lua")))

    if run_on_boot then
        local startup_type = device_type == "controller" and "controller" or device_type
        files["./startup/init.lua"] = BASE_URL ..  startup_type ..  "/startup/init.lua"
    end
    
    local files_updated = false

    for path, download_url in pairs(files) do
        local resolved_path = shell.resolve(path)
        local response_body = http_get(download_url)

        local file, fopen_error = fs.open(resolved_path, 'rb')
        local cur_contents
        if file then
            cur_contents = file.readAll()
            file.close()
        end
        
        if cur_contents and cur_contents == response_body then
            writeColoured(('Up to date: "%s"\n'):format(path), colors.lightGray)
        else
            write_file(response_body, resolved_path)
            writeColoured(('Updated: "%s"\n'):format(path), colors.lime)
            files_updated = true
        end
    end

    return files_updated
end

local function parse_cli_flags()
    local flags = {
        force = false,
        update = false,
        verbose = false,
    }
    for _, value in pairs(prog_args) do
        if value == "-f" or value == "--force-reinstall" then flags.force = true
        elseif value == "--update" then flags.update = true
        elseif value == "-v" or value == "--verbose" then flags.verbose = true
        end
    end
    return flags
end

local function main()
    local flags = parse_cli_flags()

    load_settings(flags.verbose)
    local device_type = settings.get('redionet.device_type')

    local file_changes = true
    if flags.force or not device_type then
        fresh_install()
    else
        file_changes = update(device_type)
    end

    os.queueEvent('redionet:update_complete', file_changes)
end

main()
