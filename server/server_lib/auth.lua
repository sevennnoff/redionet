--[[
    Auth module
    Stores the control password encrypted on the server and tracks the active controller client.
]]

local M = {}

local AUTH_FILE = ".redionet.auth"
local PASSWORD_LENGTH = 8
local KEY_LENGTH = 32

M.state = {
    authorized_client_id = nil,
    generated_password = nil,
}

local alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"

local function to_hex(data)
    return (data:gsub(".", function(c) return ("%02x"):format(string.byte(c)) end))
end

local function from_hex(hex)
    return (hex:gsub("%x%x", function(cc) return string.char(tonumber(cc, 16)) end))
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

local function random_string(length)
    local out = {}
    for i = 1, length do
        local idx = math.random(1, #alphabet)
        out[i] = alphabet:sub(idx, idx)
    end
    return table.concat(out)
end

local function read_auth_file()
    local handle = fs.open(AUTH_FILE, "r")
    if not handle then return nil end

    local data = textutils.unserialize(handle.readAll())
    handle.close()
    return data
end

local function write_auth_file(data)
    local handle = fs.open(AUTH_FILE, "w")
    handle.write(textutils.serialize(data))
    handle.close()
end

function M.init()
    math.randomseed(os.epoch("local") + os.getComputerID())

    local data = read_auth_file()
    if data and data.key and data.password then return end

    local key = random_string(KEY_LENGTH)
    local password = random_string(PASSWORD_LENGTH)
    write_auth_file({
        key = to_hex(key),
        password = to_hex(xor_crypt(password, key)),
    })

    M.state.generated_password = password
end

function M.verify(client_id, password)
    local data = read_auth_file()
    if not data or type(password) ~= "string" then return false end

    local key = from_hex(data.key)
    local stored_password = xor_crypt(from_hex(data.password), key)

    if password == stored_password then
        M.state.authorized_client_id = client_id
        return true
    end

    return false
end

function M.is_authorized(client_id)
    return M.state.authorized_client_id == client_id
end

function M.get_controller()
    return M.state.authorized_client_id
end

return M
