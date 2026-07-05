--[[
    Client-only bass boost (ccmusic-style low-pass reinforcement).
    Applied on speaker slaves before a single playAudio call so barrier sync is unchanged.
]]

local BASS_ALPHA = 0.035
local MIN_BASS = 0.0
local MAX_BASS = 3.0
local BASS_STEP = 0.1

local bassState = 0
local lastSongId = nil

local M = {}

M.BASS_STEP = BASS_STEP
M.MIN_BASS = MIN_BASS
M.MAX_BASS = MAX_BASS

local function clamp(value, minValue, maxValue)
    if value < minValue then return minValue end
    if value > maxValue then return maxValue end
    return value
end

local function clampSample(value)
    if value < -128 then return -128 end
    if value > 127 then return 127 end
    return value
end

local function round1(value)
    return math.floor(value * 10 + 0.5) / 10
end

function M.get()
    settings.load()
    return clamp(tonumber(settings.get("redionet.bass_boost", 0)) or 0, MIN_BASS, MAX_BASS)
end

function M.set(value)
    settings.load()
    settings.set("redionet.bass_boost", round1(clamp(tonumber(value) or 0, MIN_BASS, MAX_BASS)))
    settings.save()
    return M.get()
end

function M.adjust(delta)
    return M.set(M.get() + (delta or BASS_STEP))
end

function M.clear()
    bassState = 0
    lastSongId = nil
end

function M.process(buffer, song_id)
    if not buffer then return buffer end

    if song_id and song_id ~= lastSongId then
        bassState = 0
        lastSongId = song_id
    end

    local boost = M.get()
    if boost <= 0 then return buffer end

    for i, sample in ipairs(buffer) do
        bassState = bassState + BASS_ALPHA * (sample - bassState)
        buffer[i] = clampSample(math.floor(sample + bassState * boost))
    end

    return buffer
end

function M.format_pct()
    return tostring(math.floor(M.get() * 100 + 0.5)) .. "%"
end

return M
