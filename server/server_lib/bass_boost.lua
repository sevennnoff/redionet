--[[
    Bass boost DSP (ccmusic-style low-pass reinforcement).
    Applied on the server so every client receives identical PCM.
]]

local BASS_ALPHA = 0.035

local bassState = 0
local lastSongId = nil

local M = {}

local function clampSample(value)
    if value < -128 then return -128 end
    if value > 127 then return 127 end
    return value
end

function M.clear()
    bassState = 0
    lastSongId = nil
end

function M.process(buffer, song_id, boost)
    if not buffer then return buffer end

    boost = tonumber(boost) or 0
    if boost <= 0 then return buffer end

    if song_id and song_id ~= lastSongId then
        bassState = 0
        lastSongId = song_id
    end

    for i, sample in ipairs(buffer) do
        bassState = bassState + BASS_ALPHA * (sample - bassState)
        buffer[i] = clampSample(math.floor(sample + bassState * boost))
    end

    return buffer
end

return M
