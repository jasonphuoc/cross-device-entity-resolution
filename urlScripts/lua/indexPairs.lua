local uidOne = KEYS[1]
local uidTwo = ARGV[1]
local type = ARGV[2]

local main = function()
  redis.call("HSET", type.."_UIDS", uidOne, 1)
  redis.call("HSET", type.."_UIDS", uidTwo, 1)
end

local status, result = pcall(main)
if status then return result
else error(result) end
