local fid = KEYS[1]
local url = ARGV[1]

local main = function()
  local type = "MODELING"
  if redis.call("EXISTS", type.."_FID_TO_UIDS:"..fid) == 1 then
    redis.call("HSET", type.."_FID_TO_URLS:"..fid, url, 1)
    redis.call("HINCRBY", type.."_URL_COUNTS", url, 1)
  end

  type = "MATCHING"
  if redis.call("EXISTS", type.."_FID_TO_UIDS:"..fid) == 1 then
    redis.call("HSET", type.."_FID_TO_URLS:"..fid, url, 1)
    redis.call("HINCRBY", type.."_URL_COUNTS", url, 1)
  end
end

local status, result = pcall(main)
if status then return result
else error(result) end
