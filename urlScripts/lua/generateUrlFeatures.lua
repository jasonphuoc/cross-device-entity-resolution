local uidOne = KEYS[1]
local uidTwo = ARGV[1]
local type = ARGV[2]

local isEmpty = function(value)
  return (value == nil) or (value == "") or (cjson.encode(value) == "null") or (value == "null") or (value == "NULL") or (cjson.encode(value) == "NULL")
end

local main = function()
  local fidsOne = redis.call("HKEYS", type.."_UID_TO_FIDS:"..uidOne)
  local fidsTwo = redis.call("HKEYS", type.."_UID_TO_FIDS:"..uidTwo)
  local urlsOne = {}
  local countOne = 1
  local length = {}
  local resultSum = {}

  for i=1, 5 do
    resultSum[i] = 0
  end

  for i=1, #fidsOne do
    local currUrls = redis.call("HKEYS", type.."_FID_TO_URLS:"..fidsOne[i])
    for j=1, #currUrls do
      local _, currLength = string.gsub(currUrls[j], "/", "")
      if (currLength + 1) < 6 then
        urlsOne[countOne] = currUrls[j]
        length[countOne] = currLength + 1
        countOne = countOne + 1
      end
    end
  end

  local count = 0
  for i=1, #urlsOne do
    count = 0
    for j=1, #fidsTwo do
      if redis.call("HEXISTS", type.."_FID_TO_URLS:"..fidsTwo[j], urlsOne[i]) == 1 then
        count = count + 1
      end
    end
    if isEmpty(redis.call("HGET", type.."_URL_COUNTS", urlsOne[i])) ~= true and redis.call("HGET", type.."_URL_COUNTS", urlsOne[i]) ~= true and redis.call("HGET", type.."_URL_COUNTS", urlsOne[i]) ~= false then
      resultSum[length[i]] = resultSum[length[i]] + (count/tonumber(redis.call("HGET", type.."_URL_COUNTS", urlsOne[i])))
    end
  end

  for i=1, 5 do
    resultSum[i] = tostring(resultSum[i])
  end

  return resultSum
end

local status, result = pcall(main)
if status then return result
else error(result) end
