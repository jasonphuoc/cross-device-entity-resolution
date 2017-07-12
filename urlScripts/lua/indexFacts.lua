local line = KEYS[1]

local main = function()
  local type = "MODELING"
  local obj = cjson.decode(line)
  local uid = obj.uid;
  if redis.call("HEXISTS", type.."_UIDS", uid) == 1 then
      local facts = obj.facts
      for i=1, #facts do
        redis.call("HSET", type.."_UID_TO_FIDS:"..uid, facts[i].fid, 1)
        redis.call("HSET", type.."_FID_TO_UIDS:"..facts[i].fid, uid, 1)
      end
  end

  type = "MATCHING"
  if redis.call("HEXISTS", type.."_UIDS", uid) == 1 then
      local facts = obj.facts
      for i=1, #facts do
        redis.call("HSET", type.."_UID_TO_FIDS:"..uid, facts[i].fid, 1)
        redis.call("HSET", type.."_FID_TO_UIDS:"..facts[i].fid, uid, 1)
      end
  end
end

local status, result = pcall(main)
if status then return result
else error(result) end
