-- 获取锁标识，是否与当前线程一致？
if(redis.call('get', KEYS[1]) == ARGV[1]) then
    -- 一致，删除
    return redis.call('del', KEYS[1])
end
-- 不一致，直接返回
return 0