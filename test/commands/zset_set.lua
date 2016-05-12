--[[   --]]
ardb.call("del", "myzset")
local s = ardb.call("zadd", "myzset", "1", "one")
ardb.assert2(s == 1, s)
s = ardb.call("zadd", "myzset", "1", "uno")
ardb.assert2(s == 1, s)
s = ardb.call("zadd", "myzset", "2", "two", "3", "three")
ardb.assert2(s == 2, s)
local vs = ardb.call("zrange", "myzset", "0", "-1", "WITHSCORES")
ardb.assert2(table.getn(vs) == 8, vs)
ardb.assert2(vs[1] == "one", vs)
ardb.assert2(vs[2] == "1", vs)
ardb.assert2(vs[3] == "uno", vs)
ardb.assert2(vs[4] == "1", vs)
ardb.assert2(vs[5] == "two", vs)
ardb.assert2(vs[6] == "2", vs)
ardb.assert2(vs[7] == "three", vs)
ardb.assert2(vs[8] == "3", vs)
s = ardb.call("zcard", "myzset")
ardb.assert2(s == 4, s)
s = ardb.call("zcount", "myzset", "-inf", "+inf")
ardb.assert2(s == 4, s)
s = ardb.call("zcount", "myzset", "(1", "3")
ardb.assert2(s == 2, s)
vs = ardb.call("zrangebyscore", "myzset", "(1", "3", "WITHSCORES")
ardb.assert2(table.getn(vs) == 4, vs)
ardb.assert2(vs[1] == "two", vs)
ardb.assert2(vs[2] == "2", vs)
ardb.assert2(vs[3] == "three", vs)
ardb.assert2(vs[4] == "3", vs)
vs = ardb.call("zrevrangebyscore", "myzset", "3", "(1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "three", vs)
ardb.assert2(vs[2] == "two", vs)
vs = ardb.call("zrangebyscore", "myzset", "(1", "(2")
ardb.assert2(table.getn(vs) == 0, vs)
s = ardb.call("zincrby", "myzset", "2", "one")
ardb.assert2(s == "3", s)
s = ardb.call("zscore", "myzset", "one")
ardb.assert2(s == "3", s)
vs = ardb.call("zrange", "myzset", "0", "-1", "WITHSCORES")
ardb.assert2(vs[5] == "one", vs)
ardb.assert2(vs[6] == "3", vs)
s = ardb.call("zrank", "myzset", "one")
ardb.assert2(s == 2, s)
s = ardb.call("zrevrank", "myzset", "one")
ardb.assert2(s == 1, s)
s = ardb.call("zrank", "myzset", "not_exist")
ardb.assert2(s == false, s)
s = ardb.call("zrem", "myzset", "not_exist", "two", "one")
ardb.assert2(s == 2, s)
vs = ardb.call("zrange", "myzset", "0", "-1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "uno", vs)
ardb.assert2(vs[2] == "three", vs)
vs = ardb.call("zrevrange", "myzset", "0", "-1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "three", vs)
ardb.assert2(vs[2] == "uno", vs)

ardb.call("del", "myzset")
ardb.call("zadd", "myzset", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f", "0", "g")
s = ardb.call("zlexcount", "myzset", "-", "+")
ardb.assert2(s == 7, s)
s = ardb.call("zlexcount", "myzset", "[b", "[f")
ardb.assert2(s == 5, s)
vs = ardb.call("zrangebylex", "myzset", "[aaa", "(g")
ardb.assert2(table.getn(vs) == 5, vs)
ardb.assert2(vs[1] == "b", vs)
ardb.assert2(vs[2] == "c", vs)
ardb.assert2(vs[3] == "d", vs)
ardb.assert2(vs[4] == "e", vs)
ardb.assert2(vs[5] == "f", vs)
vs = ardb.call("zrevrangebylex", "myzset", "(g", "[aaa")
ardb.assert2(table.getn(vs) == 5, vs)
ardb.assert2(vs[1] == "f", vs)
ardb.assert2(vs[2] == "e", vs)
ardb.assert2(vs[3] == "d", vs)
ardb.assert2(vs[4] == "c", vs)
ardb.assert2(vs[5] == "b", vs)
vs = ardb.call("zrangebylex", "myzset", "[aaa", "(g", "limit", "2", "2")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "d", vs)
ardb.assert2(vs[2] == "e", vs)

ardb.call("del", "myzset")
ardb.call("zadd", "myzset", "0", "aaaa", "0", "b", "0", "c", "0", "d", "0", "e")
ardb.call("zadd", "myzset", "0", "foo", "0", "zap", "0", "zip", "0", "ALPHA", "0", "alpha")
vs = ardb.call("zrange", "myzset", "0", "-1")
ardb.assert2(table.getn(vs) == 10, vs)
s = ardb.call("ZREMRANGEBYLEX", "myzset", "[alpha", "[omega")
ardb.assert2(s ==6, s)
vs = ardb.call("zrange", "myzset", "0", "-1")
ardb.assert2(table.getn(vs) == 4, vs)
ardb.assert2(vs[1] == "ALPHA", vs)
ardb.assert2(vs[2] == "aaaa", vs)
ardb.assert2(vs[3] == "zap", vs)
ardb.assert2(vs[4] == "zip", vs)

ardb.call("del", "myzset")
ardb.call("zadd", "myzset", "1", "one", "2", "two", "3", "three")
s = ardb.call("ZREMRANGEBYRANK", "myzset", "0", "1")
ardb.assert2(s==2, s)
vs = ardb.call("zrange", "myzset", "0", "-1")
ardb.assert2(table.getn(vs) == 1, vs)
ardb.assert2(vs[1] == "three", vs)

ardb.call("del", "myzset")
ardb.call("zadd", "myzset", "1", "one", "2", "two", "3", "three")
s = ardb.call("ZREMRANGEBYSCORE", "myzset", "-inf", "(2")
ardb.assert2(s==1, s)
vs = ardb.call("zrange", "myzset", "0", "-1")
ardb.assert2(table.getn(vs) == 2, vs)
ardb.assert2(vs[1] == "two", vs)
ardb.assert2(vs[2] == "three", vs)

ardb.call("del", "zset1", "zset2", "zset3")
ardb.call("zadd", "zset1", "1", "one", "2", "two")
ardb.call("zadd", "zset2", "1", "one", "2", "two", "3", "three")
s = ardb.call("ZINTERSTORE", "zset3", "2", "zset1", "zset2", "WEIGHTS", "2", "3")
ardb.assert2(s==2, s)
vs = ardb.call("zrange", "zset3", "0", "-1", "withscores")
ardb.assert2(table.getn(vs) == 4, vs)
ardb.assert2(vs[1] == "one", vs)
ardb.assert2(vs[2] == "5", vs)
ardb.assert2(vs[3] == "two", vs)
ardb.assert2(vs[4] == "10", vs)
s = ardb.call("ZUNIONSTORE", "zset3", "2", "zset1", "zset2", "WEIGHTS", "2", "3", "AGGREGATE", "MAX")
ardb.assert2(s==3, s)
vs = ardb.call("zrange", "zset3", "0", "-1", "withscores")
ardb.assert2(table.getn(vs) == 6, vs)
ardb.assert2(vs[1] == "one", vs)
ardb.assert2(vs[2] == "3", vs)
ardb.assert2(vs[3] == "two", vs)
ardb.assert2(vs[4] == "6", vs)
ardb.assert2(vs[5] == "three", vs)
ardb.assert2(vs[6] == "9", vs)