mongo = require "mongo"

print('hello', require('mongo.driver').md5('hello'))


db = mongo.client { host = "localhost" }
assert(db:authenticate('admin', 'dev', 'asdf'))

local r = db:runCommand "listDatabases"

for k,v in ipairs(r.databases) do
	print(v.name)
end


local loc = db:getDB "hello"
local c = loc.system.namespaces:find()

while c:hasNext() do
	local r = c:next()
	print(r.name)
end

print "==============="

--db.hello.world:update({},{['$set']={a=8888}},false,true)
--assert(db:runCommand ("getLastError",1,"w",1).ok == 1)
db.hello.world:delete {a=8888}
--os.exit(0)

db.hello.world:insert {a = 1,b = 2,create_tm = os.time()}
local r = db:runCommand ("getLastError",1,"w",1)
print(r.ok)

local c = db.hello.world:find()

while c:hasNext() do
	local r = c:next()
	print(mongo.type(r._id))
end

----------------------------------
--findOne
local d = db.hello.world:findOne {a=888}
assert(d == nil)

db:disconnect()
