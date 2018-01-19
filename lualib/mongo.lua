require "libmongo"
local bson = require "bson"
local socket = require "mongo.socket"
local driver = require "mongo.driver"
local rawget = rawget
local assert = assert

local bson_encode = bson.encode
local bson_encode_order = bson.encode_order
local bson_decode = bson.decode
local empty_bson = bson_encode {}

local mongo = {}
mongo.null = assert(bson.null)
mongo.maxkey = assert(bson.maxkey)
mongo.minkey = assert(bson.minkey)
mongo.type = assert(bson.type)
mongo.objectid = assert(bson.objectid)

local mongo_cursor = {}
local cursor_meta = {
	__index = mongo_cursor,
}

local mongo_client = {}

local client_meta = {
	__index = function(self, key)
		return rawget(mongo_client, key) or self:getDB(key)
	end,
	__tostring = function (self)
		local port_string
		if self.port then
			port_string = ":" .. tostring(self.port)
		else
			port_string = ""
		end

		return "[mongo client : " .. self.host .. port_string .."]"
	end,
	__gc = function(self)
		self:disconnect()
	end
}

local mongo_db = {}

local db_meta = {
	__index = function (self, key)
		return rawget(mongo_db, key) or self:getCollection(key)
	end,
	__tostring = function (self)
		return "[mongo db : " .. self.name .. "]"
	end
}

local mongo_collection = {}
local collection_meta = {
	__index = function(self, key)
		return rawget(mongo_collection, key) or self:getCollection(key)
	end ,
	__tostring = function (self)
		return "[mongo collection : " .. self.full_name .. "]"
	end
}

function mongo.client( obj )
	obj.port = obj.port or 27017
	obj.__id = 0
	obj.__sock = assert(socket.open(obj.host, obj.port),"Connect failed")
    obj.__try_count = 1
	return setmetatable(obj, client_meta)
end

function mongo_client:getDB(dbname)
	local db = {
		connection = self,
		name = dbname,
		full_name = dbname,
		database = false,
		__cmd = dbname .. "." .. "$cmd",
	}

	db.database = db

	return setmetatable(db, db_meta)
end

function mongo_client:disconnect()
	if self.__sock then
		socket.close(self.__sock)
		self.__sock = nil
	end
end

function mongo_client:genId()
	local id = self.__id + 1
	self.__id = id
	return id
end

function mongo_client:runCommand(...)
	if not self.admin then
		self.admin = self:getDB "admin"
	end
	return self.admin:runCommand(...)
end

---------------------------------------------------------------------
--linbc add on 20140924 让mongodb luadriver支持身份验证
function mongo_client:authenticate(db_, user, pass)
    --保存一下身份验证信息,用于断线重连
    self.__auth_info = {
        user = user,
        pass = pass,
        db = db_ or 'admin',
    }
    assert(type(self.__auth_info.db) == "string")

	--先执行一下getnonce = 1
	local db = self:getDB(db_ or 'admin')
	local from_db = db:runCommand('getnonce')
	local nonce = from_db["nonce"]
	local hex_digest = driver.md5(user, ':mongo:', pass)
	hex_digest = driver.md5(nonce, user, hex_digest)

	return db:runCommand('authenticate',1,
		'user', user,
		'nonce', nonce,
		'key', hex_digest).ok == 1
end

--断线重连
function mongo_client:reconnect()
    --关闭一下释放一下资源
    self:disconnect()
	self.__sock = assert(socket.open(self.host, self.port),"Connect failed")
    self.__try_count = self.__try_count + 1
    if self.__auth_info then
        return assert(self:authenticate(self.__auth_info.db, self.__auth_info.user, self.__auth_info.pass))
    end
end

----传入
--function mongo_client:update(ns, selector, values, upsert, multi)
--	local flags = (upsert and 1 or 0) + (multi and 2 or 0)
--	assert(flags ~= 3)
--	local sock = self.connection.__sock
--	local pack = driver.update(ns, flags, bson_encode(selector), bson_encode({['$set']=values}))
--	-- todo: check send
--	socket.write(sock, pack)
--end
--
--function mongo_client:insert(ns,doc)
--	if doc._id == nil then
--		doc._id = bson.objectid()
--	end
--	local sock = self.connection.__sock
--	local pack = driver.insert(0, ns, bson_encode(doc))
--	-- todo: check send
--	-- flags support 1: ContinueOnError
--	socket.write(sock, pack)
--end
--
--function mongo_client:delete(ns, selector, single)
--	local sock = self.connection.__sock
--	local pack = driver.delete(ns, single, bson_encode(selector))
--	-- todo: check send
--	socket.write(sock, pack)
--end
--linbc add end
---------------------------------------------------------------------

local function get_reply(sock, result)
    local length = socket.read(sock, 4)
    if length == nil then return nil end

	length = driver.length(length)
	local reply = socket.read(sock, length)
    if reply == nil then return nil end

	return reply, driver.reply(reply, result)
end

function mongo_db:runCommand(cmd,cmd_v,...)
	local request_id = self.connection:genId()
	local sock = self.connection.__sock
	local bson_cmd
	if not cmd_v then
		bson_cmd = bson_encode_order(cmd,1)
	else
		bson_cmd = bson_encode_order(cmd,cmd_v,...)
	end
	local pack = driver.query(request_id, 0, self.__cmd, 0, 1, bson_cmd)
	-- todo: check send
	if not socket.write(sock, pack) then
        self:reconnect()
        return self:runCommand(cmd,cmd_v,...)
    end

	local _, succ, reply_id, doc = get_reply(sock)
	assert(request_id == reply_id, "Reply from mongod error")
	-- todo: check succ
	return bson_decode(doc)
end

function mongo_db:getCollection(collection)
	local col = {
		connection = self.connection,
		name = collection,
		full_name = self.full_name .. "." .. collection,
		database = self.database,
	}
	self[collection] = setmetatable(col, collection_meta)
	return col
end


--根据条件计算记录数
function mongo_db:count( col, selector )
--print('mongo_collection:count')
	local r = self:runCommand("count", col, "query", selector or {})
	return r.n
end

mongo_collection.getCollection = mongo_db.getCollection

function mongo_collection:insert(doc)
	if doc._id == nil then
		doc._id = bson.objectid()
	end
	local sock = self.connection.__sock
	local pack = driver.insert(0, self.full_name, bson_encode(doc))
	-- todo: check send
	-- flags support 1: ContinueOnError
	socket.write(sock, pack)
end

function mongo_collection:batch_insert(docs)
	for i=1,#docs do
		if docs[i]._id == nil then
			docs[i]._id = bson.objectid()
		end
		docs[i] = bson_encode(docs[i])
	end
	local sock = self.connection.__sock
	local pack = driver.insert(0, self.full_name, docs)
	-- todo: check send
	socket.write(sock, pack)
end

function mongo_collection:update(selector,update,upsert,multi)
	local flags = (upsert and 1 or 0) + (multi and 2 or 0)
	local sock = self.connection.__sock
	local pack = driver.update(self.full_name, flags, bson_encode(selector), bson_encode(update))
	-- todo: check send
	socket.write(sock, pack)
end

function mongo_collection:delete(selector, single)
	local sock = self.connection.__sock
	local pack = driver.delete(self.full_name, single, bson_encode(selector))
	-- todo: check send
	socket.write(sock, pack)
end

--@query: 查询的条件
--@selector: 返回的字段
function mongo_collection:findOne(query, selector)
	local request_id = self.connection:genId()
	local sock = self.connection.__sock
	local pack = driver.query(request_id, 0, self.full_name, 0, 1, query and bson_encode(query) or empty_bson, selector and bson_encode(selector))

	-- todo: check send
	if not socket.write(sock, pack) then
        self:reconnect()
        return self:findOne(query, selector)
    end

	local reply, succ, reply_id, doc = get_reply(sock)
    if reply == nil then
        self:reconnect()
        return self:findOne(query, selector)
    end
	assert(request_id == reply_id, "Reply from mongod error")
	-- todo: check succ
	return bson_decode(doc)
end

function mongo_collection:find(query, selector, limit, skip)
	return setmetatable( {
		__collection = self,
		__query = query and bson_encode(query) or empty_bson,
		__selector = selector and bson_encode(selector),
		__ptr = nil,
		__data = nil,
		__cursor = nil,
		__document = {},
		__flags = 0,
		__limit = limit or 0,
		__skip = skip or 0,
		__seen = 0,			--已读取数量
	} , cursor_meta)
end

function mongo_collection:findAndModify(query,update,new,upsert)

	return self.database:runCommand(
		"findAndModify",self.name,
		"query",query,
		"update",update,
		"new",new or false,
		"upsert",upsert or false
	)

end

function mongo_cursor:hasNext()
	if self.__ptr == nil then
		if self.__document == nil then
			return false
		end
		--超过返回的限制
		if self.__limit ~= 0 and self.__seen >= self.__limit then
			return false
		end
		local conn = self.__collection.connection
		local request_id = conn:genId()
		local sock = conn.__sock
		local pack
		if self.__data == nil then
			pack = driver.query(request_id, self.__flags, self.__collection.full_name,self.__skip,self.__limit,self.__query,self.__selector)
		else
			if self.__cursor then
				local limit = self.__limit > 0 and self.__limit - self.__seen or 0
				pack = driver.more(request_id, self.__collection.full_name, limit ,self.__cursor)
			else
				-- no more
				self.__document = nil
				self.__data = nil
				return false
			end
		end

		--todo: check send
		socket.write(sock, pack)

		local data, succ, reply_id, doc, cursor, _, num = get_reply(sock, self.__document)
		assert(request_id == reply_id, "Reply from mongod error")
		if succ then
			if doc then
				self.__data = data
				self.__ptr = 1
				self.__cursor = cursor
				self.__seen = self.__seen + num	--累计读取记录数
				return true
			else
				self.__document = nil
				self.__data = nil
				self.__cursor = nil
				return false
			end
		else
			self.__document = nil
			self.__data = nil
			self.__cursor = nil
			if doc then
				local err = bson_decode(doc)
				error(err["$err"])
			else
				error("Reply from mongod error")
			end
		end
	end

	return true
end

function mongo_cursor:next()
	if self.__ptr == nil then
		error "Call hasNext first"
	end
	local r = bson_decode(self.__document[self.__ptr])
	self.__ptr = self.__ptr + 1
	if self.__ptr > #self.__document then
		self.__ptr = nil
	end

	return r
end

function mongo_cursor:close()
	-- todo: warning hasNext after close
	if self.__cursor then
		local sock = self.__collection.connection.__sock
		local pack = driver.kill(self.__cursor)
		-- todo: check send
		socket.write(sock, pack)
	end
end

return mongo
