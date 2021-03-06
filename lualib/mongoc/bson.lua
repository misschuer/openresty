
local libbson = require 'mongoc.libbson-wrap'

local ffi = require 'ffi'
local ffi_gc = ffi.gc
local ffi_new = ffi.new


local bson_new = libbson.bson_new
local bson_reinit = libbson.bson_reinit
local bson_destroy = libbson.bson_destroy

local bson_append_double = libbson.bson_append_double
local bson_append_int64 = libbson.bson_append_int64
local bson_append_null = libbson.bson_append_null
local bson_append_int32 = libbson.bson_append_int32
local bson_append_oid = libbson.bson_append_oid
local bson_append_utf8 = libbson.bson_append_utf8
local bson_append_document = libbson.bson_append_document
local bson_append_array = libbson.bson_append_array

local bson_iter_init = libbson.bson_iter_init
local bson_iter_next = libbson.bson_iter_next
local bson_iter_key = libbson.bson_iter_key
local bson_iter_type = libbson.bson_iter_type
local bson_iter_double = libbson.bson_iter_double
local bson_iter_utf8 = libbson.bson_iter_utf8
local bson_iter_int32 = libbson.bson_iter_int32
local bson_iter_int64 = libbson.bson_iter_int64
local bson_iter_oid = libbson.bson_iter_oid
local bson_iter_document = libbson.bson_iter_document
local bson_iter_array = libbson.bson_iter_array
local bson_iter_bool = libbson.bson_iter_bool

local bson_init_static = libbson.bson_init_static
local bson_oid_to_string = libbson.bson_oid_to_string

local function tableIsArray(value)
	return type(value) == "table" and #value > 0
end

local bson = {}

local bson_meta = {
	__index = bson,
	__tostring = function (self)        
    end,
    __gc = function ( self )
    end
}

function bson.new( ptr )
	local obj = {}
	obj.ptr = ptr
	if not ptr then
		obj.ptr = ffi_gc( bson_new(), bson_destroy)
	end
	return setmetatable(obj, bson_meta)
end

function bson:re_init( )
	return bson_reinit(self.ptr)
end

function bson:append_double( key, value )
	--assert(type(value) == 'number')
	return bson_append_double(self.ptr, key, string.len(key), value)
end

function bson:append_int64( key, value )
	--assert(type(value) == 'number')
	return bson_append_int64(self.ptr, key, string.len(key), value)
end

function bson:append_int32( key, value )
	--assert(type(value) == 'number')
	return bson_append_int32(self.ptr, key, string.len(key), value)
end

function bson:append_utf8( key, value )
	--assert(type(value) == 'string')
	return bson_append_utf8(self.ptr, key, string.len(key), value, string.len(value))
end

function bson:append_document( key, value )
	--assert(type(value) == 'table' and #value == 0)
	return bson_append_document(self.ptr, key, string.len(key), value)
end

function bson:append_array( key, value )
	--assert(type(value) == 'table' and #value > 0)
	return bson_append_array(self.ptr, key, string.len(key), value)
end

function bson:append_oid( key, value )
    --assert(type(value) == libbson.BSON_TYPE_OID)
    return bson_append_oid(self.ptr, key, string  .len(key), value)
end

--从一个lua_number中生成cdata类型
function bson:new_cdata_number( luaNumber )
	local a = ffi_new('int32_t',luaNumber)
    local b = ffi_new('double',luaNumber)
    return tonumber(a) == tonumber(b) and a or b
end

--插入值,根据值类型自动选用相应函数
function bson:append_value( key, value )
	assert(key and value)
	local typ = type(value)
	if typ == 'string' then
		return self:append_utf8(key, value)
	elseif typ == 'number' then
		local a = ffi_new('int64_t', value)
	    local b = ffi_new('double', value)
	    --如果相等则说明是整型 否则则是double
	    if tonumber(a) == tonumber(b) then
			return self:append_int64(key, a)
	    else
	    	return self:append_double(key, b)
	    end
	elseif typ == 'table' then
		local the_bson = bson.new()
		local isArray = the_bson:write_values(value)
		if isArray then
			return self:append_array(key, the_bson.ptr)
		else
			return self:append_document(key, the_bson.ptr)
		end
	elseif typ == 'cdata' then
		local typ2 = tostring(ffi.typeof(value))
        if typ2 == 'ctype<int>' then
			return self:append_int32(key, value)
		elseif typ2 =='ctype<unsigned int>' or typ2 == 'ctype<int64_t>' then
			return self:append_int64(key, value)
		elseif typ2 == 'ctype<double>'  then
			return self:append_double(key, value)
        elseif typ2 == 'ctype<const struct _bson_oid_t *>' then
            return self:append_oid(key, value)
		end
	end
	error(string.format('does not support: %s,%s,%s',typ, key, tonumber(value)))
end

--获取bson_iter的value值
function bson:get_value_by_iter( iter )
	local t = bson_iter_type(iter)
	if t == libbson.BSON_TYPE_DOUBLE then
		return bson_iter_double(iter)
	elseif t == libbson.BSON_TYPE_UTF8 then
		local buflen = ffi_gc( ffi.new("uint32_t[1]", 1), ffi.free)
		local utf8 = bson_iter_utf8(iter, buflen)
		return ffi.string(utf8)
	elseif t == libbson.BSON_TYPE_INT32 then
		return tonumber(bson_iter_int32(iter))
	elseif t == libbson.BSON_TYPE_INT64 then
		return tonumber(bson_iter_int64(iter))
	elseif t == libbson.BSON_TYPE_OID then
		local oid = bson_iter_oid(iter)
		return oid
        --local str = ffi_new("char[25]")
		--bson_oid_to_string(oid, str)
		--return ffi.string(str)
	elseif t == libbson.BSON_TYPE_DOCUMENT then
		local document_len = ffi_gc( ffi.new("uint32_t[1]", 1), ffi.free)
		local document = ffi_gc( ffi.new("const uint8_t*[1]"), ffi.free)
		bson_iter_document(iter, document_len, document)
		local the_bson = bson.new()
		bson_init_static(the_bson.ptr, document[0], document_len[0])
		return the_bson:read_values()
	elseif t == libbson.BSON_TYPE_ARRAY then
		local array_len = ffi_gc( ffi.new("uint32_t[1]", 1), ffi.free)
		local array = ffi_gc( ffi.new("const uint8_t*[1]"), ffi.free)
		bson_iter_array(iter, array_len, array)
		local the_bson = bson.new()
		bson_init_static(the_bson.ptr, array[0], array_len[0])
		return the_bson:read_values(true)
	elseif t == libbson.BSON_TYPE_BOOL then
		return bson_iter_bool (iter)
	else
		print("t",t)
		--TODO:未支持的类型在这里加一下
		error('does not support:',key,	t)
	end
end

--传入key直接返回值,如果为空则返回nil
function bson:read_value( key )
	local iter = ffi_gc( ffi.new('bson_iter_t'), ffi.free)
	bson_iter_init (iter, assert(self.ptr))
	while bson_iter_next(iter) do
		if ffi.string( bson_iter_key(iter) )== key then
			return self:get_value_by_iter(iter)
		end
	end
end

function bson:read_values( isArray )
	local iter = ffi_gc( ffi.new('bson_iter_t'), ffi.free)
	bson_iter_init (iter, assert(self.ptr))

	local values = {}
	while bson_iter_next(iter) do
		local k = ffi.string( bson_iter_key(iter) )
		if isArray then
			k = tonumber(k) + 1
		end
		values[k] = self:get_value_by_iter(iter)
	end
	return values
end

function bson:write_values( values )
	local isArray = tableIsArray(values)
	if isArray then
		for i = 1, #values do
			self:append_value( tostring(i - 1), values[i] )
		end
	else
		for k,v in pairs(values) do
			self:append_value( k,v )
		end
	end
	return isArray
end

function bson.runTests( )
	local values = {}
	values.a = 1
	values.b = '2'
	values.c = 3.1
	values.d = ffi_new('int32_t', 4)
	values.e = ffi_new('int64_t',5)
	values.f = ffi_new('double',6.2)

	local the_bson = bson.new()
	the_bson:write_values(values)

	local values2 = the_bson:read_values()
	for k,v in pairs(values) do
		print(k,v)
		assert(v == values2[k])
	end
	print('bson.runTests test ok!')
end

return bson
