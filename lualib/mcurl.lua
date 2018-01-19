require( "luacurl" )
assert(curl)

local mcurl = {}

function mcurl.new(  )
	local self = setmetatable({} , mcurl)
	mcurl.__index = mcurl
	self.multis = {}
	self.timeout = 3
	self.childs = {}
	return self
end

function mcurl:__get_can_use_multi( ... )
	local multi
	local num = #self.multis
	if num > 0 then
		local old_multi = self.multis[num]
		if old_multi.available > 1 then
			multi = old_multi
		end
	end
	if not multi then
		multi = {}
		multi.ptr = curlm.new() 
		multi.available = 1000			-- 可用次数最多支持1000个child
		self.multis[num + 1] = multi
	end
	return multi
end

function mcurl:add( url, postData, callback, userData)
	if not url then
		return false
	end
	local handle = curl.new()
	local child = {}
	child.key = #self.childs + 1
	child.handle = handle
	child.code = nil
	child.datas = {}
	child.userData = userData
	child.callback = callback
    self.childs[child.key] = child
	handle:setopt( curl.OPT_URL, url)
	handle:setopt( curl.OPT_POSTFIELDS, postData)
	handle:setopt( curl.OPT_NOSIGNAL, true)
	handle:setopt( curl.OPT_CONNECTTIMEOUT, self.timeout)
    handle:setopt( curl.OPT_WRITEFUNCTION,  function ( t, buffer)
		local child = self.childs[t[1]]
		table.insert(child.datas, buffer)
    	return string.len( buffer )
	end)
    handle:setopt( curl.OPT_WRITEDATA, {child.key});
    local multi = self:__get_can_use_multi()
    multi.available = multi.available - 1
	child.parent = multi
	child.handlecurl = multi.ptr:add(handle)
    return true
end

function mcurl:perform( ... )
	for i = 1, #self.multis do
		self.multis[i].ptr:perform_wait_select()
	end
	for k,v in pairs(self.childs) do
		v.parent.ptr:remove(v.handle)
		if v.callback ~= nil then
			v.callback(v.userData, v.handle:getinfo(curl.INFO_RESPONSE_CODE), table.concat(v.datas))
			v.handle:close()		
		end
	end
end


local function test()
	local URL0 = 'http://192.168.30.11/12j_develop/1.txt'
	local URL = 'http://192.168.30.10/'
	cm = mcurl.new() 
	for i = 1, 2000 do
		cm:add(URL0, i + 1 , function (userData, responseCode, value)
			print(userData, responseCode, value)
		end, i)
	end
	cm:perform()
	print("test end")
end

return mcurl
