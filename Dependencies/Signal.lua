--!non-strict
local ScriptSignal = {}
ScriptSignal.__index = ScriptSignal

local ScriptConnection = {}
ScriptConnection.__index = ScriptConnection


local FreeThread = nil

local function RunHandlerInFreeThread(handler, ...)
	local thread = FreeThread
	FreeThread = nil

	handler(...)

	FreeThread = thread
end

local function CreateFreeThread()
	FreeThread = coroutine.running()

	while true do
		RunHandlerInFreeThread( coroutine.yield() )
	end
end

function ScriptSignal.new()
	return setmetatable({
		_active = true,
		_head = nil
	}, ScriptSignal)
end

function ScriptSignal.Is(object)
	return typeof(object) == 'table'
		and getmetatable(object) == ScriptSignal
end

function ScriptSignal:IsActive()
	return self._active == true
end

function ScriptSignal:Connect(
	handler
)

	assert(
		typeof(handler) == 'function',
		"Must be function"
	)

	if self._active ~= true then
		return setmetatable({
			Connected = false,
			_node = nil
		}, ScriptConnection)
	end

	local _head = self._head

	local node = {
		_signal = self,
		_connection = nil,
		_handler = handler,

		_next = _head,
		_prev = nil
	}

	if _head ~= nil then
		_head._prev = node
	end

	self._head = node

	local connection = setmetatable({
		Connected = true,
		_node = node
	}, ScriptConnection)

	node._connection = connection

	return connection
end

function ScriptSignal:ConnectOnce(
	handler
)
	assert(
		typeof(handler) == 'function',
		"Must be function"
	)

	local connection
	connection = self:Connect(function(...)
		connection:Disconnect()
		handler(...)
	end)
end

function ScriptSignal:Wait()
	local thread do
		thread = coroutine.running()

		local connection
		connection = self:Connect(function(...)
			connection:Disconnect()
			task.spawn(thread, ...)
		end)
	end

	return coroutine.yield()
end

function ScriptSignal:Fire(...: any)
	local node = self._head
	while node ~= nil do
		if node._connection ~= nil then
			if FreeThread == nil then
				task.spawn(CreateFreeThread)
			end

			task.spawn(
				FreeThread :: thread,
				node._handler, ...
			)
		end

		node = node._next
	end
end

function ScriptSignal:DisconnectAll()
	local node = self._head
	while node ~= nil do
		local _connection = node._connection

		if _connection ~= nil then
			_connection.Connected = false
			_connection._node = nil
			node._connection = nil
		end

		node = node._next
	end

	self._head = nil
end

function ScriptSignal:Destroy()
	if self._active ~= true then
		return
	end

	self:DisconnectAll()
	self._active = false
end

function ScriptConnection:Disconnect()
	if self.Connected ~= true then
		return
	end

	self.Connected = false

	local _node = self._node
	local _prev = _node._prev
	local _next = _node._next

	if _next ~= nil then
		_next._prev = _prev
	end

	if _prev ~= nil then
		_prev._next = _next
	else
		-- _node == _signal._head

		_node._signal._head = _next
	end

	_node._connection = nil
	self._node = nil
end

-- Compatibility methods for TopbarPlus
ScriptConnection.destroy = ScriptConnection.Disconnect
ScriptConnection.Destroy = ScriptConnection.Disconnect
ScriptConnection.disconnect = ScriptConnection.Disconnect
ScriptSignal.destroy = ScriptSignal.Destroy
ScriptSignal.Disconnect = ScriptSignal.Destroy
ScriptSignal.disconnect = ScriptSignal.Destroy

ScriptSignal.connect = ScriptSignal.Connect
ScriptSignal.wait = ScriptSignal.Wait
ScriptSignal.fire = ScriptSignal.Fire

return ScriptSignal
