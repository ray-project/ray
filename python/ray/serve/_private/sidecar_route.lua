-- sidecar_route.lua
-- HAProxy Lua action: calls ingress /internal/route for sidecar routing decision.
-- Returns the sidecar server name (e.g. "sidecar_172.25.105.113_41319") that
-- HAProxy uses with use-server to pick the exact backend server.

local ROUTE_PATHS = {
    ["/v1/chat/completions"] = true,
    ["/v1/completions"] = true,
}

core.register_action("pick_sidecar", {"http-req"}, function(txn)
    local path = txn.sf:path()
    if not ROUTE_PATHS[path] then
        return
    end

    local body = txn.sf:req_body()
    if not body or body == "" then
        return
    end

    -- Only route streaming requests
    if not string.find(body, '"stream"') or not string.find(body, "true") then
        return
    end

    -- Get ingress port from HAProxy variable (set in config)
    local ingress_port = txn:get_var("txn.ingress_port")
    if not ingress_port then
        ingress_port = 30000
    end

    local sock = core.tcp()
    sock:settimeout(5)

    local ok, err = sock:connect("127.0.0.1", tonumber(ingress_port))
    if not ok then
        return
    end

    local req = "POST /internal/route HTTP/1.0\r\n"
        .. "Content-Type: application/json\r\n"
        .. "Content-Length: " .. #body .. "\r\n"
        .. "\r\n"
        .. body

    sock:send(req)
    local response = sock:receive("*a")
    sock:close()

    if not response then
        return
    end

    -- Parse host and port from JSON response
    local host = response:match('"host"%s*:%s*"([^"]+)"')
    local port = response:match('"port"%s*:%s*(%d+)')

    if host and port then
        -- Build server name matching the HAProxy config convention
        local server_name = "sc_" .. host:gsub("%.", "_") .. "_" .. port
        txn:set_var("txn.sidecar_server", server_name)
        txn:set_var("txn.is_streaming", true)
    end
end, 0)
