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

    if not string.find(body, '"stream"') or not string.find(body, "true") then
        return
    end

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

    local host = response:match('"host"%s*:%s*"([^"]+)"')
    local port = response:match('"port"%s*:%s*(%d+)')

    if host and port then
        local server_name = "sc_" .. host:gsub("%.", "_") .. "_" .. port
        txn:set_var("txn.sidecar_server", server_name)
        txn:set_var("txn.is_streaming", true)
    end
end, 0)
