-- Observe native LLM streams at HAProxy, using one proxy clock for both ends.
-- The filter only copies bounded payloads and captures timestamps. Encoding and
-- nonblocking syslog delivery run in an independent task; Python does SSE
-- decoding and histogram recording off the data-plane thread.
local HEADER = "x-ray-llm-metric-tags"
local MAX_PENDING = 256
local MAX_PAYLOAD = 262144
local MAX_TAGS = 2048
local FRAGMENT_BYTES = 2048
local observations = core.queue()
local dropped = 0

local function now_us()
    local now = core.now()
    return now.sec * 1000000 + now.usec
end

local function hex(body)
    return (body:gsub(".", function(c) return string.format("%02x", c:byte()) end))
end

core.register_task(function()
    while true do
        local item = observations:pop_wait()
        local parts = item.drop and 0 or math.max(1, math.ceil(#item.body / FRAGMENT_BYTES))
        for part = 0, parts - 1 do
            local body = item.body:sub(part * FRAGMENT_BYTES + 1,
                                       (part + 1) * FRAGMENT_BYTES)
            local message = string.format(
                "ray_llm_stream|%s|%d|%d|%d|%.0f|%.0f|%s|%d|%s",
                item.id, item.frame, part, parts, item.started, item.now,
                item.tags, item.ending and -1 or #body,
                item.ending and tostring(item.router_us or -1) or hex(body))
            -- Debug reaches only the metrics socket, not the access-log target.
            -- tune.lua.log.stderr off keeps generated text out of stderr.
            core.log(core.debug, message)
        end
        if dropped > 0 then
            core.log(core.debug, "ray_llm_stream|drop|" .. dropped)
            dropped = 0
        end
    end
end)

local Observer = {id = "LLM streaming metrics", flags = filter.FLT_CFG_FL_HTX}
Observer.__index = Observer

function Observer:new()
    return setmetatable({frame = 0}, Observer)
end

function Observer:start_analyze(txn, channel)
    if not channel:is_resp() then
        -- Runs before http-request actions, including body wait and routing.
        self.started = now_us()
    end
end

function Observer:http_headers(txn, message)
    if message.channel:is_resp() then
        local headers = message:get_headers()
        local tags = headers[HEADER] and headers[HEADER][0]
        local content_type = headers["content-type"] and headers["content-type"][0]
        message:del_header(HEADER)
        if txn:get_var("txn.via_ingress_request_router") and tags and #tags <= MAX_TAGS
            and message:get_stline().code == "200"
            and content_type and content_type:find("^text/event%-stream") then
            self.tags = tags
            -- Do not reuse client request IDs: concurrent requests may share one.
            self.id = txn.f:uuid()
            self.enabled = true
            filter.register_data_filter(self, message.channel)
        end
    end
    return filter.CONTINUE
end

function Observer:enqueue(body, ending, router_us)
    if observations:size() >= MAX_PENDING or #body > MAX_PAYLOAD then
        self.enabled = false
        dropped = dropped + 1
        if observations:size() < MAX_PENDING then observations:push({drop = true}) end
        return
    end
    observations:push({id = self.id, tags = self.tags, frame = self.frame,
                       started = self.started, now = now_us(), body = body,
                       ending = ending, router_us = router_us})
    self.frame = self.frame + 1
end

function Observer:http_payload(txn, message)
    if self.enabled then
        if message:input() > MAX_PAYLOAD then
            self.enabled = false
            dropped = dropped + 1
            if observations:size() < MAX_PENDING then observations:push({drop = true}) end
            return
        end
        local body = message:body()
        if body and #body > 0 then self:enqueue(body, false) end
    end
end

function Observer:http_end(txn, message)
    if message.channel:is_resp() and self.enabled then
        -- Reuse the routing action's timer. Only the completion packet carries
        -- it; per-token payload packets stay unchanged.
        self:enqueue("", true, txn:get_var("txn.ingress_request_router_latency_us"))
    end
end

core.register_filter("llm_stream_metrics", Observer, function(observer, args)
    return observer
end)
