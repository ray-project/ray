-- Native threads parse/aggregate SSE. This Lua task only exports snapshots.
@STREAM_METRICS_EXPORT_CONFIG@
local load,err=package.loadlib(STREAM_METRICS_MODULE,"luaopen_ray_stream_metrics")
assert(load,"Cannot load the packaged ray-haproxy stream observer: "..tostring(err))
local native_metrics=load()
native_metrics.configure(HISTOGRAMS)

-- Minimal protobuf wire encoder. Schemas/bucket options are rendered once by
-- Python at configuration time; exported distributions contain exact counts and
-- sums, never synthetic observations reconstructed from bucket boundaries.
local function varint(value)
    local bytes={}
    repeat
        local byte=value & 127
        value=value >> 7
        bytes[#bytes+1]=string.char(byte+(value ~= 0 and 128 or 0))
    until value == 0
    return table.concat(bytes)
end
local function field(number,bytes)
    return varint((number << 3)|2)..varint(#bytes)..bytes
end
local function integer(number,value)
    return varint(number << 3)..varint(value)
end
local function double(number,value)
    return varint((number << 3)|1)..string.pack("<d",value)
end

local component
local errors = 0
local function label_values(tags)
    return {component,DEFAULT_LABELS[1],tags.ReplicaId,DEFAULT_LABELS[2],
            DEFAULT_LABELS[3],DEFAULT_LABELS[4],tags.engine_worker_id,tags.engine,tags.model_name}
end
local function timeseries(labels,point)
    local chunks={}
    for _,label in ipairs(labels) do chunks[#chunks+1]=field(2,field(1,label)) end
    chunks[#chunks+1]=field(3,point)
    return table.concat(chunks)
end
local function report_snapshot()
    local groups,native_errors,group_count=native_metrics.snapshot()
    if not groups then return false end
    -- Native errors are cumulative; export failures are counted separately.
    local total_errors=errors+native_errors
    if group_count == 0 and total_errors == 0 then return true end
    local metrics={}
    for _,group in pairs(groups) do
        local labels=label_values(group.tags)
        for i,schema in ipairs(HISTOGRAMS) do
            local histogram=group.histograms[i]
            if histogram.count > 0 then
                local distribution={integer(1,histogram.count),
                                    double(2,histogram.sum_us/1000000),field(4,schema.options)}
                for _,count in ipairs(histogram.buckets) do
                    distribution[#distribution+1]=field(5,integer(1,count))
                end
                local point=field(4,table.concat(distribution))
                local metric=field(1,schema.descriptor)..field(2,timeseries(labels,point))
                metrics[#metrics+1]=field(1,metric)
            end
        end
    end
    if total_errors > 0 then
        local labels={component,DEFAULT_LABELS[1],DEFAULT_LABELS[2],
                      DEFAULT_LABELS[3],DEFAULT_LABELS[4]}
        local metric=field(1,ERROR_DESCRIPTOR)..field(2,timeseries(labels,integer(2,total_errors)))
        metrics[#metrics+1]=field(1,metric)
    end
    metrics[#metrics+1]=field(2,REPORT_WORKER_ID)
    local protobuf=table.concat(metrics)
    local reply=core.httpclient():post{
        url="http://localhost/ray.rpc.ReporterService/ReportOCMetrics",
        dst="unix@"..EXPORT_SOCKET,
        headers={["content-type"]={"application/grpc"},["te"]={"trailers"}},
        -- HTTPClient timeouts are milliseconds, unlike core.sleep's seconds.
        body=string.pack(">BI4",0,#protobuf)..protobuf,timeout=1000,
    }
    if reply.status ~= 200 or reply.body ~= string.pack(">BI4",0,0) then
        errors=errors+1
        return false
    end
    return true
end


-- Only the private Unix frontend exposes bounded queue diagnostics.
core.register_service("stream_metrics_status","http",function(applet)
    local status=native_metrics.status()
    status.export_errors=errors
    status.component=component
    status.thread=core.thread
    status.generation=EXPORT_GENERATION
    applet:set_status(200)
    applet:add_header("content-type","application/json")
    applet:start_response()
    applet:send(native_metrics.encode(status))
end)

core.register_task(function()
    component="haproxy:stream:"..EXPORT_GENERATION..":"..core.thread
    core.set_nice(100)
    while native_metrics.status().stopping == 0 do
        core.sleep(1)
        if not pcall(report_snapshot) then errors=errors+1 end
    end
    -- Native export jobs and the private no-stop listener keep this generation
    -- alive after public listeners stop. In-flight streams can finish normally.
    local deadline
    local exported=false
    while true do
        local status=native_metrics.status()
        if status.active == 0 then
            deadline=deadline or (status.now_us+3000000)
            if status.now_us >= deadline then break end
            if status.pending == 0 then
                local ok,result=pcall(report_snapshot)
                exported=ok and result
                if exported then break end
            end
            core.msleep(50)
        else
            core.sleep(1)
            if not pcall(report_snapshot) then errors=errors+1 end
        end
    end
    if not exported then core.Warning("Stream metrics final flush failed or timed out") end
    native_metrics.release_export()
end)
