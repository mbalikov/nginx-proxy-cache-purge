-- purge_sliced_cache.lua
--
-- cache_key argument here means:
--   the "base" proxy_cache_key value BEFORE appending $slice_range.
--
-- Example:
--   proxy_cache_key $uri$is_args$args$slice_range;
--   => cache_key should be exactly: $uri$is_args$args (resolved value)
--
-- If your real key has a delimiter before $slice_range, include it
-- in cache_key, for example:
--   proxy_cache_key "$host$request_uri|$slice_range";
--   => cache_key must end with "|"

local _M = {}

local DEFAULT_SLICE_SIZE = 10 * 1024 * 1024   -- 10 MiB
local DEFAULT_PROBE_CHUNK = 4096
local DEFAULT_MAX_PROBE   = 128 * 1024        -- 128 KiB

local function rstrip_slash(s)
    return (s:gsub("/+$", ""))
end

local function parse_levels(levels)
    if type(levels) == "table" then
        return levels
    end

    if type(levels) ~= "string" or levels == "" then
        error("cache_levels must be a string like '1:2'")
    end

    local out = {}
    for part in levels:gmatch("%d+") do
        local n = tonumber(part)
        if n ~= 1 and n ~= 2 then
            error("each cache level must be 1 or 2")
        end
        out[#out + 1] = n
    end

    if #out < 1 or #out > 3 then
        error("cache_levels must contain 1 to 3 levels")
    end

    return out
end

local function file_exists(path)
    local f = io.open(path, "rb")
    if f then
        f:close()
        return true
    end
    return false
end

local function build_slice_key(cache_key, slice_start, slice_size)
    local slice_end = slice_start + slice_size - 1
    return string.format("%sbytes=%d-%d", cache_key, slice_start, slice_end)
end

local function key_to_cache_path(cache_path, cache_key, cache_levels)
    local levels = parse_levels(cache_levels)
    local md5hex = ngx.md5(cache_key)

    -- NGINX levels=1:2 means:
    --   /cache_root/<last 1 hex>/<previous 2 hex>/<full md5>
    local dirs = {}
    local pos = #md5hex

    for i = 1, #levels do
        local n = levels[i]
        dirs[#dirs + 1] = md5hex:sub(pos - n + 1, pos)
        pos = pos - n
    end

    local root = rstrip_slash(cache_path)
    local path = root

    if #dirs > 0 then
        path = path .. "/" .. table.concat(dirs, "/")
    end

    path = path .. "/" .. md5hex
    return md5hex, path
end

local function read_content_range(file_path, expected_cache_key, probe_chunk_bytes, max_probe_bytes)
    local f, err = io.open(file_path, "rb")
    if not f then
        return nil, ("cannot open cache file '%s': %s"):format(file_path, err or "unknown error")
    end

    local probe_chunk = probe_chunk_bytes or DEFAULT_PROBE_CHUNK
    local max_probe   = max_probe_bytes or DEFAULT_MAX_PROBE

    local buf = ""
    local total_read = 0
    local key_checked = false

    while total_read < max_probe do
        local want = math.min(probe_chunk, max_probe - total_read)
        local chunk = f:read(want)
        if not chunk then
            break
        end

        buf = buf .. chunk
        total_read = total_read + #chunk

        -- Verify KEY: <cache_key>
        if not key_checked then
            local actual_key = buf:match("KEY:%s*(.-)\r?\n")
            if actual_key then
                key_checked = true

                if actual_key ~= expected_cache_key then
                    f:close()
                    return nil, ("cache file key mismatch for '%s': expected '%s', got '%s'")
                        :format(file_path, expected_cache_key, actual_key)
                end
            end
        end

        local range_start, range_end, total =
            buf:match("Content%-Range:%s*bytes%s+(%d+)%-(%d+)/([%d%*]+)")

        if total then
            f:close()

	    if not key_checked then
		return nil, "KEY line not found but Content-Range is present"
	    end

            if total == "*" then
                return nil, "Content-Range total is '*', cannot determine object size"
            end

            return {
                range_start = tonumber(range_start),
                range_end   = tonumber(range_end),
                object_size = tonumber(total),
                bytes_scanned = total_read,
            }
        end
    end

    f:close()

    if not key_checked then
        return nil, ("KEY line not found in first %d bytes of '%s'"):format(max_probe, file_path)
    end

    return nil, ("Content-Range not found in first %d bytes of '%s'"):format(max_probe, file_path)
end

function _M.purge_sliced_cache(cache_path, cache_key, cache_levels, slice_size, opts)
    opts = opts or {}

    slice_size = tonumber(slice_size) or DEFAULT_SLICE_SIZE

    local probe_chunk = opts.probe_chunk_bytes or DEFAULT_PROBE_CHUNK
    local max_probe   = opts.max_probe_bytes or DEFAULT_MAX_PROBE

    if type(cache_path) ~= "string" or cache_path == "" then
        return nil, "cache_path is required"
    end

    if type(cache_key) ~= "string" or cache_key == "" then
        return nil, "cache_key is required"
    end

    if slice_size <= 0 then
        return nil, "slice_size must be a positive integer"
    end

    local first_slice_key = build_slice_key(cache_key, 0, slice_size)
    local first_md5, first_path = key_to_cache_path(cache_path, first_slice_key, cache_levels)

    if not file_exists(first_path) then
        return nil, "first slice cache file not found", {
            first_slice_key = first_slice_key,
            first_slice_md5 = first_md5,
            first_slice_path = first_path,
        }
    end

    local cr, read_err = read_content_range(first_path, first_slice_key, probe_chunk, max_probe)
    if not cr then
        return nil, read_err, {
            first_slice_key = first_slice_key,
            first_slice_md5 = first_md5,
            first_slice_path = first_path,
        }
    end

    local object_size = cr.object_size
    local slice_count = math.floor((object_size + slice_size - 1) / slice_size)

    local deleted = {}
    local missing = {}
    local failed  = {}

    for i = 0, slice_count - 1 do
        local start = i * slice_size
        local slice_key = build_slice_key(cache_key, start, slice_size)
        local md5hex, path = key_to_cache_path(cache_path, slice_key, cache_levels)

        if file_exists(path) then
            local ok, rm_err, rm_code = os.remove(path)
            if ok then
                deleted[#deleted + 1] = {
                    key = slice_key,
                    md5 = md5hex,
                    path = path,
                }
            else
                failed[#failed + 1] = {
                    key = slice_key,
                    md5 = md5hex,
                    path = path,
                    err = rm_err,
                    code = rm_code,
                }
            end
        else
            missing[#missing + 1] = {
                key = slice_key,
                md5 = md5hex,
                path = path,
            }
        end
    end

    return {
        object_size = object_size,
        slice_size = slice_size,
        slice_count = slice_count,
        first_slice = {
            key = first_slice_key,
            md5 = first_md5,
            path = first_path,
            content_range = {
                start = cr.range_start,
                ["end"] = cr.range_end,
                total = cr.object_size,
            },
            bytes_scanned = cr.bytes_scanned,
        },
        deleted = deleted,
        missing = missing,
        failed = failed,
    }
end

return _M
