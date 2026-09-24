--[[
  slurmqueuepacker - job_submit plugin

  This runs synchronously inside slurmctld on every submission, so it does
  exactly one thing: index a precomputed table. All the decision-making happens
  in sqpd, out of band. Everything below is wrapped in pcall; a plugin that
  throws rejects a user's job.

  Failure is always to the site's static rule:
    - the table file is missing, unreadable or unparsable
    - the table is older than its own max_age
    - the disable file exists
    - anything at all raises
  In every one of those cases the behaviour is exactly what this site did before
  installing the packer.
--]]

local TABLE_PATH   = "/run/sqp/policy.lua"
local DISABLE_PATH = "/etc/sqp/disable"

-- Static fallback. Keep these in step with sqp.toml [policy].
local SLIM = "zen5,zen3"
local FAT  = "zen5x,zen3x"
local RATIO_THRESHOLD = 6000            -- MB per CPU
local MIN_MEM_MB = 512
local INTERACTIVE = "interactive"
local GPU_PARTITION = "gpu-a10"

local cache = { body = nil, tbl = nil }

local function file_exists(p)
    local f = io.open(p, "r")
    if f then f:close(); return true end
    return false
end

-- Reload only when the file actually changed. sqpd rewrites it only when the
-- decision surface changes, so in the steady state this costs one stat().
local function load_table()
    if file_exists(DISABLE_PATH) then return nil end
    local f = io.open(TABLE_PATH, "r")
    if not f then return nil end
    local body = f:read("*a")
    f:close()
    if not body or #body == 0 then return nil end
    -- Cache on the body itself. sqpd rewrites the file only when the decisions
    -- change, so this compare succeeds almost every time and costs a few KB of
    -- string comparison; caching on length alone could collide.
    if cache.body == body and cache.tbl then return cache.tbl end
    local chunk = load(body, "sqp-policy", "t", {})
    if not chunk then return nil end
    local ok, tbl = pcall(chunk)
    if not ok or type(tbl) ~= "table" or type(tbl.t) ~= "table" then return nil end
    cache.body, cache.tbl = body, tbl
    return tbl
end

local function bucket(value, edges)
    local i = 0
    for k = 1, #edges do
        if value >= edges[k] then i = i + 1 else break end
    end
    return i
end

local function static_choice(mem_mb, cpus)
    if (mem_mb / cpus) < RATIO_THRESHOLD then return SLIM else return FAT end
end

-- The bucket table cannot answer feasibility: its top shape buckets are
-- open-ended, so a set chosen for a 0.86 TB job can be handed to a 2.2 TB job
-- that none of its partitions can hold. Filter by the job's real size against
-- each partition's largest node, which sqpd emits alongside the table.
local function keep_feasible(parts, tbl, mem_mb, cpus)
    if type(tbl.cap) ~= "table" then return parts end
    local out, biggest, biggest_mem = {}, nil, -1
    for p, c in pairs(tbl.cap) do
        if c[2] > biggest_mem then biggest, biggest_mem = p, c[2] end
    end
    for p in string.gmatch(parts, "[^,]+") do
        local c = tbl.cap[p]
        if c then
            if cpus <= c[1] and mem_mb <= c[2] then out[#out + 1] = p end
        else
            out[#out + 1] = p            -- unknown partition: do not second-guess
        end
    end
    if #out > 0 then return table.concat(out, ",") end
    -- Nothing in the looked-up set can hold this job, because the set was chosen
    -- for a bucket representative smaller than the job actually is. Pruning
    -- cannot recover from that, so recompute feasibility over ALL partitions
    -- from the caps -- which is the one thing the plugin has enough information
    -- to do on its own.
    for p, c in pairs(tbl.cap) do
        if cpus <= c[1] and mem_mb <= c[2] then out[#out + 1] = p end
    end
    if #out > 0 then
        table.sort(out)
        return table.concat(out, ",")
    end
    -- Genuinely fits nowhere. Name the roomiest partition anyway, so the user
    -- gets Slurm's ordinary "node configuration is not available" rather than a
    -- job with no partition at all.
    return biggest
end

local function packed_choice(mem_mb, cpus, minutes)
    local tbl = load_table()
    if not tbl then return nil, "no table" end
    if tbl.generated_at and tbl.max_age and
       (os.time() - tbl.generated_at) > tbl.max_age then
        return nil, "stale"
    end
    local i = bucket(mem_mb / cpus, tbl.mpc_edges)
    local j = bucket(cpus, tbl.cpu_edges)
    local k = bucket(minutes / 60, tbl.wt_edges)
    local parts = tbl.t[string.format("%d,%d,%d", i, j, k)]
    if type(parts) ~= "string" or parts == "" then return nil, "no entry" end
    local kept = keep_feasible(parts, tbl, mem_mb, cpus)
    if type(kept) ~= "string" or kept == "" then return nil, "infeasible" end
    local note = ""
    if kept ~= parts then note = " refit" end
    return kept, string.format("v%d b%d,%d,%d%s", tbl.version or 0, i, j, k, note)
end

function slurm_job_submit(job_desc, part_list, submit_uid)
    -- Admin escape hatch, unchanged from the site's original script.
    if job_desc.account == "root" and not job_desc.partition ~= "" then
        return slurm.SUCCESS
    end
    if job_desc.reservation and job_desc.reservation ~= "" then
        slurm.log_user("Reservation '%s': set --partition yourself to match it.",
                       job_desc.reservation)
        return slurm.SUCCESS
    end

    -- Defaults, so the arithmetic below cannot divide by zero or nil.
    if job_desc.min_mem_per_node and job_desc.min_mem_per_node == 0 then
        slurm.log_user("Memory per node of 0 is not allowed; use --exclusive instead.")
        return slurm.ERROR
    end
    if not job_desc.min_mem_per_node or job_desc.min_mem_per_node == slurm.NO_VAL64
       or job_desc.min_mem_per_node < MIN_MEM_MB then
        job_desc.min_mem_per_node = MIN_MEM_MB
    end
    if not job_desc.min_cpus or job_desc.min_cpus == 0
       or job_desc.min_cpus == slurm.NO_VAL then
        job_desc.min_cpus = 1
    end

    local is_batch = (job_desc.script and job_desc.script ~= "")

    if job_desc.tres_per_node and
       string.find(string.lower(job_desc.tres_per_node), "gpu") then
        job_desc.partition = GPU_PARTITION
        return slurm.SUCCESS
    end
    if not is_batch then
        job_desc.partition = INTERACTIVE
        job_desc.qos = INTERACTIVE
        return slurm.SUCCESS
    end
    if job_desc.comment and
       string.find(string.lower(job_desc.comment), "openondemand_interactive") then
        job_desc.partition = INTERACTIVE
        job_desc.qos = INTERACTIVE
        return slurm.SUCCESS
    end
    if job_desc.name and string.find(string.lower(job_desc.name), "sshdbridge") then
        job_desc.partition = INTERACTIVE
        job_desc.qos = INTERACTIVE
        return slurm.SUCCESS
    end

    local mem = job_desc.min_mem_per_node
    local cpus = job_desc.min_cpus
    local minutes = job_desc.time_limit
    if not minutes or minutes == slurm.NO_VAL or minutes == slurm.INFINITE then
        minutes = 60
    end

    -- The whole packer, from the plugin's point of view: one table lookup.
    local ok, parts, why = pcall(packed_choice, mem, cpus, minutes)
    if ok and parts then
        job_desc.partition = parts
        slurm.log_info("sqp: uid=%.0f name='%s' %dc %dMB -> %s (%s)",
                       submit_uid, job_desc.name or "?", cpus, mem, parts, why or "")
    else
        job_desc.partition = static_choice(mem, cpus)
        slurm.log_info("sqp: uid=%.0f name='%s' %dc %dMB -> %s (fallback: %s)",
                       submit_uid, job_desc.name or "?", cpus, mem,
                       job_desc.partition, tostring(parts or why))
    end
    return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
    return slurm.SUCCESS
end

return slurm.SUCCESS
