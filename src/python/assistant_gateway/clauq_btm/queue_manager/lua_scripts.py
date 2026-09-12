from __future__ import annotations

# Atomically update task fields only if the task exists and is in the expected status (e.g. "pending").
# KEYS[1]: task_key (e.g. "task:<task_id>")
# ARGV[1]: expected_status (e.g. "pending")
# ARGV[2..N]: field1, val1, field2, val2, ...
UPDATE_TASK_LUA = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
    return -1
end
if status ~= ARGV[1] then
    return -2
end
for i = 2, #ARGV, 2 do
    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1])
end
return 1
"""

