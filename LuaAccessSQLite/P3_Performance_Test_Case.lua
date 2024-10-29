package.path = package.path .. ";.\\?.lua"
local socket = require("socket")
print("=================require luasocket=================")
local sqlite3 = require("lsqlite3")
print("=================require lsqlite3=================")


local function test_case_0(db)
    -- local stmt = db:prepare("SELECT * FROM JobListQC1 WHERE status = ?")
	local select_start = socket.gettime()
    local stmt = db:prepare("SELECT * FROM JobList WHERE qc_id = ?")  -- no status value "L" here
    for i = 1, 1000 do
        -- stmt:bind_values("\"L\"")
        stmt:bind_values(1)
        local result = stmt:step()
        while result == sqlite3.ROW do
            local job_id = stmt:get_value(4)
            local cntr_type = stmt:get_value(10)
            local pri_precedence = stmt:get_value(20)
            -- print("job_id:", job_id, "cntr_type:", cntr_type, "pri_precedence:", pri_precedence)
            result = stmt:step()
        end
        --[[
        if result == sqlite3.DONE then
            print("All rows have been retrieved.")
        elseif result == sqlite3.BUSY then
            print("Database is busy, please try again.")
        elseif result == sqlite3.ERROR then
            print("An error occurred: " .. db:errmsg())
        elseif result == sqlite3.MISUSE then
            print("API misuse detected.")
        end
        ]]
    end
	local select_end = socket.gettime()
    stmt:finalize()
    print(string.format("test_case_0, time cost: %.6f seconds", select_end - select_start))
end


local my_db_init = require("TestLuaJIT_Read_Insert")
function G_test_case_0()
    local db = my_db_init.Init_db()
    test_case_0(db)
    my_db_init.Close_db(db)
end

G_test_case_0()
