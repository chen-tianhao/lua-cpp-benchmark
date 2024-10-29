
local sqlite3 = require("lsqlite3")
print("=================require lsqlite3=================")


local function test_case_0(db)
    -- local stmt = db:prepare("SELECT * FROM JobListQC1 WHERE status = ?")
    local stmt = db:prepare("SELECT * FROM JobList WHERE qc_id = ? and status = ?")
    for i = 1, 10 do
        -- stmt:bind_values("\"L\"")
        stmt:bind_values(1, "\"L\"")
        print("---->" .. i)
        local result = stmt:step()
        while result == sqlite3.ROW do
            print("====>" .. sqlite3.ROW)
            local job_id = stmt:get_value(4)
            local cntr_type = stmt:get_value(10)
            local pri_precedence = stmt:get_value(20)
            print("job_id:", job_id, "cntr_type:", cntr_type, "pri_precedence:", pri_precedence)
        end

        if result == sqlite3.DONE then
            print("All rows have been retrieved.")
        elseif result == sqlite3.BUSY then
            print("Database is busy, please try again.")
        elseif result == sqlite3.ERROR then
            print("An error occurred: " .. db:errmsg())
        elseif result == sqlite3.MISUSE then
            print("API misuse detected.")
        end
        
    end
    stmt:finalize()
end

local db = sqlite3.open("SimulationP3.db")
test_case_0(db)
db:close()

