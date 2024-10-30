package.path = package.path .. ";.\\?.lua"
local socket = require("socket")
local sqlite3 = require("lsqlite3")
local my_db_init = require("TestLuaJIT_Read_Insert")


local function average(data, column)
    local sum = 0
    local count = 0

    for _, row in ipairs(data) do
        if row[column] then  -- make sure the column exists
            sum = sum + row[column]
            count = count + 1
        end
    end

    if count == 0 then
        return nil, "The specific column does not exist in the data."
    else
        return sum / count
    end
end


local function write_result_to_csv(filename, data)
    local file = io.open(filename, "w")
    if file == nil then
        print("Failed to open file: " .. filename)
        return
    end
    -- Write head for each column
    file:write(",SELECT 1 col,SELECT 42 cols,SELECT * (76 cols)\n")
    
    -- Write data rows
    for _, row in ipairs(data) do
        -- Write each row and separate with quotes if needed 
        local line = string.format(", %.6f, %s, %.6f\n", row.one_col, "", row.all_col)
        file:write(line)
    end

    -- calcute average value
    local avg_one = average(data, "one_col")
    local avg_all = average(data, "all_col")
    local line = string.format("Average, %.6f, %s, %.6f\n", avg_one, "", avg_all)
    file:write(line)

    file:close()
end


local function test_case_0(db, is_full_col)
    -- local stmt = db:prepare("SELECT * FROM JobListQC1 WHERE status = ?")
    local sql_str = ""
    if is_full_col then
        sql_str = "SELECT * FROM JobList WHERE status = ?"
    else
        sql_str = "SELECT job_id FROM JobList WHERE status = ?"
    end
	local select_start = socket.gettime()
    local stmt = db:prepare(sql_str)
    for i = 1, 1000 do
        stmt:bind_values("L")
        local result = stmt:step()
        while result == sqlite3.ROW do
            local job_id = (is_full_col) and stmt:get_value(4) or stmt:get_value(0)
            -- local cntr_type = stmt:get_value(10)
            -- local pri_precedence = stmt:get_value(20)
            -- print("job_id:", job_id)
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
    local time_cost = select_end - select_start
    print(string.format("test_case_0(%s), time cost: %.6f seconds", (is_full_col) and "*" or "1", time_cost))
    return time_cost
end


function G_test_case_0(db)
    local results = {}
    for i = 1, 10 do
        local result = {}
        local time_cost_one_col = test_case_0(db, false)
        local time_cost_all_col = test_case_0(db, true)
        result.one_col = time_cost_one_col
        result.all_col = time_cost_all_col
        table.insert(results, result)
    end
    write_result_to_csv("my_outcome.csv", results)
end


local function test_case_1(db, is_full_col)
    -- local stmt = db:prepare("SELECT * FROM JobListQC1 WHERE status = ?")
    local sql_str = ""
    if is_full_col then
        sql_str = "SELECT * FROM JobList WHERE status is null"  -- no status value "L" here, use null instead
    else
        sql_str = "SELECT job_id FROM JobList WHERE status is null"  -- no status value "L" here, use null instead
    end
	local select_start = socket.gettime()
    local stmt = db:prepare(sql_str)
    for i = 1, 1000 do
        -- stmt:bind_values("\"L\"")
        local result = stmt:step()
        while result == sqlite3.ROW do
            local job_id = (is_full_col) and stmt:get_value(4) or stmt:get_value(0)
            -- local cntr_type = stmt:get_value(10)
            -- local pri_precedence = stmt:get_value(20)
            print("job_id:", job_id)
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
    local time_cost = select_end - select_start
    print(string.format("test_case_0(%s), time cost: %.6f seconds", (is_full_col) and "*" or "1", time_cost))
    return time_cost
end


local db = my_db_init.Init_db()
G_test_case_0(db)
my_db_init.Close_db(db)
