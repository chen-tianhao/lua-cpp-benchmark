package.path = package.path .. ";.\\?.lua"
local socket = require("socket")
local sqlite3 = require("lsqlite3")
local my_db_init = require("TestLuaJIT_Read_Insert")


local DB_ON_DISK = "SimulationP3.db"
local DB_IN_MEMORY = ":memory:"
local NUM_OF_ITER = 1000
local NUM_OF_RUNS = 10


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


local function write_result_to_csv(filename, mode, headers, data)
    local file = io.open(filename, mode)
    if file == nil then
        print("Failed to open file: " .. filename)
        return
    end
    -- Write head for each column
    file:write(headers .. "\n")
    
    -- Write data rows
    for _, row in ipairs(data) do
        -- Write each row and separate with quotes if needed 
        local line = string.format(", %.6f, %.6f, %.6f\n", row.col_B, row.col_C, row.col_D)
        file:write(line)
    end

    -- calcute average value
    local avg_B = average(data, "col_B")
    local avg_C = average(data, "col_C")
    local avg_D = average(data, "col_D")
    local line = string.format("Average, %.6f, %.6f, %.6f\n", avg_B, avg_C, avg_D)
    file:write(line)
    file:write("\n")

    file:close()
end


local function test_case_0(db, is_full_col)
    local sql_str = ""
    if is_full_col then
        sql_str = "SELECT * FROM JobList WHERE status = ?"
    else
        sql_str = "SELECT job_id FROM JobList WHERE status = ?"
    end
    local stmt = db:prepare(sql_str)
    stmt:bind_values("L")
	local select_start = socket.gettime()
    for i = 1, NUM_OF_ITER do
        local result = stmt:step()
        while result == sqlite3.ROW do
            -- local job_id = (is_full_col) and stmt:get_value(4) or stmt:get_value(0)
            result = stmt:step()
            -- stmt:reset()  -- no need to reset if binding value is the same 
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
    for i = 1, NUM_OF_RUNS do
        local result = {}
        local time_cost_one_col = test_case_0(db, false)
        local time_cost_all_col = test_case_0(db, true)
        result.col_B = time_cost_one_col
        result.col_C = 0
        result.col_D = time_cost_all_col
        table.insert(results, result)
    end
    write_result_to_csv("my_outcome.csv", "w", ",SELECT 1 col,SELECT 42 cols,SELECT * (76 cols)", results)
end


local function test_case_1(db)
    -- Read one row using index
    local sql_str = "SELECT * FROM JobList WHERE job_id_int = 5001"
	local select_start = socket.gettime()
    db:exec("BEGIN TRANSACTION")
    local stmt = db:prepare(sql_str)
    for i = 1, NUM_OF_ITER do
        local result = stmt:step()
        -- can comment while due to there is only on row according to the index
        while result == sqlite3.ROW do
            -- local job_id = stmt:get_value(4)
            result = stmt:step()
            -- stmt:reset()   -- no need to reset if binding value is the same 
        end
    end
    db:exec("COMMIT")
	local select_end = socket.gettime()
    stmt:finalize()
    local read_one_col = select_end - select_start
    print(string.format("test_case_1, Read one col using index, time cost: %.6f seconds", read_one_col))

    -- Write one row using index
    local sql_str = "Update JobList set ht_type = 'AGV121' WHERE job_id_int = 5001"
	local select_start = socket.gettime()
    db:exec("BEGIN TRANSACTION")
    local stmt = db:prepare(sql_str)
    for i = 1, NUM_OF_ITER do
        local result = stmt:step()
        -- can comment while due to there is only on row according to the index
        while result == sqlite3.ROW do
            -- local job_id = stmt:get_value(4)
            result = stmt:step()
            -- stmt:reset()  -- no need to reset if binding value is the same 
        end
    end
    db:exec("COMMIT")
	local select_end = socket.gettime()
    stmt:finalize()
    local write_one_col = select_end - select_start
    print(string.format("test_case_1, Write one col using index, time cost: %.6f seconds", write_one_col))

    return read_one_col, write_one_col
end


function G_test_case_1(db)
    local results = {}
    for i = 1, NUM_OF_RUNS do
        local result = {}
        local read_one_col, write_one_col = test_case_1(db)
        result.col_B = read_one_col
        result.col_C = write_one_col
        result.col_D = 0
        table.insert(results, result)
    end
    write_result_to_csv("my_outcome.csv", "a", ",Read one col using idx, Write one col using idx, Read all cols using idx", results)
end


local db = my_db_init.Init_db(":memory:", 364)
G_test_case_0(db)
my_db_init.Close_db(db)

local db = my_db_init.Init_db(":memory:", 10016)
G_test_case_1(db)
my_db_init.Close_db(db)

