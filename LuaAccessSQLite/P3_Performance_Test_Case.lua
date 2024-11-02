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
    -- Read one col using index
    local sql_str = "SELECT * FROM JobList WHERE job_id_int = 5001"
	local select_start = socket.gettime()
    db:exec("BEGIN TRANSACTION")
    local stmt1 = db:prepare(sql_str)
    for i = 1, NUM_OF_ITER do
        local result = stmt1:step()
        -- at most one row because of index is used
        if result == sqlite3.ROW then
            -- local job_id = stmt1:get_value(4)
        --    result = stmt1:step()
            -- stmt1:reset()   -- no need to reset if binding value is the same 
        end
    end
    db:exec("COMMIT")
	local select_end = socket.gettime()
    stmt1:finalize()
    local read_one_col = select_end - select_start
    print(string.format("test_case_1, Read one col using index, time cost: %.6f seconds", read_one_col))

    -- Write one col using index
    local sql_str = "Update JobList set ht_type = 'AGV121' WHERE job_id_int = 5001"
	local select_start = socket.gettime()
    db:exec("BEGIN TRANSACTION")
    local stmt2 = db:prepare(sql_str)
    for i = 1, NUM_OF_ITER do
        local result = stmt2:step()
        -- at most one row because of index is used
        if result == sqlite3.ROW then
            -- local job_id = stmt2:get_value(4)
        --    result = stmt2:step()
            -- stmt2:reset()  -- no need to reset if binding value is the same 
        end
    end
    db:exec("COMMIT")
	local select_end = socket.gettime()
    stmt2:finalize()
    local write_one_col = select_end - select_start
    print(string.format("test_case_1, Write one col using index, time cost: %.6f seconds", write_one_col))

    -- Read all cols using index and do assignment operation
    local sql_str = "SELECT * FROM JobList WHERE job_id_int = 5001"
	local select_start = socket.gettime()
    db:exec("BEGIN TRANSACTION")
    local stmt3 = db:prepare(sql_str)
    for i = 1, NUM_OF_ITER do
        local result = stmt3:step()
        -- at most one row because of index is used
        if result == sqlite3.ROW then
            local vv_c = stmt3:get_value(0)
            local qc_id = stmt3:get_value(1)
            local qc_seq_n = stmt3:get_value(2)
            local job_seq_n = stmt3:get_value(3)
            local job_id = stmt3:get_value(4)
            local job_id_int = stmt3:get_value(5)
            local cntr_n = stmt3:get_value(6)
            local ht_type = stmt3:get_value(7)
            local cntr_wt = stmt3:get_value(8)
            local cntr_size = stmt3:get_value(9)
            local cntr_type = stmt3:get_value(10)
            local cntr_op_status = stmt3:get_value(11)
            local bay_position = stmt3:get_value(12)
            local cone_decone_i = stmt3:get_value(13) -- (bool)
            local status = stmt3:get_value(14)
            local hau_gate_in_time = stmt3:get_value(15)
            local job_in_htme_i = stmt3:get_value(16) -- (bool) 
            local lift_type = stmt3:get_value(17)
            local job_pairing_id = stmt3:get_value(18)
            local dual_cycle_i = stmt3:get_value(19) -- (bool)
            local pri_precedence = stmt3:get_value(20) -- (json)
            local call_in_precedence = stmt3:get_value(21) -- (json)
            local wms_duration = stmt3:get_value(22)
            local mps_start_time_dt = stmt3:get_value(23)
            local mps_min_handling_duration = stmt3:get_value(24)
            local mps_actual_duration = stmt3:get_value(25)
            local pri_task_id = stmt3:get_value(26) -- (not int)
            local pri_task_state = stmt3:get_value(27)
            local pri_task_st_start_dt = stmt3:get_value(28)
            local pri_task_st_end_dt = stmt3:get_value(29)
            local pri_task_actual_start_dt = stmt3:get_value(30)
            local pri_task_actual_end_dt = stmt3:get_value(31)
            local sec_task_id = stmt3:get_value(32) -- (not int) 
            local sec_task_state = stmt3:get_value(33)
            local sec_task_st_start_dt = stmt3:get_value(34)
            local sec_task_st_end_dt = stmt3:get_value(35)
            local sec_task_actual_start_dt = stmt3:get_value(36)
            local sec_task_actual_end_dt = stmt3:get_value(37)
            local plat_st_start_dt = stmt3:get_value(38)
            local plat_st_end_dt = stmt3:get_value(39)
            local plat_actual_start_dt = stmt3:get_value(40)
            local plat_actual_end_dt = stmt3:get_value(41)
            local qc_op_type = stmt3:get_value(42)
            local yc_op_type = stmt3:get_value(43)
            local src_blk_id = stmt3:get_value(44)
            local src_slot_n = stmt3:get_value(45)
            local src_row_n = stmt3:get_value(46)
            local src_level_n = stmt3:get_value(47)
            local shuff_slot_n = stmt3:get_value(48)
            local shuff_row_n = stmt3:get_value(49)
            local shuff_level_n = stmt3:get_value(50)
            local overstow_cntr_q = stmt3:get_value(51)
            local yc_id = stmt3:get_value(52)
            local yc_st_start_dt = stmt3:get_value(53)
            local yc_st_end_dt = stmt3:get_value(54)
            local yc_actual_start_dt = stmt3:get_value(55)
            local yc_actual_end_dt = stmt3:get_value(56)
            local RTA_choice = stmt3:get_value(57) -- (json)
            local tallied_ht_m = stmt3:get_value(58)
            local tallied_trip_id_mount = stmt3:get_value(59)
            local tallied_trip_id_offload = stmt3:get_value(60)
            local assign_state_mount = stmt3:get_value(61)
            local assign_state_offload = stmt3:get_value(62)
            local ht_scheduled_start_dt = stmt3:get_value(63)
            local ht_scheduled_end_dt = stmt3:get_value(64)
            local ht_actual_start_dt = stmt3:get_value(65)
            local ht_actual_end_dt = stmt3:get_value(66)
            local ht_scheduled_mount_location_x = stmt3:get_value(67) -- (json) 
            local ht_scheduled_offload_location_x = stmt3:get_value(68) -- (json) 
            local virtual_pool_id = stmt3:get_value(69)
            local physical_pool_id = stmt3:get_value(70)
            local door_dir_source = stmt3:get_value(71)
            local door_dir_dest = stmt3:get_value(72)
            local is_pending_call_in = stmt3:get_value(73)
            local called_in_yc_id = stmt3:get_value(74)
            -- stmt3:reset()   -- no need to reset if binding value is the same 
        end
    end
    db:exec("COMMIT")
	local select_end = socket.gettime()
    stmt3:finalize()
    local read_all_col = select_end - select_start
    print(string.format("test_case_1, Read all col using index, time cost: %.6f seconds", read_all_col))

    return read_one_col, write_one_col, read_all_col
end


function G_test_case_1(db)
    local results = {}
    for i = 1, NUM_OF_RUNS do
        local result = {}
        local read_one_col, write_one_col, read_all_col = test_case_1(db)
        result.col_B = read_one_col
        result.col_C = write_one_col
        result.col_D = read_all_col
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

