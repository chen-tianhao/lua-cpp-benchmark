local cjson = require("cjson")
print("=================require cjson=================")
local sqlite3 = require("lsqlite3")
print("=================require lsqlite3=================")


local function open_db_create_table(db_conn_str)
    local db = sqlite3.open(db_conn_str)
    local create_table_sql = [=[
        CREATE TABLE IF NOT EXISTS JobList (
            vv_c  TEXT,
            qc_id  INTEGER,
            qc_seq_n  INTEGER,
            job_seq_n  INTEGER,
            job_id  TEXT,
            job_id_int  INTEGER,
            cntr_n  TEXT,
            ht_type  TEXT,
            cntr_wt  INTEGER,
            cntr_size  INTEGER,
            cntr_type  TEXT,
            cntr_op_status  TEXT,
            bay_position  INTEGER,
            cone_decone_i  INTEGER, -- (bool)
            status  TEXT,
            hau_gate_in_time  INTEGER,
            job_in_htme_i  INTEGER, -- (bool)
            lift_type  TEXT,
            job_pairing_id  TEXT,
            dual_cycle_i  INTEGER, -- (bool)
            pri_precedence  TEXT, -- (json)
            call_in_precedence  TEXT, -- (json)
            wms_duration  INTEGER,
            mps_start_time_dt  TEXT,
            mps_min_handling_duration  INTEGER,
            mps_actual_duration  INTEGER,
            pri_task_id  TEXT, -- (not int)
            pri_task_state  TEXT,
            pri_task_st_start_dt  TEXT,
            pri_task_st_end_dt  TEXT,
            pri_task_actual_start_dt  TEXT,
            pri_task_actual_end_dt  TEXT,
            sec_task_id  TEXT, -- (not int)
            sec_task_state  TEXT,
            sec_task_st_start_dt  TEXT,
            sec_task_st_end_dt  TEXT,
            sec_task_actual_start_dt  TEXT,
            sec_task_actual_end_dt  TEXT,
            plat_st_start_dt  TEXT,
            plat_st_end_dt  TEXT,
            plat_actual_start_dt  TEXT,
            plat_actual_end_dt  TEXT,
            qc_op_type  TEXT,
            yc_op_type  TEXT,
            src_blk_id  INTEGER,
            src_slot_n  INTEGER,
            src_row_n  INTEGER,
            src_level_n  INTEGER,
            shuff_slot_n  INTEGER,
            shuff_row_n  INTEGER,
            shuff_level_n  INTEGER,
            overstow_cntr_q  INTEGER,
            yc_id  TEXT,
            yc_st_start_dt  TEXT,
            yc_st_end_dt  TEXT,
            yc_actual_start_dt  TEXT,
            yc_actual_end_dt  TEXT,
            RTA_choice  TEXT, -- (json)
            tallied_ht_m  TEXT,
            tallied_trip_id_mount  TEXT,
            tallied_trip_id_offload  TEXT,
            assign_state_mount  TEXT,
            assign_state_offload  TEXT,
            ht_scheduled_start_dt  TEXT,
            ht_scheduled_end_dt  TEXT,
            ht_actual_start_dt  TEXT,
            ht_actual_end_dt  TEXT,
            ht_scheduled_mount_location_x  TEXT, -- (json)
            ht_scheduled_offload_location_x  TEXT, -- (json)
            virtual_pool_id  TEXT,
            physical_pool_id  TEXT,
            door_dir_source  TEXT,
            door_dir_dest  TEXT,
            is_pending_call_in  INTEGER,
            called_in_yc_id  TEXT,
            PRIMARY KEY (job_id_int)
        )
    ]=]
    local ret = db:exec(create_table_sql)
    if not ret then
        print("Error DB exec: ", db:errmsg())
        return nil
    end
    return db
end


local function splitCsvRow_v0(line)
    local fields = {}
    for field in line:gmatch("([^,]+)") do
        table.insert(fields, field)
    end
    return fields
end


local function splitCsvRow(line)
    local fields = {}
    local inQuotes = false
    local field = ""

    for char in line:gmatch(".") do
        if char == "\"" then
            inQuotes = not inQuotes  -- change the state of inQuotes
        elseif char == "," and not inQuotes then  -- if comma is not in quotes, it's a field separator
            table.insert(fields, field)
            field = ""
        else
            field = field .. char
        end
    end

    -- add the last field
    table.insert(fields, field)

    return fields
end


local function csv_to_db(filepath, db)
    local rows = {}  -- store a all rows of CSV data
    local file = io.open(filepath, "r")
    if not file then
        print("Cannot open file: " .. filepath)
        return nil
    end

    -- prepare SQL statement for repeat insertion
    local sql_str = [=[
        INSERT INTO JobList(vv_c, qc_id, qc_seq_n, job_seq_n, job_id, job_id_int, cntr_n, ht_type, cntr_wt, 
            cntr_size, cntr_type, cntr_op_status, bay_position, cone_decone_i, status, hau_gate_in_time, 
            job_in_htme_i, lift_type, job_pairing_id, dual_cycle_i, pri_precedence, call_in_precedence, 
            wms_duration, mps_start_time_dt, mps_min_handling_duration, mps_actual_duration, pri_task_id, 
            pri_task_state, pri_task_st_start_dt, pri_task_st_end_dt, pri_task_actual_start_dt, 
            pri_task_actual_end_dt, sec_task_id, sec_task_state, sec_task_st_start_dt, sec_task_st_end_dt, 
            sec_task_actual_start_dt, sec_task_actual_end_dt, plat_st_start_dt, plat_st_end_dt, 
            plat_actual_start_dt, plat_actual_end_dt, qc_op_type, yc_op_type, src_blk_id, src_slot_n, 
            src_row_n, src_level_n, shuff_slot_n, shuff_row_n, shuff_level_n, overstow_cntr_q, yc_id, 
            yc_st_start_dt, yc_st_end_dt, yc_actual_start_dt, yc_actual_end_dt, RTA_choice, tallied_ht_m, 
            tallied_trip_id_mount, tallied_trip_id_offload, assign_state_mount, assign_state_offload, 
            ht_scheduled_start_dt, ht_scheduled_end_dt, ht_actual_start_dt, ht_actual_end_dt, 
            ht_scheduled_mount_location_x, ht_scheduled_offload_location_x, virtual_pool_id, physical_pool_id, 
            door_dir_source, door_dir_dest, is_pending_call_in, called_in_yc_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ]=]
    local stmt = db:prepare(sql_str)
    if not stmt then
        print("Error preparing SQL: ", db:errmsg())
        return
    end

    local max_column_num = 75
    local i = 1
    for row in file:lines() do
        if i > 3 then  -- Skip the first 3 rows
            local parsed_row = {}
            -- Pre-fill all cell in row with nil, to make sure all cell are filled
            for j = 1, max_column_num do
                parsed_row[j] = nil
            end
            
            local fields = splitCsvRow(row)
            for j, cell in ipairs(fields) do
                --[=[ Try to parse as JSON, keep the original string if not valid JSON
                local success, json_object = pcall(cjson.decode, cell)
                if success then
                    print("[" .. i .. "," .. j .. "]: json: " .. cjson.encode(json_object))
                    parsed_row[j] = json_object  -- Store as JSON if valid JSON
                elseif cell == "" then
                    print("[" .. i .. "," .. j .. "]: nil")
                    parsed_row[j] = nil
                else
                    print("[" .. i .. "," .. j .. "]: data: " .. cell)
                    parsed_row[j] = cell  -- Store original string if not valid JSON
                end
                ]=]
                parsed_row[j] = cell
            end
            table.insert(rows, parsed_row)  -- Insert parsed row into table, not used currently
            stmt:bind_values(table.unpack(parsed_row))
            stmt:step()
            stmt:reset()
        end
        i = i + 1  -- Increment row counter
    end

    stmt:finalize()
    file:close()
    return rows
end

local db_init = {}

function db_init.Close_db(db)
    db:close()
end


function db_init.Init_db()
    local db = open_db_create_table("SimulationP3.db")
    -- local db = open_db_create_table(":memory:")
    local filepath = "F:\\Downloads\\GlobalTablesInPreviousSolution_2hr_Modified.csv"
    local filedata = csv_to_db(filepath, db)
    -- close_db(db)
    print("Data inserting complete")
    return db
end

return db_init
