local sqlite3 = require("lsqlite3")
local file = io.open("F:\\Downloads\\GlobalTablesInPreviousSolution_2hr_Modified.csv", "r")

local db = sqlite3.open("SimulationP3.db")

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
    return
end

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
local insert_sql = db:prepare(sql_str)
if not insert_sql then
    print("Error preparing SQL: ", db:errmsg())
    return
end

local column_max = 75  -- set first N column to read
local i=1
for line in file:lines() do
    if i <= 3 then
        i = i + 1  -- skip first 3 lines (head)
    else
        local row = {}
        local column_idx = 1

        for cell in line:gmatch("([^,]*),?") do
            if column_idx <= column_max then
                if cell == "" then
                    cell = nil
                end
                table.insert(row, cell)
            end
            column_idx = column_idx + 1
        end

        if #row == column_max then
            insert_sql:bind_values(table.unpack(row))
            insert_sql:step()
            insert_sql:reset()
        end
    end
end

insert_sql:finalize()
db:close()
file:close()

print("Data inserting complete")
