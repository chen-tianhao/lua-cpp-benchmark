#include <sstream>
#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include "../sqlite3/sqlite3.h"
#include <set>

std::string DB_ON_DISK = "SimulationP3cpp.db";
std::string DB_IN_MEMORY = ":memory:";
const int NUM_OF_ITER = 1000;
const int NUM_OF_RUNS = 10;

// 回调函数，用于从查询中获取数据
static int callback(void* NotUsed, int argc, char** argv, char** azColName) {
    for (int i = 0; i < argc; i++) {
        std::cout << azColName[i] << ": " << (argv[i] ? argv[i] : "NULL") << std::endl;
    }
    std::cout << std::endl;
    return 0;
}

static int callbackWithoutShow(void* NotUsed, int argc, char** argv, char** azColName) {
    return 0;
}

sqlite3* OpenDb(const char* dbConnStr)
{
    sqlite3* db;
    int ret = sqlite3_open(dbConnStr, &db);
    if (ret) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return NULL;
    }
    else {
        return db;
    }
}

int ReCreateTable(sqlite3* db, std::string table_name)
{
    char* errMsg = (char*)"";
    std::string sqlDropTable = "DROP TABLE IF EXISTS " + table_name + ";";
    int ret = sqlite3_exec(db, sqlDropTable.c_str(), nullptr, 0, &errMsg);
    if (ret != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }

    // Create a table
    std::string sqlCreateTable = "CREATE TABLE " + table_name + "(\r\n\
            vv_c  TEXT,\r\n\
            qc_id  INTEGER,\r\n\
            qc_seq_n  INTEGER,\r\n\
            job_seq_n  INTEGER,\r\n\
            job_id  TEXT,\r\n\
            job_id_int  INTEGER,\r\n\
            cntr_n  TEXT,\r\n\
            ht_type  TEXT,\r\n\
            cntr_wt  INTEGER,\r\n\
            cntr_size  INTEGER,\r\n\
            cntr_type  TEXT,\r\n\
            cntr_op_status  TEXT,\r\n\
            bay_position  INTEGER,\r\n\
            cone_decone_i  INTEGER, -- (bool)\r\n\
            status  TEXT,\r\n\
            hau_gate_in_time  INTEGER,\r\n\
            job_in_htme_i  INTEGER, -- (bool)\r\n\
            lift_type  TEXT,\r\n\
            job_pairing_id  TEXT,\r\n\
            dual_cycle_i  INTEGER, -- (bool)\r\n\
            pri_precedence  TEXT, -- (json)\r\n\
            call_in_precedence  TEXT, -- (json)\r\n\
            wms_duration  INTEGER,\r\n\
            mps_start_time_dt  TEXT,\r\n\
            mps_min_handling_duration  INTEGER,\r\n\
            mps_actual_duration  INTEGER,\r\n\
            pri_task_id  TEXT, -- (not int)\r\n\
            pri_task_state  TEXT,\r\n\
            pri_task_st_start_dt  TEXT,\r\n\
            pri_task_st_end_dt  TEXT,\r\n\
            pri_task_actual_start_dt  TEXT,\r\n\
            pri_task_actual_end_dt  TEXT,\r\n\
            sec_task_id  TEXT, -- (not int)\r\n\
            sec_task_state  TEXT,\r\n\
            sec_task_st_start_dt  TEXT,\r\n\
            sec_task_st_end_dt  TEXT,\r\n\
            sec_task_actual_start_dt  TEXT,\r\n\
            sec_task_actual_end_dt  TEXT,\r\n\
            plat_st_start_dt  TEXT,\r\n\
            plat_st_end_dt  TEXT,\r\n\
            plat_actual_start_dt  TEXT,\r\n\
            plat_actual_end_dt  TEXT,\r\n\
            qc_op_type  TEXT,\r\n\
            yc_op_type  TEXT,\r\n\
            src_blk_id  INTEGER,\r\n\
            src_slot_n  INTEGER,\r\n\
            src_row_n  INTEGER,\r\n\
            src_level_n  INTEGER,\r\n\
            shuff_slot_n  INTEGER,\r\n\
            shuff_row_n  INTEGER,\r\n\
            shuff_level_n  INTEGER,\r\n\
            overstow_cntr_q  INTEGER,\r\n\
            yc_id  TEXT,\r\n\
            yc_st_start_dt  TEXT,\r\n\
            yc_st_end_dt  TEXT,\r\n\
            yc_actual_start_dt  TEXT,\r\n\
            yc_actual_end_dt  TEXT,\r\n\
            RTA_choice  TEXT, -- (json)\r\n\
            tallied_ht_m  TEXT,\r\n\
            tallied_trip_id_mount  TEXT,\r\n\
            tallied_trip_id_offload  TEXT,\r\n\
            assign_state_mount  TEXT,\r\n\
            assign_state_offload  TEXT,\r\n\
            ht_scheduled_start_dt  TEXT,\r\n\
            ht_scheduled_end_dt  TEXT,\r\n\
            ht_actual_start_dt  TEXT,\r\n\
            ht_actual_end_dt  TEXT,\r\n\
            ht_scheduled_mount_location_x  TEXT, -- (json)\r\n\
            ht_scheduled_offload_location_x  TEXT, -- (json)\r\n\
            virtual_pool_id  TEXT,\r\n\
            physical_pool_id  TEXT,\r\n\
            door_dir_source  TEXT,\r\n\
            door_dir_dest  TEXT,\r\n\
            is_pending_call_in  INTEGER,\r\n\
            called_in_yc_id  TEXT,\r\n\
            PRIMARY KEY(job_id_int)\r\n\
            );";

    ret = sqlite3_exec(db, sqlCreateTable.c_str(), nullptr, 0, &errMsg);
    if (ret != SQLITE_OK) {
        std::cerr << "SQL error: " << std::string(errMsg) << std::endl;
        sqlite3_free(errMsg);
    }
    return ret;
}

std::vector<std::string> parseCSVLine(const std::string& line, int jobId) {
    std::vector<std::string> rowValues;
    std::istringstream stream(line);
    std::string cell;
    bool insideQuotes = false;
    char currentChar;

    int j = 0;
    while (stream.get(currentChar)) {
        if (currentChar == '"') {
            insideQuotes = !insideQuotes;
        }
        else if (currentChar == ',' && !insideQuotes) {
            if (j == 4 || j == 5)
                rowValues.push_back(std::to_string(jobId));
            else
                rowValues.push_back(cell);
            cell.clear();
            j++;
        }
        else {
            cell += currentChar;
        }
    }
    // 添加最后一个单元格
    rowValues.push_back(cell);
    return rowValues;
}

int CsvToDbUntilCap(std::string filepath, sqlite3* db, std::string table_name, int cap = INT_MAX, int job_start_id = 1)
{
    if (cap == 0) return 0;

    char* errMsg = 0;
    
    // 打开 CSV 文件
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open the CSV file." << std::endl;
        return -1;
    }

    std::string line;
    std::getline(file, line);  // 读取首行以获取列头
    std::stringstream headerStream(line);
    std::vector<std::string> headers;
    std::string header;

    while (std::getline(headerStream, header, ',')) {
        headers.push_back(header);
    }
    int columnCount = headers.size();

    std::getline(file, line);   // skip 2nd line
    std::getline(file, line);   // skip 3rd line

    // 准备插入语句
    std::string insertSQL = "INSERT INTO " + table_name + "(vv_c, qc_id, qc_seq_n, job_seq_n, job_id, job_id_int, cntr_n, ht_type, cntr_wt,\
        cntr_size, cntr_type, cntr_op_status, bay_position, cone_decone_i, status, hau_gate_in_time,\
        job_in_htme_i, lift_type, job_pairing_id, dual_cycle_i, pri_precedence, call_in_precedence,\
        wms_duration, mps_start_time_dt, mps_min_handling_duration, mps_actual_duration, pri_task_id,\
        pri_task_state, pri_task_st_start_dt, pri_task_st_end_dt, pri_task_actual_start_dt,\
        pri_task_actual_end_dt, sec_task_id, sec_task_state, sec_task_st_start_dt, sec_task_st_end_dt,\
        sec_task_actual_start_dt, sec_task_actual_end_dt, plat_st_start_dt, plat_st_end_dt,\
        plat_actual_start_dt, plat_actual_end_dt, qc_op_type, yc_op_type, src_blk_id, src_slot_n,\
        src_row_n, src_level_n, shuff_slot_n, shuff_row_n, shuff_level_n, overstow_cntr_q, yc_id,\
        yc_st_start_dt, yc_st_end_dt, yc_actual_start_dt, yc_actual_end_dt, RTA_choice, tallied_ht_m,\
        tallied_trip_id_mount, tallied_trip_id_offload, assign_state_mount, assign_state_offload,\
        ht_scheduled_start_dt, ht_scheduled_end_dt, ht_actual_start_dt, ht_actual_end_dt,\
        ht_scheduled_mount_location_x, ht_scheduled_offload_location_x, virtual_pool_id, physical_pool_id,\
        door_dir_source, door_dir_dest, is_pending_call_in, called_in_yc_id)\
        VALUES(? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? ,\
            ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? ,\
            ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? )";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db, insertSQL.c_str(), -1, &stmt, nullptr) != SQLITE_OK) 
    {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return -1;
    }

    // 读取 CSV 数据并插入到数据库
    int i = 0;
    int job_id = job_start_id;
    while (std::getline(file, line)) {
        if (i == cap) break;
        std::vector<std::string> rowValues;
        std::string value;

        rowValues = parseCSVLine(line, job_id++);
        i++;

        if (rowValues.size() != columnCount) {
            std::cerr << "Row column count does not match header count." << std::endl;
            continue;
        }

        std::set<int> columnIdx = { 1,2,3,5,8,9,12,13,15,16,19,22,24,25,44,45,46,47,48,49,50,51,73 };
        for (int j = 0; j < columnCount; j++) {
            if (columnIdx.find(j) != columnIdx.end())
            {
                if (rowValues[j] == "")
                {
                    sqlite3_bind_text(stmt, j + 1, "", -1, SQLITE_TRANSIENT);
                }
                else
                {
                    sqlite3_bind_int(stmt, j + 1, std::stoi(rowValues[j]));
                }
            }
            else
            {
                sqlite3_bind_text(stmt, j + 1, rowValues[j].c_str(), -1, SQLITE_TRANSIENT);
            }
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            std::cerr << "Error inserting data: " << sqlite3_errmsg(db) << std::endl;
        }

        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    file.close();
    
    if (i < cap) // not met the cap, repeat inserting rows
        CsvToDbUntilCap(filepath, db, "JobList", cap - i, job_id);
    return 0;
}

struct ResultStru
{
    double col_B;
    double col_C;
    double col_D;
};

double average(std::vector<ResultStru> data, char column)
{
    double sum = 0;
    int count = 0;

    for (int i = 0; i < data.size(); i++)
    {
        ResultStru row = data[i];
        double cellValue = 0;
        if (column == 'B') cellValue = row.col_B;
        else if (column == 'C') cellValue = row.col_C;
        else if (column == 'D') cellValue = row.col_D;
        sum = sum + cellValue;
        count = count + 1;
        
        if (count == 0)
            return 0;
        else
            return sum / count;
    }
}

void WriteResultToCsv(const char* csvFilename, const char* mode, const char* headers, std::vector<ResultStru> data)
{
    std::ios_base::openmode _mode = std::ios::trunc;
    if (strcmp(mode, "a")==0) _mode = std::ios::app;
    std::ofstream outputFile(csvFilename, _mode);

    if (outputFile.is_open()) {
        outputFile << headers << std::endl;

        for (int i = 0; i < data.size(); i++)
        {
            ResultStru row = data[i];
            // Write each row and separate with quotes if needed
            std::stringstream ss;
            ss << ", " << row.col_B << ", " << row.col_C << ", " << row.col_D;
            outputFile << ss.str() << std::endl;
        }

        double avg_B = average(data, 'B');
        double avg_C = average(data, 'C');
        double avg_D = average(data, 'D');
        std::stringstream ssAvg;
        ssAvg << "Average, " << avg_B << ", " << avg_C << ", " << avg_D;
        std::cout << ssAvg.str() << std::endl;
        outputFile << ssAvg.str() << std::endl;
        outputFile << std::endl;

        outputFile.close();
    }
    else {
        std::cerr << "Error opening file!" << std::endl;
    }
    return;
}

double TestCase0(sqlite3* db, bool isFullCol)
{
    std::string sql_str = "";
    if (isFullCol)
        sql_str = "SELECT * FROM JobList WHERE status = ?";
    else
        sql_str = "SELECT job_id FROM JobList WHERE status = ?";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db, sql_str.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
    {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }
    sqlite3_bind_text(stmt, 1, "L", -1, SQLITE_TRANSIENT);
    
    auto selectStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_OF_ITER; i++)
    {
        // 逐行执行查询并读取结果
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            // 假设列是 TEXT 类型，使用 sqlite3_column_text 读取
            const unsigned char* colValue = sqlite3_column_text(stmt, 0);
            // if (colValue) {
            //     std::cout << "col: " << colValue << std::endl;
            // }
            // else {
            //     std::cout << "col: NULL" << std::endl;
            // }
        }
    }
    auto selectEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> select_elapsed = selectEnd - selectStart;
    // std::cout << "TestCase0(" << (isFullCol?"all)":"one)") << " time: " << select_elapsed.count() << " seconds" << std::endl;
    
    sqlite3_finalize(stmt);
    return select_elapsed.count();
}

void gTestCase0(sqlite3* db)
{
    std::cout << "=============== gTestCase0 ===============" << std::endl;
    std::vector<ResultStru> results;
    for (int i = 0; i < NUM_OF_RUNS; i++)
    {
        ResultStru result;
        result.col_B = TestCase0(db, false);
        result.col_C = 0;
        result.col_D = TestCase0(db, true);
        results.push_back(result);
    }
    WriteResultToCsv("my_outcome_cpp.csv", "w", ",SELECT 1 col,SELECT 42 cols,SELECT * (76 cols)", results);
}


ResultStru TestCase1(sqlite3* db)
{
    char* errMsg;

    //////////////////// Read one col using index ////////////////////
    std::string sqlStr = "SELECT * FROM JobList WHERE job_id_int = 5001";
    auto selectOneStart = std::chrono::high_resolution_clock::now();

    int ret = sqlite3_exec(db, "BEGIN TRANSACTION;", 0, 0, &errMsg);
    if (ret != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        exit(ret);
    }   
    sqlite3_stmt* stmt1;
    if (sqlite3_prepare_v2(db, sqlStr.c_str(), -1, &stmt1, nullptr) != SQLITE_OK)
    {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        exit(-1);
    }
    for (int i = 0; i < NUM_OF_ITER; i++)
    {
        // at most one row because of index is used
        if (sqlite3_step(stmt1) == SQLITE_ROW)
        {
            //local job_id = stmt1 : get_value(4)
            //ret = stmt1 : step()
            //stmt1 : reset()   --no need to reset if binding value is the same
        }
    }
    ret = sqlite3_exec(db, "COMMIT;", 0, 0, &errMsg);
    if (ret != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        exit(ret);
    }
    auto selectOneEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> selectOneElapsed = selectOneEnd - selectOneStart;
    sqlite3_finalize(stmt1);

    //////////////////// Write one col using index ////////////////////
    sqlStr = "Update JobList set ht_type = 'AGV121' WHERE job_id_int = 5001";
    auto writeStart = std::chrono::high_resolution_clock::now();
    ret = sqlite3_exec(db, "BEGIN TRANSACTION;", 0, 0, &errMsg);
    if (ret != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        exit(ret);
    }
    sqlite3_stmt* stmt2;
    if (sqlite3_prepare_v2(db, sqlStr.c_str(), -1, &stmt2, nullptr) != SQLITE_OK)
    {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        exit(-1);
    }
    for (int i = 0; i < NUM_OF_ITER; i++)
    {
        ret = sqlite3_step(stmt2);
    }
    ret = sqlite3_exec(db, "COMMIT;", 0, 0, &errMsg);
    if (ret != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        exit(ret);
    }
    auto writeEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> writeOneElapsed = writeEnd - writeStart;
    sqlite3_finalize(stmt2);

    //////////////////// Read all cols using index and do assignment operation ////////////////////
    sqlStr = "SELECT * FROM JobList WHERE job_id_int = 5001";
    auto selectAllStart = std::chrono::high_resolution_clock::now();
    ret = sqlite3_exec(db, "BEGIN TRANSACTION;", 0, 0, &errMsg);
    if (ret != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        exit(ret);
    }
    sqlite3_stmt* stmt3;
    if (sqlite3_prepare_v2(db, sqlStr.c_str(), -1, &stmt3, nullptr) != SQLITE_OK)
    {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        exit(-1);
    }

    for (int i = 0; i < NUM_OF_ITER; i++)
    {
        ret = sqlite3_step(stmt3);
        // at most one row because of index is used
        if (ret == SQLITE_ROW)
        {
            // const unsigned char* colValue = sqlite3_column_text(stmt3, columnIndex);
            // int intValue = sqlite3_column_int(stmt3, columnIndex);
            // sqlite3_int64 bigIntValue = sqlite3_column_int64(stmt3, columnIndex);
            // double realValue = sqlite3_column_double(stmt3, columnIndex);
            // if (sqlite3_column_type(stmt3, columnIndex) == SQLITE_NULL) { Handle NULL value here }
            const unsigned char* vv_c = sqlite3_column_text(stmt3, 0);
            int qc_id = sqlite3_column_int(stmt3, 1);
            int qc_seq_n = sqlite3_column_int(stmt3, 2);
            int job_seq_n = sqlite3_column_int(stmt3, 3);
            const unsigned char* job_id = sqlite3_column_text(stmt3, 4);
            int job_id_int = sqlite3_column_int(stmt3, 5);
            const unsigned char* cntr_n = sqlite3_column_text(stmt3, 6);
            const unsigned char* ht_type = sqlite3_column_text(stmt3, 7);
            int cntr_wt = sqlite3_column_int(stmt3, 8);
            int cntr_size = sqlite3_column_int(stmt3, 9);
            const unsigned char* cntr_type = sqlite3_column_text(stmt3, 10);
            const unsigned char* cntr_op_status = sqlite3_column_text(stmt3, 11);
            int bay_position = sqlite3_column_int(stmt3, 12);
            int cone_decone_i = sqlite3_column_int(stmt3, 13);
            const unsigned char* status = sqlite3_column_text(stmt3, 14);
            int hau_gate_in_time = sqlite3_column_int(stmt3, 15);
            int job_in_htme_i = sqlite3_column_int(stmt3, 16);
            const unsigned char* lift_type = sqlite3_column_text(stmt3, 17);
            const unsigned char* job_pairing_id = sqlite3_column_text(stmt3, 18);
            int dual_cycle_i = sqlite3_column_int(stmt3, 19);
            const unsigned char* pri_precedence = sqlite3_column_text(stmt3, 20);
            const unsigned char* call_in_precedence = sqlite3_column_text(stmt3, 21);
            int wms_duration = sqlite3_column_int(stmt3, 22);
            const unsigned char* mps_start_time_dt = sqlite3_column_text(stmt3, 23);
            int mps_min_handling_duration = sqlite3_column_int(stmt3, 24);
            int mps_actual_duration = sqlite3_column_int(stmt3, 25);
            const unsigned char* pri_task_id = sqlite3_column_text(stmt3, 26);
            const unsigned char* pri_task_state = sqlite3_column_text(stmt3, 27);
            const unsigned char* pri_task_st_start_dt = sqlite3_column_text(stmt3, 28);
            const unsigned char* pri_task_st_end_dt = sqlite3_column_text(stmt3, 29);
            const unsigned char* pri_task_actual_start_dt = sqlite3_column_text(stmt3, 30);
            const unsigned char* pri_task_actual_end_dt = sqlite3_column_text(stmt3, 31);
            const unsigned char* sec_task_id = sqlite3_column_text(stmt3, 32);
            const unsigned char* sec_task_state = sqlite3_column_text(stmt3, 33);
            const unsigned char* sec_task_st_start_dt = sqlite3_column_text(stmt3, 34);
            const unsigned char* sec_task_st_end_dt = sqlite3_column_text(stmt3, 35);
            const unsigned char* sec_task_actual_start_dt = sqlite3_column_text(stmt3, 36);
            const unsigned char* sec_task_actual_end_dt = sqlite3_column_text(stmt3, 37);
            const unsigned char* plat_st_start_dt = sqlite3_column_text(stmt3, 38);
            const unsigned char* plat_st_end_dt = sqlite3_column_text(stmt3, 39);
            const unsigned char* plat_actual_start_dt = sqlite3_column_text(stmt3, 40);
            const unsigned char* plat_actual_end_dt = sqlite3_column_text(stmt3, 41);
            const unsigned char* qc_op_type = sqlite3_column_text(stmt3, 42);
            const unsigned char* yc_op_type = sqlite3_column_text(stmt3, 43);
            int src_blk_id = sqlite3_column_int(stmt3, 44);
            int src_slot_n = sqlite3_column_int(stmt3, 45);
            int src_row_n = sqlite3_column_int(stmt3, 46);
            int src_level_n = sqlite3_column_int(stmt3, 47);
            int shuff_slot_n = sqlite3_column_int(stmt3, 48);
            int shuff_row_n = sqlite3_column_int(stmt3, 49);
            int shuff_level_n = sqlite3_column_int(stmt3, 50);
            int overstow_cntr_q = sqlite3_column_int(stmt3, 51);
            const unsigned char* yc_id = sqlite3_column_text(stmt3, 52);
            const unsigned char* yc_st_start_dt = sqlite3_column_text(stmt3, 53);
            const unsigned char* yc_st_end_dt = sqlite3_column_text(stmt3, 54);
            const unsigned char* yc_actual_start_dt = sqlite3_column_text(stmt3, 55);
            const unsigned char* yc_actual_end_dt = sqlite3_column_text(stmt3, 56);
            const unsigned char* RTA_choice = sqlite3_column_text(stmt3, 57);
            const unsigned char* tallied_ht_m = sqlite3_column_text(stmt3, 58);
            const unsigned char* tallied_trip_id_mount = sqlite3_column_text(stmt3, 59);
            const unsigned char* tallied_trip_id_offload = sqlite3_column_text(stmt3, 60);
            const unsigned char* assign_state_mount = sqlite3_column_text(stmt3, 61);
            const unsigned char* assign_state_offload = sqlite3_column_text(stmt3, 62);
            const unsigned char* ht_scheduled_start_dt = sqlite3_column_text(stmt3, 63);
            const unsigned char* ht_scheduled_end_dt = sqlite3_column_text(stmt3, 64);
            const unsigned char* ht_actual_start_dt = sqlite3_column_text(stmt3, 65);
            const unsigned char* ht_actual_end_dt = sqlite3_column_text(stmt3, 66);
            const unsigned char* ht_scheduled_mount_location_x = sqlite3_column_text(stmt3, 67);
            const unsigned char* ht_scheduled_offload_location_x = sqlite3_column_text(stmt3, 68);
            const unsigned char* virtual_pool_id = sqlite3_column_text(stmt3, 69);
            const unsigned char* physical_pool_id = sqlite3_column_text(stmt3, 70);
            const unsigned char* door_dir_source = sqlite3_column_text(stmt3, 71);
            const unsigned char* door_dir_dest = sqlite3_column_text(stmt3, 72);
            int is_pending_call_in = sqlite3_column_int(stmt3, 73);
            const unsigned char* called_in_yc_id = sqlite3_column_text(stmt3, 74);
        }
    }

    ret = sqlite3_exec(db, "COMMIT;", 0, 0, &errMsg);
    if (ret != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        exit(ret);
    }
    auto selectAllEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> selectAllElapsed = selectAllEnd - selectAllStart;
    // std::cout << "TestCase1 time cost: " << selectOneElapsed.count() << ", " << writeOneElapsed.count() << ", " << selectAllElapsed.count() << " seconds" << std::endl;
    sqlite3_finalize(stmt3);
    ResultStru result;
    result.col_B = selectOneElapsed.count();
    result.col_C = writeOneElapsed.count();
    result.col_D = selectAllElapsed.count();
    return result;
}

void gTestCase1(sqlite3* db)
{
    std::cout << "=============== gTestCase1 ===============" << std::endl;

    std::vector<ResultStru> results;
    for (int i = 0; i < NUM_OF_RUNS; i++)
    {
        ResultStru result = TestCase1(db);
        results.push_back(result);
    }
    WriteResultToCsv("my_outcome_cpp.csv", "a", ",Read one col using idx, Write one col using idx, Read all cols using idx", results);
}


double TestCase2(sqlite3* db, int numOfTransfer)
{
    char* errMsg;
    auto transferStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_OF_ITER; i++)
    {
        ReCreateTable(db, "JobList_SmallTable");
        std::string sqlStr = "insert into JobList_SmallTable select * from JobList where job_id_int <= " + std::to_string(numOfTransfer);
        int ret = sqlite3_exec(db, sqlStr.c_str(), 0, 0, &errMsg);
        if (ret != SQLITE_OK)
        {
            std::cerr << "SQL error: " << errMsg << std::endl;
            sqlite3_free(errMsg);
            sqlite3_close(db);
            exit(ret);
        }
    }
    auto transferEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> transferElapsed = transferEnd - transferStart;
    std::cout << "TestCase2, transfer " << numOfTransfer << " rows, time cost: " << transferElapsed.count() << " seconds" << std::endl;
    return transferElapsed.count();
}

void  gTestCase2(sqlite3* db)
{
    std::cout << "=============== gTestCase2 ===============" << std::endl;

    std::vector<ResultStru> results;
    for (int i = 0; i < NUM_OF_RUNS; i++)
    {
        ResultStru result;
        double transfer_1 = TestCase2(db, 1);
        double transfer_10 = TestCase2(db, 10);
        double transfer_100 = TestCase2(db, 100);
        result.col_B = transfer_1;
        result.col_C = transfer_10;
        result.col_D = transfer_100;
        results.push_back(result);
    }
    WriteResultToCsv("my_outcome_cpp.csv", "a", ",Transfer 1 row, Transfer 10 rows, Transfer 100 rows", results);
}

sqlite3* InitDb(const char* dbConnStr, int cap, int capSmallTable)
{
    sqlite3* db = OpenDb(dbConnStr);
    // return db;
    int ret = ReCreateTable(db, "JobList");
    if (ret != 0)
    {
        std::cerr << "ReCreateTable JobList fail" << std::endl;
    }
    ret = ReCreateTable(db, "JobList_SmallTable");
    if (ret != 0)
    {
        std::cerr << "ReCreateTable JobList_SmallTable fail" << std::endl;
    }
    const char* filepath = "F:\\Downloads\\GlobalTablesInPreviousSolution_2hr_Modified.csv";
    ret = CsvToDbUntilCap(filepath, db, "JobList", cap);
    ret = CsvToDbUntilCap(filepath, db, "JobList_SmallTable", capSmallTable);

    std::cout<<"Data inserting complete"<<std::endl;
    return db;
}


int main() {
    
    sqlite3* db1 = InitDb(DB_IN_MEMORY.c_str(), 364, 0);
    gTestCase0(db1);
    sqlite3_close(db1);
    
    sqlite3* db2 = InitDb(DB_IN_MEMORY.c_str(), 10000, 0);
    gTestCase1(db2);
    gTestCase2(db2);
    sqlite3_close(db2);
    return 0;
}
