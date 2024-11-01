#include <sstream>
#include <iostream>
#include <chrono>
#include "../sqlite3/sqlite3.h"

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

int CreatTable(sqlite3* db)
{
    char* errMsg = 0;
    // 创建一个表
    const char* sqlCreateTable = "DROP TABLE IF EXISTS COMPANY;"
        "CREATE TABLE COMPANY("
        "ID INT PRIMARY KEY     NOT NULL,"
        "NAME           TEXT    NOT NULL,"
        "AGE            INT     NOT NULL,"
        "ADDRESS        CHAR(50),"
        "SALARY         REAL );";

    int rc = sqlite3_exec(db, sqlCreateTable, nullptr, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }
    else {
        std::cout << "Table created successfully" << std::endl;
    }
    return rc;
}

int BatchInsert(sqlite3* db, int rowNum)
{
    char* errMsg = 0;

    // 开始事务
    int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", 0, 0, &errMsg);
    if (rc != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        return rc;
    }

    std::stringstream ss;
    // 插入数据
    for (int i = 0; i < rowNum; i++)
    {
        ss << "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(" << i << ",'" << i << "','" << i << "','" << i << "','" << i << "');" << std::endl;
    }
    std::string sql_str = ss.str();

    rc = sqlite3_exec(db, sql_str.c_str(), nullptr, 0, &errMsg);
    if (rc != SQLITE_OK) 
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        // 如果插入过程中发生错误，回滚事务
        sqlite3_exec(db, "ROLLBACK;", 0, 0, &errMsg);
        sqlite3_close(db);
        return rc;
    }

    // 提交事务
    rc = sqlite3_exec(db, "COMMIT;", 0, 0, &errMsg);
    if (rc != SQLITE_OK) 
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        return rc;
    }
    return rc;
}

int SingleInsert(sqlite3* db, int rowNum)
{
    char* errMsg = 0;
    // 插入数据
    std::stringstream ss;
    for (int i = 0; i < rowNum; i++)
    {
        ss << "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(" << i << ",'" << i << "','" << i << "','" << i << "','" << i << "');" << std::endl;
    }
    std::string sql_str = ss.str();

    int rc = sqlite3_exec(db, sql_str.c_str(), nullptr, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }
    return rc;
}

int ReadTable(sqlite3* db, std::string key, int value)
{
    char* errMsg = 0;
    std::string sqlSelectData = "SELECT * FROM COMPANY WHERE " + key + " = " + std::to_string(value) + ";";
    //auto select_start = std::chrono::high_resolution_clock::now();
    int rc = sqlite3_exec(db, sqlSelectData.c_str(), callbackWithoutShow, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        return rc;
    }
    //auto select_end = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double> select_elapsed = select_end - select_start;
    //std::cout << "Select time: " << select_elapsed.count() << " seconds" << std::endl;
    return rc;
}

int UpdateTable(sqlite3* db, double setValue, std::string key, int value)
{
    char* errMsg = 0;
    std::string sqlSelectData = "UPDATE COMPANY SET SALARY = " + std::to_string(setValue) + " WHERE " + key + " = " + std::to_string(value) + ";";
    //auto select_start = std::chrono::high_resolution_clock::now();
    int rc = sqlite3_exec(db, sqlSelectData.c_str(), callbackWithoutShow, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        return rc;
    }
    //auto select_end = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double> select_elapsed = select_end - select_start;
    //std::cout << "Select time: " << select_elapsed.count() << " seconds" << std::endl;
    return rc;
}

int main() {
    sqlite3* db;
    int rowNumber = 10000;
    int runNumberInsert = 10;
    int runNumberSelect = 10;

    // 打开数据库连接，如果数据库不存在则创建一个新的数据库
    int rc = sqlite3_open(":memory:", &db);
    if (rc) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return 1;
    }
    else {
        std::cout << "Opened database successfully" << std::endl;
    }
    
    // Batch Insert
    double totalTimeCose = 0;
    for (int i = 0; i < runNumberInsert; i++)
    {
        CreatTable(db);
        auto insertStart = std::chrono::high_resolution_clock::now();
        BatchInsert(db, rowNumber);
        auto insertEnd = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> insert_elapsed = insertEnd - insertStart;
        std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;
        totalTimeCose += insert_elapsed.count();
    }
    std::cout << "Averave batch insert time: " << totalTimeCose/runNumberInsert << " seconds" << std::endl;

    // Read common
    auto readStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runNumberSelect; i++)
    {
        ReadTable(db, "AGE", runNumberSelect + 100);
    }
    auto readEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> read_elapsed = readEnd - readStart;
    std::cout << "Averave select time (common): " << read_elapsed.count() / runNumberSelect << " seconds" << std::endl;

    // Read by key
    auto readByKeyStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runNumberSelect; i++)
    {
        ReadTable(db, "ID", runNumberSelect + 100);
    }
    auto readByKeyEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> read_by_key_elapsed = readByKeyEnd - readByKeyStart;
    std::cout << "Averave select time (by key): " << read_by_key_elapsed.count() / runNumberSelect << " seconds" << std::endl;

    // Update common
    auto updateStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runNumberSelect; i++)
    {
        UpdateTable(db, 88.8, "AGE", runNumberSelect + 100);
    }
    auto updateEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> update_elapsed = updateEnd - updateStart;
    std::cout << "Averave update time (common): " << update_elapsed.count() / runNumberSelect << " seconds" << std::endl;

    // Update by key
    auto updateByKeyStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runNumberSelect; i++)
    {
        UpdateTable(db, 99.9, "ID", runNumberSelect + 100);
    }
    auto updateByKeyEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> update_by_key_elapsed = updateByKeyEnd - updateByKeyStart;
    std::cout << "Averave update time (by key): " << update_by_key_elapsed.count() / runNumberSelect << " seconds" << std::endl;

    // Single Insert
    totalTimeCose = 0;
    for (int i = 0; i < runNumberInsert; i++)
    {
        CreatTable(db);
        auto insertStart = std::chrono::high_resolution_clock::now();
        SingleInsert(db, rowNumber);
        auto insertEnd = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> insert_elapsed = insertEnd - insertStart;
        std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;
        totalTimeCose += insert_elapsed.count();
    }
    std::cout << "Averave single insert time: " << totalTimeCose / runNumberInsert << " seconds" << std::endl;

    // 关闭数据库连接
    sqlite3_close(db);
    return 0;
}
