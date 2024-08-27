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

int InsertRows(sqlite3* db, int rowNum)
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
    else {
        std::cout << "Records created successfully" << std::endl;
    }
    return rc;
}

int InsertRowsTransaction(sqlite3* db, int rowNum)
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
    else {
        std::cout << "Records created successfully" << std::endl;
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

int ReadTable(sqlite3* db)
{
    char* errMsg = 0;
    // 读取数据
    const char* sqlSelectData = "SELECT * FROM COMPANY";
    auto select_start = std::chrono::high_resolution_clock::now();
    int rc = sqlite3_exec(db, sqlSelectData, callbackWithoutShow, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }
    auto select_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> select_elapsed = select_end - select_start;
    std::cout << "Select time: " << select_elapsed.count() << " seconds" << std::endl;
    return rc;
}

int main() {
    sqlite3* db;
    int rowNumber = 500;
    int runNumber = 10;

    // 打开数据库连接，如果数据库不存在则创建一个新的数据库
    int rc = sqlite3_open("example.db", &db);
    if (rc) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return 1;
    }
    else {
        std::cout << "Opened database successfully" << std::endl;
    }

    double totalTimeCose = 0;
    for (int i = 0; i < runNumber; i++)
    {
        CreatTable(db);
        auto insertStart = std::chrono::high_resolution_clock::now();
        InsertRows(db, rowNumber);
        auto insertEnd = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> insert_elapsed = insertEnd - insertStart;
        std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;
        totalTimeCose += insert_elapsed.count();
    }
    std::cout << "Averave insert time (non-transaction): " << totalTimeCose/runNumber << " seconds" << std::endl;

    totalTimeCose = 0;
    for (int i = 0; i < runNumber; i++)
    {
        CreatTable(db);
        auto insertStart = std::chrono::high_resolution_clock::now();
        InsertRowsTransaction(db, rowNumber);
        auto insertEnd = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> insert_elapsed = insertEnd - insertStart;
        std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;
        totalTimeCose += insert_elapsed.count();
    }
    std::cout << "Averave insert time (transaction): " << totalTimeCose/runNumber << " seconds" << std::endl;

    // 关闭数据库连接
    sqlite3_close(db);
    return 0;
}
