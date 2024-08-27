#include <sstream>
#include <iostream>
#include <chrono>
#include "../sqlite3/sqlite3.h"

// �ص����������ڴӲ�ѯ�л�ȡ����
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
    // ����һ����
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
    // ��������
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

    // ��ʼ����
    int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", 0, 0, &errMsg);
    if (rc != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        return rc;
    }

    std::stringstream ss;
    // ��������
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
        // �����������з������󣬻ع�����
        sqlite3_exec(db, "ROLLBACK;", 0, 0, &errMsg);
        sqlite3_close(db);
        return rc;
    }
    else {
        std::cout << "Records created successfully" << std::endl;
    }

    // �ύ����
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
    // ��ȡ����
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

    // �����ݿ����ӣ�������ݿⲻ�����򴴽�һ���µ����ݿ�
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

    // �ر����ݿ�����
    sqlite3_close(db);
    return 0;
}
