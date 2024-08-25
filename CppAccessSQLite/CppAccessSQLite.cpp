#include <sstream>
#include <iostream>
#include <sqlite3.h>
#include <chrono>

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

int main() {
    sqlite3* db;
    char* errMsg = 0;
    int rc;

    // �����ݿ����ӣ�������ݿⲻ�����򴴽�һ���µ����ݿ�
    rc = sqlite3_open("example.db", &db);
    if (rc) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return 1;
    }
    else {
        std::cout << "Opened database successfully" << std::endl;
    }

    // ����һ����
    const char* sqlCreateTable = "CREATE TABLE IF NOT EXISTS COMPANY("
        "ID INT PRIMARY KEY     NOT NULL,"
        "NAME           TEXT    NOT NULL,"
        "AGE            INT     NOT NULL,"
        "ADDRESS        CHAR(50),"
        "SALARY         REAL );"
        "delete from COMPANY;";

    rc = sqlite3_exec(db, sqlCreateTable, nullptr, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }
    else {
        std::cout << "Table created successfully" << std::endl;
    }

    // ��������
    std::stringstream ss;
    for (int i = 0; i < 1000; i++)
    {
        ss << "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(" << i << ",'" << i << "','" << i << "','" << i << "','" << i << "');" << std::endl;
    }
    std::string sql_str = ss.str();

    auto insert_start = std::chrono::high_resolution_clock::now();
    rc = sqlite3_exec(db, sql_str.c_str(), nullptr, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }
    else {
        std::cout << "Records created successfully" << std::endl;
    }
    auto insert_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> insert_elapsed = insert_end - insert_start;
    std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;

    // ��ȡ����
    const char* sqlSelectData = "SELECT * FROM COMPANY";
    auto select_start = std::chrono::high_resolution_clock::now();
    rc = sqlite3_exec(db, sqlSelectData, callbackWithoutShow, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
    }
    auto select_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> select_elapsed = select_end - select_start;
    std::cout << "Select time: " << select_elapsed.count() << " seconds" << std::endl;

    // �ر����ݿ�����
    sqlite3_close(db);

    return 0;
}
