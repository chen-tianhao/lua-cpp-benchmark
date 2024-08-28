#include <iostream>
#include <sstream>
#include <string>
#include <windows.h>
#include <sqlext.h>
#include <chrono>

void checkError(SQLRETURN retCode, SQLHANDLE handle, SQLSMALLINT type) {
    if (retCode != SQL_SUCCESS && retCode != SQL_SUCCESS_WITH_INFO) {
        SQLSMALLINT i = 0;
        SQLINTEGER native;
        SQLWCHAR state[7];
        SQLWCHAR text[256];
        SQLSMALLINT len;
        SQLRETURN ret;
        do {
            ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text) / sizeof(SQLWCHAR), &len);
            if (SQL_SUCCEEDED(ret)) {
                std::wcerr << L"ODBC Error: " << state << L":" << i << L":" << text << std::endl;
            }
        } while (ret == SQL_SUCCESS);
    }
}

int CreatTable(SQLHSTMT hStmt)
{
    // Create a table using std::string and convert to wide string
    std::string recreateTableSQL = "DROP TABLE IF EXISTS COMPANY;"
        "CREATE TABLE IF NOT EXISTS COMPANY("
        "ID INT PRIMARY KEY     NOT NULL,"
        "NAME           TEXT    NOT NULL,"
        "AGE            INT     NOT NULL,"
        "ADDRESS        CHAR(50),"
        "SALARY         REAL );";
    std::wstring wCreateTableSQL(recreateTableSQL.begin(), recreateTableSQL.end());

    SQLRETURN retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wCreateTableSQL.c_str(), SQL_NTS);
    std::cout << "Recreate table." << std::endl;
    checkError(retCode, hStmt, SQL_HANDLE_STMT);
    return retCode;
}

int InsertRows(SQLHSTMT hStmt, int rowNum)
{
    int retCode = 0;
    // Execute each INSERT statement individually
    for (int i = 1; i <= rowNum; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(" << i << ",'" << i << "','" << i << "','" << i << "','" << i << "');" << std::endl;
        std::string insertSQL = ss.str();
        std::wstring wInsertSQL(insertSQL.begin(), insertSQL.end());

        retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wInsertSQL.c_str(), SQL_NTS);
        checkError(retCode, hStmt, SQL_HANDLE_STMT);
    }
    return retCode;
}

int InsertRowsTransaction(SQLHSTMT hStmt, SQLHDBC hDbc, int rowNum)
{
    int retCode = 0;
    // Turn off auto-commit mode for transaction handling
    SQLSetConnectAttr(hDbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    // Execute each INSERT statement individually
    for (int i = 1; i <= rowNum; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(" << i << ",'" << i << "','" << i << "','" << i << "','" << i << "');" << std::endl;
        std::string insertSQL = ss.str();
        std::wstring wInsertSQL(insertSQL.begin(), insertSQL.end());

        retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wInsertSQL.c_str(), SQL_NTS);
        checkError(retCode, hStmt, SQL_HANDLE_STMT);
    }

    // Commit the transaction
    retCode = SQLEndTran(SQL_HANDLE_DBC, hDbc, SQL_COMMIT);
    checkError(retCode, hDbc, SQL_HANDLE_DBC);

    // Turn auto-commit back on
    SQLSetConnectAttr(hDbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    return retCode;
}

int ReadTable(SQLHSTMT hStmt)
{
    // Query data using std::string and convert to wide string
    std::string selectSQL = "SELECT * FROM COMPANY;";
    std::wstring wSelectSQL(selectSQL.begin(), selectSQL.end());

    int retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wSelectSQL.c_str(), SQL_NTS);
    checkError(retCode, hStmt, SQL_HANDLE_STMT);

    // Bind columns and fetch data
    SQLINTEGER id;
    SQLWCHAR name[64];
    SQLINTEGER age;
    SQLWCHAR address[256];
    SQLINTEGER salary;

    SQLBindCol(hStmt, 1, SQL_C_LONG, &id, 0, NULL);
    SQLBindCol(hStmt, 2, SQL_C_WCHAR, name, sizeof(name) / sizeof(SQLWCHAR), NULL);
    SQLBindCol(hStmt, 3, SQL_C_LONG, &age, 0, NULL);
    SQLBindCol(hStmt, 4, SQL_C_WCHAR, address, sizeof(address) / sizeof(SQLWCHAR), NULL);
    SQLBindCol(hStmt, 5, SQL_C_LONG, &salary, 0, NULL);

    /*
    while (SQLFetch(hStmt) == SQL_SUCCESS) {
        std::wcout << L"ID: " << id << L", Name: " << name << L", Age: " << age
            << L", Address: " << address << L", Salary: " << salary << std::endl;
    }
    */
    return retCode;
}

int main() 
{
    SQLHENV hEnv = NULL;
    SQLHDBC hDbc = NULL;
    SQLHSTMT hStmt = NULL;

    // Allocate an environment handle
    int retCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    checkError(retCode, hEnv, SQL_HANDLE_ENV);

    // Set the ODBC version environment attribute
    retCode = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    checkError(retCode, hEnv, SQL_HANDLE_ENV);

    // Allocate a connection handle
    retCode = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    checkError(retCode, hDbc, SQL_HANDLE_DBC);

    // Use std::string to define the connection string with driver and database path
    std::string connectionString = "DRIVER={SQLite3 ODBC Driver};DATABASE=example.db;";
    std::wstring wConnectionString(connectionString.begin(), connectionString.end());

    // Connect to the database using connection string
    retCode = SQLDriverConnect(hDbc, NULL, (SQLWCHAR*)wConnectionString.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    checkError(retCode, hDbc, SQL_HANDLE_DBC);

    // Allocate a statement handle
    retCode = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    checkError(retCode, hStmt, SQL_HANDLE_STMT);

    int rowNumber = 500;
    int runNumber = 10;

    double totalTimeCose = 0;
    for (int i = 0; i < runNumber; i++)
    {
        CreatTable(hStmt);
        auto insertStart = std::chrono::high_resolution_clock::now();
        InsertRows(hStmt, rowNumber);
        auto insertEnd = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> insert_elapsed = insertEnd - insertStart;
        std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;
        totalTimeCose += insert_elapsed.count();
    }
    std::cout << "Averave insert time (non-transaction): " << totalTimeCose / runNumber << " seconds" << std::endl;
    
    totalTimeCose = 0;
    for (int i = 0; i < runNumber; i++)
    {
        CreatTable(hStmt);
        auto insertStart = std::chrono::high_resolution_clock::now();
        InsertRowsTransaction(hStmt, hDbc, rowNumber);
        auto insertEnd = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> insert_elapsed = insertEnd - insertStart;
        std::cout << "Insert time: " << insert_elapsed.count() << " seconds" << std::endl;
        totalTimeCose += insert_elapsed.count();
    }
    std::cout << "Averave insert time (transaction): " << totalTimeCose / runNumber << " seconds" << std::endl;

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    return 0;
}
