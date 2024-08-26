#include <iostream>
#include <string>
#include <windows.h>
#include <sqlext.h>

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

int main() {
    SQLHENV hEnv = NULL;
    SQLHDBC hDbc = NULL;
    SQLHSTMT hStmt = NULL;
    SQLRETURN retCode;

    // Allocate an environment handle
    retCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    checkError(retCode, hEnv, SQL_HANDLE_ENV);

    // Set the ODBC version environment attribute
    retCode = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    checkError(retCode, hEnv, SQL_HANDLE_ENV);

    // Allocate a connection handle
    retCode = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    checkError(retCode, hDbc, SQL_HANDLE_DBC);

    // Use std::string to define the DSN and SQL queries
    std::string dsn = "DSN=YourSQLiteDSN;";
    std::wstring wdsn(dsn.begin(), dsn.end()); // Convert to std::wstring

    // Connect to the database using DSN
    retCode = SQLDriverConnect(hDbc, NULL, (SQLWCHAR*)wdsn.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    checkError(retCode, hDbc, SQL_HANDLE_DBC);

    // Allocate a statement handle
    retCode = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    checkError(retCode, hStmt, SQL_HANDLE_STMT);

    // Create a table using std::string and convert to wide string
    std::string createTableSQL = "CREATE TABLE IF NOT EXISTS COMPANY(ID INT, NAME TEXT, AGE INT, ADDRESS TEXT, SALARY REAL);";
    std::wstring wCreateTableSQL(createTableSQL.begin(), createTableSQL.end());

    retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wCreateTableSQL.c_str(), SQL_NTS);
    checkError(retCode, hStmt, SQL_HANDLE_STMT);

    // Insert data using std::string and convert to wide string
    std::string insertSQL = "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (1, 'John', 30, 'California', 10000.0);";
    std::wstring wInsertSQL(insertSQL.begin(), insertSQL.end());

    retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wInsertSQL.c_str(), SQL_NTS);
    checkError(retCode, hStmt, SQL_HANDLE_STMT);

    // Query data using std::string and convert to wide string
    std::string selectSQL = "SELECT ID, NAME, AGE, ADDRESS, SALARY FROM COMPANY;";
    std::wstring wSelectSQL(selectSQL.begin(), selectSQL.end());

    retCode = SQLExecDirect(hStmt, (SQLWCHAR*)wSelectSQL.c_str(), SQL_NTS);
    checkError(retCode, hStmt, SQL_HANDLE_STMT);

    // Bind columns and fetch data
    SQLINTEGER id;
    SQLWCHAR name[64];
    SQLINTEGER age;
    SQLWCHAR address[256];
    SQLFLOAT salary;

    SQLBindCol(hStmt, 1, SQL_C_LONG, &id, 0, NULL);
    SQLBindCol(hStmt, 2, SQL_C_WCHAR, name, sizeof(name) / sizeof(SQLWCHAR), NULL);
    SQLBindCol(hStmt, 3, SQL_C_LONG, &age, 0, NULL);
    SQLBindCol(hStmt, 4, SQL_C_WCHAR, address, sizeof(address) / sizeof(SQLWCHAR), NULL);
    SQLBindCol(hStmt, 5, SQL_C_FLOAT, &salary, 0, NULL);

    while (SQLFetch(hStmt) == SQL_SUCCESS) {
        std::wcout << L"ID: " << id << L", Name: " << name << L", Age: " << age
            << L", Address: " << address << L", Salary: " << salary << std::endl;
    }

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    return 0;
}
