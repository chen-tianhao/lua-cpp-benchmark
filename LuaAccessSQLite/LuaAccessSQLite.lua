local socket = require("socket")
-- 加载 LuaSQL SQLite3 模块
local luasql = require("luasql.sqlite3")

local num_of_rows = 1000
local num_of_runs = 10

-- 创建一个 SQLite3 环境对象
local env = luasql.sqlite3()

-- 连接到数据库，如果数据库文件不存在则会创建一个新的数据库
local conn = env:connect("example.db")

function experiment()
    -- 创建一个表，如果表不存在
    local create_table_sql = [[
    DROP TABLE IF EXISTS COMPANY;
    CREATE TABLE COMPANY(
            ID INT PRIMARY KEY     NOT NULL,
            NAME           TEXT    NOT NULL,
            AGE            INT     NOT NULL,
            ADDRESS        CHAR(50),
            SALARY         REAL );]]
    conn:execute(create_table_sql)
    print("Table created successfully")

    -- 插入数据
    local insert_statements = {}
    for i = 1, num_of_rows do
        table.insert(insert_statements, string.format("INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(%d, '%d', %d, '%d', '%d');", i, i, i, i, i))
    end
    local insert_start = socket.gettime()
    for _, sql in ipairs(insert_statements) do
        conn:execute(sql)
    end
    local insert_end = socket.gettime()
    print("Records created successfully")
    print(string.format("Insert time: %.4f seconds", insert_end - insert_start))

    -- 查询数据
    local read_start = socket.gettime()
    local cursor = conn:execute("SELECT * FROM COMPANY;")
    local read_end = socket.gettime()
    print(string.format("Select time: %.4f seconds", read_end - read_start))

    -- 遍历结果集
    local row = cursor:fetch({}, "a")  -- "a" 
    while row do
        for key, value in pairs(row) do
            print(key .. ": " .. tostring(value))
        end
        print("---") -- 分隔每一行
        row = cursor:fetch(row, "a")
    end
    -- 关闭游标
    cursor:close()

    return insert_end - insert_start, read_end - read_start
end

local total_insert = 0
local total_read = 0
for i = 1, num_of_runs do
    local insert_second, read_second = experiment()
    total_insert = total_insert + insert_second
    total_read = total_read + read_second
end

print(string.format("Insert time: %.6f seconds", total_insert/num_of_runs))
print(string.format("Read time: %.6f seconds", total_read/num_of_runs))

-- 关闭连接
conn:close()

-- 关闭环境对象
env:close()

