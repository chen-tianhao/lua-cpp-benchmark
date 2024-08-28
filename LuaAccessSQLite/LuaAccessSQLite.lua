local socket = require("socket")
local luasql = require("luasql.sqlite3")

local num_of_rows = 1000
local num_of_runs = 10

local env = luasql.sqlite3()

local conn = env:connect("example.db")

function experiment()
    conn:execute("DROP TABLE IF EXISTS COMPANY;")
    conn:execute([[
        CREATE TABLE COMPANY(
            ID INT PRIMARY KEY NOT NULL,
            NAME TEXT NOT NULL,
            AGE INT NOT NULL,
            ADDRESS CHAR(50),
            SALARY REAL );
    ]])
    print("Table created successfully")

    local insert_statements = {}
    for i = 1, num_of_rows do
        table.insert(insert_statements, string.format(
            "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(%d, '%s', %d, '%s', '%f');", 
            i, tostring(i), i, tostring(i), i
        ))
    end

    local insert_start = socket.gettime()
    for _, sql in ipairs(insert_statements) do
        conn:execute(sql)
    end
    local insert_end = socket.gettime()
    print("Records created successfully")
    -- print(string.format("Insert time: %.4f seconds", insert_end - insert_start))

    local read_start = socket.gettime()
    local cursor, err = conn:execute("SELECT * FROM COMPANY;")
    if not cursor then
        error("Failed to execute SELECT statement: " .. err)
    end
    local read_end = socket.gettime()
    -- print(string.format("Select time: %.4f seconds", read_end - read_start))

    --[[
    local row = cursor:fetch({}, "a")
    while row do
        for key, value in pairs(row) do
            print(key .. ": " .. tostring(value))
        end
        print("---")
        row = cursor:fetch(row, "a")
    end
    ]]
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

print(string.format("Average insert time: %.6f seconds", total_insert / num_of_runs))
print(string.format("Average read time: %.6f seconds", total_read / num_of_runs))

conn:close()
env:close()
