local socket = require("socket")
local luasql = require("luasql.sqlite3")

local num_of_rows = 10000
local run_num_of_insert = 1
local run_num_of_select = 10

local env = luasql.sqlite3()

local conn = env:connect("example.db")

function CreateTable()
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
end

function SingleInsert()
	for i = 1, num_of_rows do
        local sql = string.format(
            "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(%d, '%s', %d, '%s', '%f');", 
            i, tostring(i), i, tostring(i), i
        )
		conn:execute(sql)
    end
    print("Records created successfully")
end

function BatchInsert()
	conn:execute("BEGIN TRANSACTION;")
    for i = 1, num_of_rows do
        local sql = string.format(
            "INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES(%d, '%s', %d, '%s', '%f');", 
            i, tostring(i), i, tostring(i), i
        )
		conn:execute(sql)
    end
	conn:execute("COMMIT;")
    print("Records created successfully")
end

function ReadTable(key, value)
    local cursor, err = conn:execute("SELECT * FROM COMPANY WHERE" + key + " = " + value + ";")
    if not cursor then
        error("Failed to execute SELECT statement: " .. err)
    end

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

    return read_end - read_start
end

function UpdateTable(setValue, key, value)
    local cursor, err = conn:execute("UPDATE COMPANY SET SALARY = " + setValue + " WHERE " + key + " = " + value + ";")
    if not cursor then
        error("Failed to execute SELECT statement: " .. err)
    end

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
end

local total_insert = 0
for i = 1, run_num_of_insert do
	CreateTable()
	local insert_start = socket.gettime()
    local insert_second = BatchInsert()
	local insert_end = socket.gettime()
	print(string.format("Insert time: %.6f seconds", insert_end - insert_start))
    total_insert = total_insert + insert_end - insert_start
end
print(string.format("Average batch insert time: %.6f seconds", total_insert / run_num_of_insert))

total_insert = 0
for i = 1, run_num_of_insert do
	CreateTable()
	local insert_start = socket.gettime()
    local insert_second = SingleInsert()
	local insert_end = socket.gettime()
	print(string.format("Insert time: %.6f seconds", insert_end - insert_start))
    total_insert = total_insert + insert_end - insert_start
end
print(string.format("Average single insert time: %.6f seconds", total_insert / run_num_of_insert))

local total_read = 0
-- print(string.format("Average read time: %.6f seconds", total_read / run_num_of_insert))

conn:close()
env:close()
