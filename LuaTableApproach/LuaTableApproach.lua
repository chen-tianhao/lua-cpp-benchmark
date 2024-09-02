local socket = require("socket")
-- 定义 COMPANY 表
local COMPANY = {}

-- 插入数据行到表中
function insert_into_company(id, name, age, address, salary)
    table.insert(COMPANY, {
        ID = id,
        NAME = name,
        AGE = age,
        ADDRESS = address,
        SALARY = salary
    })
end

function print_table()
    -- 打印表中的数据
    for i, row in ipairs(COMPANY) do
        print(string.format("ID: %d, Name: %s, Age: %d, Address: %s, Salary: %.2f", 
            row.ID, row.NAME, row.AGE, row.ADDRESS, row.SALARY))
    end
end

function print_table_structure()
    -- 定义表结构
    local COMPANY_STRUCTURE = {
        ID = "INT PRIMARY KEY NOT NULL",
        NAME = "TEXT NOT NULL",
        AGE = "INT NOT NULL",
        ADDRESS = "CHAR(50)",
        SALARY = "REAL"
    }

    -- 打印表结构
    for column, definition in pairs(COMPANY_STRUCTURE) do
        print(string.format("Column: %s, Definition: %s", column, definition))
    end
end

-- 查询数据
function select_from_company(condition)
    for i, row in ipairs(COMPANY) do
        if condition(row) then
            -- print(string.format("ID: %d, Name: %s, Age: %d, Address: %s, Salary: %.2f", 
            --     row.ID, row.NAME, row.AGE, row.ADDRESS, row.SALARY))
        end
    end
end

-- 更新数据
function update_company(condition, updates)
    for i, row in ipairs(COMPANY) do
        if condition(row) then
            for k, v in pairs(updates) do
                row[k] = v
            end
        end
    end
end

local num_of_rows = 10000
local num_of_runs_insert = 1
local num_of_runs_select = 10

local insert_start = os.clock()
local insert_start2 = socket.gettime()
for i = 1, num_of_runs_insert do
    for j = 1, num_of_rows do
        insert_into_company(j, tostring(j), j, tostring(j), j)
    end
end
local insert_end = os.clock()
local insert_end2 = socket.gettime()
print(string.format("Average insert time: %.6f(%.6f) seconds", insert_end - insert_start, insert_end2 - insert_start2))

local select_start = os.clock()
local select_start2 = socket.gettime()
for i = 1, num_of_runs_select do
    select_from_company(function(row)
        return row.AGE == (i + 24)
    end)
end
local select_end = os.clock()
local select_end2 = socket.gettime()
print(string.format("Average select time: %.6f(%.6f) seconds", select_end - select_start, select_end2 - select_start2))

local update_start = os.clock()
local update_start2 = socket.gettime()
for i = 1, num_of_runs_select do
    update_company(function(row)
        return row.ID == (i + 24)
    end, {SALARY = 11000.00})
end
local update_end = os.clock()
local update_end2 = socket.gettime()
print(string.format("Average update time: %.6f(%.6f) seconds", update_end - update_start, update_end2 - update_start2))

select_from_company(function(row)
    return row.SALARY > 10009.00
end)

-- 示例: 查询年龄大于 24 的员工
-- select_from_company(function(row)
--     return row.AGE > 24
-- end)

-- 示例: 将 ID 为 2 的员工的工资更新为 18000.00
-- update_company(function(row)
--     return row.ID == 2
-- end, {SALARY = 18000.00})
