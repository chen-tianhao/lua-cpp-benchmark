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
            --print(string.format("ID: %d, Name: %s, Age: %d, Address: %s, Salary: %.2f", 
            --    row.ID, row.NAME, row.AGE, row.ADDRESS, row.SALARY))
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


local insert_start = os.clock()
local insert_start2 = socket.gettime()
for i = 1, 1000 do
	insert_into_company(i, tostring(i), i, tostring(i), i)
end
local insert_end = os.clock()
local insert_end2 = socket.gettime()
print(string.format("Average insert time: %.6f(%.6f) seconds", insert_end - insert_start, insert_end2 - insert_start2))

-- 示例: 查询年龄大于 24 的员工
select_from_company(function(row)
    return row.AGE > 24
end)

-- 示例: 将 ID 为 2 的员工的工资更新为 18000.00
update_company(function(row)
    return row.ID == 2
end, {SALARY = 18000.00})
