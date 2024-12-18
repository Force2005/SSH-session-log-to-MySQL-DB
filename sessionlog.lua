local mysql = require("luasql.mysql")
local io = require("io")
local os = require("os")

local db_host = "localhost"
local db_name = "ssh_logs"
local db_user = "Ваш_пользователь"
local db_pass = "Ваш_пароль"
local db_table = "connections"

local colors = {
    reset = "\27[0m",
    red = "\27[31m",
    green = "\27[32m"
}

local env = assert(mysql.mysql())
local conn

local function execute_query_safe(query)
    local success, err = pcall(function()
        return conn:execute(query)
    end)

    if not success then
        io.stderr:write(colors.red .. "Ошибка выполнения запроса: " .. (err or "unknown") .. "\n" .. colors.reset)
    end

    return success
end

local function setup_database()
    local temp_conn = assert(env:connect("", db_user, db_pass, db_host))
    local db_exists_query = string.format(
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'",
        db_name
    )
    local cursor = temp_conn:execute(db_exists_query)
    local db_exists = cursor:fetch() ~= nil
    if not db_exists then
        execute_query_safe("CREATE DATABASE " .. db_name)
        print(colors.green .. "Создана база данных: " .. db_name .. colors.reset)
    else
        print(colors.green .. "База данных уже существует: " .. db_name .. colors.reset)
    end
    cursor:close()
    temp_conn:close()
    conn = assert(env:connect(db_name, db_user, db_pass, db_host))
    local create_table_query = [[
        CREATE TABLE IF NOT EXISTS `connections` (
            `id` int NOT NULL AUTO_INCREMENT,
            `ip` varchar(45) NOT NULL,
            `username` varchar(50) NOT NULL,
            `connect_time` datetime DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    ]]
    execute_query_safe(create_table_query)
end

local function connect_to_db()
    local attempts = 0
    while attempts < 5 do
        local success, db_conn = pcall(function()
            setup_database()
            return conn
        end)

        if success then
            print(colors.green .. "Подключение к базе данных установлено." .. colors.reset)
            return conn
        else
            attempts = attempts + 1
            io.stderr:write(colors.red .. "Ошибка подключения к базе данных. Повторная попытка...\n" .. colors.reset)
            os.execute("sleep 5")
        end
    end
    error("Не удалось подключиться к базе данных после 5 попыток.")
end

conn = connect_to_db()

local function ensure_connection()
    if not conn then
        conn = connect_to_db()
    else
        local success, err = pcall(function()
            conn:execute("SELECT 1")
        end)

        if not success then
            io.stderr:write(colors.red .. "Соединение с базой данных потеряно. Переподключение...\n" .. colors.reset)
            conn = connect_to_db()
        end
    end
end

local function log_ip(ip, username)
    ensure_connection()

    local query = string.format(
        "INSERT INTO %s (ip, username, connect_time) VALUES ('%s', '%s', NOW())",
        db_table, ip, username
    )

    local success, err = pcall(function()
        return conn:execute(query)
    end)

    if not success then
        io.stderr:write(colors.red .. "Ошибка записи в базу данных: " .. (err or "unknown") .. "\n" .. colors.reset)
    end
end

local function process_ssh_connection(line)
    local username, ip = line:match("Accepted .- for (%S+) from (%d+%.%d+%.%d+%.%d+)")
    if ip and username then
        local connect_time = os.date("%Y-%m-%d %H:%M:%S")
        log_ip(ip, username)
        print(string.format(
            "[%s%s%s] SSH Подключение: IP=%s%s%s, Пользователь=%s",
            colors.green, connect_time, colors.reset,
            colors.red, ip, colors.reset,
            username
        ))
    end
end

local function monitor_ssh_logs()
    local auth_log_file = "/var/log/auth.log"
    local file, err = io.open(auth_log_file, "r")

    if not file then
        io.stderr:write(colors.red .. "Ошибка открытия файла логов: " .. err .. "\n" .. colors.reset)
        os.exit(1)
    end

    file:seek("end")

    while true do
        local line = file:read()
        if line then
            pcall(function()
                process_ssh_connection(line)
            end)
        else
            os.execute("sleep 1")
        end
    end
end

pcall(monitor_ssh_logs)

