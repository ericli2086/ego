#!/bin/bash

# SQLite3 数据库文件
DB_FILE="test.db"

sqlite3 $DB_FILE <<EOF
-- 创建表结构
CREATE TABLE IF NOT EXISTS user (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    email TEXT,
    phone TEXT,
    age INTEGER,
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_time DATETIME DEFAULT NULL
);

-- 创建单独的唯一索引 (email)
CREATE UNIQUE INDEX IF NOT EXISTS uk_email ON user (email);

-- 创建单独的唯一索引 (phone)
CREATE UNIQUE INDEX IF NOT EXISTS uk_phone ON user (phone);

-- 创建联合唯一索引 (email 和 deleted_time)
CREATE UNIQUE INDEX IF NOT EXISTS uk_email_deleted_time ON user (email, deleted_time);

-- 创建联合唯一索引 (phone 和 deleted_time)
CREATE UNIQUE INDEX IF NOT EXISTS uk_phone_deleted_time ON user (phone, deleted_time);
EOF

# 插入 1000000 条数据
for i in $(seq 1 100)
do
    # 随机生成数据，部分字段可以为空
    username="user_$i"
    email="user_$i@example.com"
    phone="1234567890$i"
    created_time=$(date +"%Y-%m-%d %H:%M:%S")
    updated_time=$(date +"%Y-%m-%d %H:%M:%S")
    deleted_time=NULL  # 默认删除时间为空，表示未删除

    # 执行插入 SQL
    sqlite3 $DB_FILE "INSERT INTO user (username, email, phone, created_time, updated_time, deleted_time) VALUES ('$username', '$email', '$phone', '$created_time', '$updated_time', $deleted_time);"

    # 每10000条输出一次
    if (( i % 10000 == 0 )); then
        echo "Inserted $i records"
    fi
done

echo "Insertion completed!"

mkdir -p ./cfgs/sqlite
cat  << EOF > ./cfgs/sqlite/_base.yaml
type: sqlite
dsn: "./test.db"
database: test
pool:
  max_open_conns: 20
  max_idle_conns: 10
  max_life_time: 3600s
  max_idle_time: 300s
EOF