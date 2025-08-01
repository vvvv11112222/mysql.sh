#!/bin/bash
MYSQL_HOST="192.168.215.128"
MYSQL_PORT="3307"
MYSQL_USER="root"
MYSQL_PASSWORD="rootpass"

for db_idx in $(seq 0 7); do
    db_name="user_${db_idx}"
    
    # 创建数据库
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" << EOF
CREATE DATABASE IF NOT EXISTS ${db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ${db_name};

$(for i in $(seq 0 15); do
    table_idx=$((db_idx * 16 + i))
    echo "CREATE TABLE IF NOT EXISTS user_info_${table_idx} (
        id VARCHAR(36) NOT NULL COMMENT '用户ID',
        name VARCHAR(50) NOT NULL COMMENT '用户名',
        phone VARCHAR(20) NOT NULL COMMENT '手机号',
        age TINYINT UNSIGNED DEFAULT NULL COMMENT '年龄',
        school VARCHAR(100) DEFAULT NULL COMMENT '学校',
        gender TINYINT(1) DEFAULT NULL COMMENT '性别：0-女，1-男，2-未知',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        PRIMARY KEY (id),
        UNIQUE KEY uni_id (id),
        UNIQUE KEY uni_name (name),
        KEY idx_id_name_phone (id,name,phone)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户信息表';"
done)
EOF
done
