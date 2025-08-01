#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <mysql.h>
#include <zlib.h>  // 用于CRC32计算
#include <time.h>   // 用于时间操作

// 添加 Windows 平台头文件支持
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

// 数据库配置
#define MASTER_HOST "192.168.215.128"
#define SLAVE_HOST "192.168.215.128"
#define DB_USER "root"
#define DB_PASSWORD_MASTER "rootpass"
#define DB_PASSWORD_SLAVE "rootpass"
#define DB_PORT_MASTER 3307
#define DB_PORT_SLAVE 3308

#define START_ID 100000000LL
#define USER_COUNT 1000
#define MAX_RETRIES 30
#define RETRY_DELAY_MS 100

// 分库分表核心配置（8库×8表=64分片）
#define DB_COUNT 8       // 总库数
#define TABLE_PER_DB 8   // 每个库的表数
#define TOTAL_SHARDS (DB_COUNT * TABLE_PER_DB)  // 总分片数：64

// 全局同步条件变量
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int sync_completed = 0;

// 数据库连接结构
typedef struct {
    MYSQL* write_conn;  // 主库连接
    MYSQL* read_conn;   // 从库连接
} DBConnections;

// 分库分表计算函数（核心修改）
void calculate_shard(long long id, int* db_index, int* table_index) {
    // 1. 计算ID的CRC32哈希值（确保数据均匀分布）
    uLong crc = crc32(0L, (const Bytef*)&id, sizeof(id));

    // 2. 取模64得到分片索引（0-63，对应64个分片）
    int shard_index = crc % TOTAL_SHARDS;  // TOTAL_SHARDS=64

    // 3. 计算库索引（0-7）：每个库负责8个分片
    *db_index = shard_index / TABLE_PER_DB;  // 例如：0-7→0，8-15→1，…，56-63→7

    // 4. 计算表索引（0-7）：每个库内的表偏移量
    *table_index = shard_index % TABLE_PER_DB;  // 例如：分片9→9%8=1→表索引1
}

// 跨平台毫秒级睡眠函数
void msleep(unsigned int milliseconds) {
#ifdef _WIN32
    Sleep(milliseconds);
#else
    struct timespec ts;
    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (milliseconds % 1000) * 1000000;
    nanosleep(&ts, NULL);
#endif
}

// 初始化数据库连接
DBConnections* init_db_connections() {
    DBConnections* conns = malloc(sizeof(DBConnections));
    if (!conns) return NULL;

    // 初始化写连接（主库）
    conns->write_conn = mysql_init(NULL);
    if (!mysql_real_connect(conns->write_conn, MASTER_HOST, DB_USER,
        DB_PASSWORD_MASTER, NULL, DB_PORT_MASTER, NULL, 0)) {
        fprintf(stderr, "Master connection error: %s\n", mysql_error(conns->write_conn));
        free(conns);
        return NULL;
    }

    // 初始化读连接（从库）
    conns->read_conn = mysql_init(NULL);
    if (!mysql_real_connect(conns->read_conn, SLAVE_HOST, DB_USER,
        DB_PASSWORD_SLAVE, NULL, DB_PORT_SLAVE, NULL, 0)) {
        fprintf(stderr, "Slave connection error: %s\n", mysql_error(conns->read_conn));
        mysql_close(conns->write_conn);
        free(conns);
        return NULL;
    }

    return conns;
}

// 创建数据库和表结构（核心修改）
int create_database_schema(MYSQL* conn) {
    // 创建8个库（user_0到user_7）
    for (int db = 0; db < DB_COUNT; db++) {  // 循环8次（0-7）
        char db_query[128];
        snprintf(db_query, sizeof(db_query), "CREATE DATABASE IF NOT EXISTS user_%d", db);
        if (mysql_query(conn, db_query)) {
            fprintf(stderr, "Create DB error: %s\n", mysql_error(conn));
            return 0;
        }

        // 切换到当前库
        char use_db[128];
        snprintf(use_db, sizeof(use_db), "USE user_%d", db);
        if (mysql_query(conn, use_db)) {
            fprintf(stderr, "Use DB error: %s\n", mysql_error(conn));
            return 0;
        }

        // 每个库创建8张表（user_info_0到user_info_7）
        for (int table = 0; table < TABLE_PER_DB; table++) {  // 循环8次（0-7）
            char table_query[256];
            snprintf(table_query, sizeof(table_query),
                "CREATE TABLE IF NOT EXISTS user_info_%d ("
                "id BIGINT PRIMARY KEY, "  // 用户ID（主键）
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"  // 插入时间
                ")", table);

            if (mysql_query(conn, table_query)) {
                fprintf(stderr, "Create table error: %s\n", mysql_error(conn));
                return 0;
            }
        }
    }
    return 1;  // 所有库表创建成功
}

// 插入用户数据到主库（自动适配新的分库分表规则）
void insert_users(DBConnections* conns) {
    printf("Inserting %d users...\n", USER_COUNT);

    for (long long i = 0; i < USER_COUNT; i++) {
        long long user_id = START_ID + i;
        int db_index, table_index;
        calculate_shard(user_id, &db_index, &table_index);  // 使用修改后的分片函数

        char query[256];
        snprintf(query, sizeof(query),
            "INSERT INTO user_%d.user_info_%d (id) VALUES (%lld)",
            db_index, table_index, user_id);  // 库和表索引自动适配0-7

        if (mysql_query(conns->write_conn, query)) {
            fprintf(stderr, "Insert failed: %s\nError: %s\n",
                query, mysql_error(conns->write_conn));
        }
    }
    printf("Insert completed.\n");
}

// 检查从库数据同步状态（自动适配新的分库分表规则）
void* check_sync_status(void* arg) {
    DBConnections* conns = (DBConnections*)arg;
    long long last_id = START_ID + USER_COUNT - 1;
    int db_index, table_index;
    calculate_shard(last_id, &db_index, &table_index);

    char query[256];
    snprintf(query, sizeof(query),
        "SELECT 1 FROM user_%d.user_info_%d WHERE id = %lld",
        db_index, table_index, last_id);

    int retries = 0;
    while (retries < MAX_RETRIES) {
        if (mysql_query(conns->read_conn, query) == 0) {
            MYSQL_RES* result = mysql_store_result(conns->read_conn);
            if (result && mysql_num_rows(result) > 0) {
                mysql_free_result(result);
                pthread_mutex_lock(&mutex);
                sync_completed = 1;
                pthread_cond_signal(&cond);
                pthread_mutex_unlock(&mutex);
                return NULL;
            }
            if (result) mysql_free_result(result);
        }   

        msleep(RETRY_DELAY_MS);
        retries++;
    }

    fprintf(stderr, "Timeout waiting for data synchronization!\n");
    pthread_mutex_lock(&mutex);
    sync_completed = -1;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return NULL;
}

// 从从库读取所有用户（自动适配新的分库分表规则）
void read_users(DBConnections* conns) {
    printf("Reading all users from slave...\n");

    int found_count = 0;
    for (long long i = 0; i < USER_COUNT; i++) {
        long long user_id = START_ID + i;
        int db_index, table_index;
        calculate_shard(user_id, &db_index, &table_index);

        char query[256];
        snprintf(query, sizeof(query),
            "SELECT id FROM user_%d.user_info_%d WHERE id = %lld",
            db_index, table_index, user_id);

        if (mysql_query(conns->read_conn, query)) {
            fprintf(stderr, "Query failed: %s\nError: %s\n",
                query, mysql_error(conns->read_conn));
            continue;
        }

        MYSQL_RES* result = mysql_store_result(conns->read_conn);
        if (result) {
            if (mysql_num_rows(result) > 0) {
                found_count++;
            }
            mysql_free_result(result);
        }
    }
    printf("Read completed. Found %d/%d users.\n", found_count, USER_COUNT);
}

// 主函数
int main() {
    // 初始化数据库连接
    DBConnections* conns = init_db_connections();
    if (!conns) {
        fprintf(stderr, "Failed to initialize database connections\n");
        return 1;
    }

    // 创建数据库结构（8库8表）
    if (!create_database_schema(conns->write_conn)) {
        fprintf(stderr, "Failed to create database schema\n");
        mysql_close(conns->write_conn);
        mysql_close(conns->read_conn);
        free(conns);
        return 1;
    }

    // 插入用户数据
    insert_users(conns);

    // 创建线程检查从库同步状态
    pthread_t sync_thread;
    if (pthread_create(&sync_thread, NULL, check_sync_status, conns) != 0) {
        fprintf(stderr, "Failed to create sync thread\n");
        mysql_close(conns->write_conn);
        mysql_close(conns->read_conn);
        free(conns);
        return 1;
    }

    // 主线程等待同步完成
    pthread_mutex_lock(&mutex);
    while (!sync_completed) {
        pthread_cond_wait(&cond, &mutex);
    }
    pthread_mutex_unlock(&mutex);

    if (sync_completed == -1) {
        fprintf(stderr, "Data synchronization failed. Aborting read operation.\n");
    }
    else {
        // 从从库读取所有用户
        read_users(conns);
    }

    // 清理资源
    pthread_join(sync_thread, NULL);
    mysql_close(conns->write_conn);
    mysql_close(conns->read_conn);
    free(conns);
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    return 0;
}