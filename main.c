#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#include "mysql/mysql.h"
#include "mysql/errmsg.h"

#define NUM_THREADS 20

#define MYSQL_TIMEOUT 10

#define LOG_ERROR 0
#define LOG_WARNING 1

#define ast_mutex_t pthread_mutex_t

#define ast_mutex_init(a) __pthread_mutex_init(a);
#define ast_mutex_lock(a) __pthread_mutex_lock(a);
#define ast_mutex_unlock(a) __pthread_mutex_unlock(a);

#define ast_malloc(size) (malloc(size))
#define ast_str_buffer(s) (s)

static char* hostname = "";
static char* dbuser = "";
static char* password = "";
static char* dbname = "";
static int dbport = 3306;

void __pthread_mutex_init(ast_mutex_t* recursive_mutex);
void __pthread_mutex_lock(ast_mutex_t* recursive_mutex);
void __pthread_mutex_unlock(ast_mutex_t* recursive_mutex);
static void ast_log(int level, const char* format, ...);
static void ast_copy_string(char* dst, const char* src, size_t size);

typedef struct {
  MYSQL* sql;
  ast_mutex_t lock;
  int id;
}mysql_conn_t;

typedef struct {
  mysql_conn_t** conn;
  ast_mutex_t lock;
  int index;
  int size;
  int port;
  char host[128];
  char user[64];
  char password[64];
  char database[64];
} mysql_pool_t;

static mysql_pool_t* pool;

static int create_pool(int size, const char* host, const char* user, const char* password, const char* database, int port)
{
  if (!(pool = ast_malloc(sizeof(mysql_pool_t)))) {
    ast_log(LOG_ERROR, "Memory error\n");
    return -1;
  }

  ast_mutex_init(&pool->lock);
  pool->index = 0;
  pool->size = size;
  pool->port = port;

  ast_copy_string(pool->host, host, sizeof(pool->host));
  ast_copy_string(pool->user, user, sizeof(pool->user));
  ast_copy_string(pool->password, password, sizeof(pool->password));
  ast_copy_string(pool->database, database, sizeof(pool->database));

  if (!(pool->conn = ast_malloc(sizeof(mysql_conn_t*) * (size_t)pool->size))) {
    ast_log(LOG_ERROR, "Memory error\n");
    return -1;
  }

  for (int i = 0; i < pool->size; i++) {
    if (!(pool->conn[i] = ast_malloc(sizeof(mysql_conn_t)))) {
      ast_log(LOG_ERROR, "Memory error\n");
      return -1;
    }
    pool->conn[i]->id = i;
    pool->conn[i]->sql = NULL;
    ast_mutex_init(&pool->conn[i]->lock);
  }

  return 0;
}

static void ast_exec_mysql(const char* query_str, size_t query_len, const char* csv)
{
  mysql_conn_t* conn = NULL;
  MYSQL_RES* res = NULL;
  char filename[128];
  FILE* fp = NULL;

  ast_mutex_lock(&pool->lock);
  if ((fp = fopen("/var/log/file.csv", "at")) == NULL) {
    ast_log(LOG_ERROR, "Failed to open file: %s\n", filename);
  }
  else {
    fprintf(fp, "%s\n", csv);
    fclose(fp);
  }
  if (++pool->index >= pool->size) {
    pool->index = 0;
  }
  conn = pool->conn[pool->index];
  ast_mutex_unlock(&pool->lock);

  ast_mutex_lock(&conn->lock);
  if (conn->sql == NULL) {
    if ((conn->sql = mysql_init(NULL)) == NULL) {
      ast_log(LOG_ERROR, "[%d] mysql_init() failed\n", conn->id);
      ast_mutex_unlock(&conn->lock);
      return;
    }
    if (!mysql_real_connect(conn->sql, pool->host, pool->user, pool->password, pool->database, (unsigned int)pool->port, NULL, 0)) {
      ast_log(LOG_ERROR, "[%d] mysql_real_connect() failed: %s\n", conn->id, mysql_error(conn->sql));
      mysql_close(conn->sql);
      conn->sql = NULL;
      ast_mutex_unlock(&conn->lock);
      return;
    }
  }

  if (mysql_ping(conn->sql) != 0) {
    ast_log(LOG_ERROR, "[%d] mysql_ping() failed: %s\n", conn->id, mysql_error(conn->sql));
    mysql_close(conn->sql);
    conn->sql = NULL;
    ast_mutex_unlock(&conn->lock);
    return;
  }
  
  if (mysql_real_query(conn->sql, query_str, query_len)) {
    ast_log(LOG_ERROR, "[%d] mysql_real_query() failed: %s\n", conn->id, mysql_error(conn->sql));
    mysql_close(conn->sql);
    conn->sql = NULL;
    ast_mutex_unlock(&conn->lock);
    return;
  }

  ast_log(LOG_ERROR, "[%d] mysql_real_query() success\n", conn->id);

  if ((res = mysql_store_result(conn->sql))) {
    mysql_free_result(res);
  }
  ast_mutex_unlock(&conn->lock);
}

void* thread_function(void* arg)
{
  char csv[128];
  char query[128];
  
  int thread_id = *(int*)arg;

  strcpy(csv, "\"2024-05-25 02:37:09\", \"""\", \"2000\"");
  strcpy(query, "SELECT * from mytable;");

  for (;;) {
    printf("Hello from thread %d\n", thread_id);
    ast_exec_mysql(query, strlen(query), csv);
    sleep(1);
  }
  return NULL;
}

int main(int arg, char* argv[])
{
  if (create_pool(4, ast_str_buffer(hostname), ast_str_buffer(dbuser), ast_str_buffer(password), ast_str_buffer(dbname), dbport)) {
    return -1;
  }

  pthread_t threads[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    int* thread_id = malloc(sizeof(int));
    if (thread_id == NULL) {
      perror("malloc");
      return EXIT_FAILURE;
    }
    *thread_id = i;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    if (pthread_create(&threads[i], &attr, thread_function, thread_id) != 0) {
      perror("pthread_create");
      free(thread_id);
      return EXIT_FAILURE;
    }
    usleep(500000);
    pthread_attr_destroy(&attr);
  }
  
  printf("Press any key to exit...\n");
  getchar();

  return 0;
}

static void ast_log(int level, const char* format, ...)
{
  char buffer[1024];

  va_list args;
  va_start(args, format);

  vsnprintf(buffer, sizeof(buffer), format, args);

  va_end(args);

  time_t now = time(NULL);
  struct tm* t = localtime(&now);

  char time_str[20];
  strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", t);

  printf("[%s] %s\n", time_str, buffer);
}

void ast_copy_string(char* dst, const char* src, size_t size)
{
  strcpy(dst, src);
}

void __pthread_mutex_lock(ast_mutex_t* recursive_mutex)
{
  pthread_mutex_lock(recursive_mutex);
}

void __pthread_mutex_unlock(ast_mutex_t* recursive_mutex)
{
  pthread_mutex_unlock(recursive_mutex);
}

void __pthread_mutex_init(ast_mutex_t* recursive_mutex)
{
  pthread_mutexattr_t attr;

  if (pthread_mutexattr_init(&attr) != 0) {
    perror("pthread_mutexattr_init");
    exit(EXIT_FAILURE);
  }

  if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) != 0) {
    perror("pthread_mutexattr_settype");
    pthread_mutexattr_destroy(&attr);
    exit(EXIT_FAILURE);
  }

  if (pthread_mutex_init(recursive_mutex, &attr) != 0) {
    perror("pthread_mutex_init");
    pthread_mutexattr_destroy(&attr);
    exit(EXIT_FAILURE);
  }

  pthread_mutexattr_destroy(&attr);
}
