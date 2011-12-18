#include <mysql.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>

#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define BUF_SIZE 8192

pthread_mutex_t file_servers_mutex= PTHREAD_MUTEX_INITIALIZER;

// file_put
my_bool file_put_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
char *file_put(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error);

// file_delete
my_bool file_delete_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
char *file_delete(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error);


my_bool file_put_init(UDF_INIT *initid, UDF_ARGS *args, char *message){
  if( args->arg_count!=2 && args->arg_count!=3 ){
    strncpy(message, "file_put('/path/to/file','file_contents'[,0755])", MYSQL_ERRMSG_SIZE);
    return 1;
  }
  args->arg_type[0] = STRING_RESULT;
  args->arg_type[1] = STRING_RESULT;
  args->arg_type[2] = INT_RESULT;
  initid->ptr = NULL;

  return 0;
}

char *file_put(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error){

  pthread_mutex_lock(&file_servers_mutex);

  unsigned int perm=0,perm_arg=0;
  if( args->arg_count==3 ){
    perm_arg = *((long*)args->args[2]);
    char buf[12];
    if( args->arg_type[2] == INT_RESULT && args->lengths[2] == 4 ){
      sprintf(buf,"0%d",perm_arg);
      perm_arg = strtol(buf,NULL,8);
    }

    // user
    if( perm_arg & 0400 ){ perm |= S_IRUSR; }
    if( perm_arg & 0200 ){ perm |= S_IWUSR; }
    if( perm_arg & 0100 ){ perm |= S_IXUSR; }
    // group
    if( perm_arg & 0040 ){ perm |= S_IRGRP; }
    if( perm_arg & 0020 ){ perm |= S_IWGRP; }
    if( perm_arg & 0010 ){ perm |= S_IXGRP; }
    // other
    if( perm_arg & 0004 ){ perm |= S_IROTH; }
    if( perm_arg & 0002 ){ perm |= S_IWOTH; }
    if( perm_arg & 0001 ){ perm |= S_IXOTH; }

  }else{
    perm = 00666;
  }

  int fd;
  long written_bytes = 0;
  fd = open(args->args[0],O_RDWR|O_CREAT|O_TRUNC,perm);
  written_bytes = write(fd,args->args[1],args->lengths[1]);
  close(fd);

  chmod(args->args[0],perm);

  sprintf(result,"%ld",written_bytes);
  *length = strlen(result);
  pthread_mutex_unlock(&file_servers_mutex);

  return result;
}

my_bool file_delete_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if( args->arg_count!=1 ){
    strncpy(message, "file_delete('/path/to/file')", MYSQL_ERRMSG_SIZE);
    return 1;
  }
  args->arg_type[0] = STRING_RESULT;
  initid->ptr = NULL;

  return 0;
}

char *file_delete(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error)
{

  pthread_mutex_lock(&file_servers_mutex);

  int is_deleted = unlink(args->args[0]);
  sprintf(result,"%d",is_deleted);
  *length = strlen(result);
  pthread_mutex_unlock(&file_servers_mutex);

  return result;
}

