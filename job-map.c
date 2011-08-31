#define _GNU_SOURCE
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <getopt.h>

#define ERROR(fmt,arg...) \
  fprintf(stderr, "%s: "fmt, program_invocation_short_name, ##arg)

#define FATAL(fmt,arg...) do { \
  ERROR(fmt,##arg); \
  exit(1); \
} while (0)

#define JOB_NONE "0"

struct linux_dirent64 {
  uint64_t       d_ino;
  int64_t        d_off;
  unsigned short d_reclen;
  unsigned char  d_type;
  char           d_name[0];
};

int main(int argc, char *argv[])
{
  const char *execd_spool_path = "/share/sge6.2/execd_spool";
  /* const char *out_path = NULL; */
  int sleep_interval = 0;
  /* int rename = 0; */
  FILE *out_file = stdout;

  if (chdir(execd_spool_path) < 0)
    FATAL("cannot change to `%s': %m\n", execd_spool_path);

  DIR *execd_spool_dir = opendir(".");
  if (execd_spool_dir == NULL)
    FATAL("cannot open `%s': %m\n", execd_spool_path);

  while (1) {
    struct dirent *host_de;
    while ((host_de = readdir(execd_spool_dir)) != NULL) {
      if (host_de->d_name[0] == '.')
        continue;

      const char *host = host_de->d_name;
      char active_jobs_path[HOST_NAME_MAX + 1 + 20];
      snprintf(active_jobs_path, sizeof(active_jobs_path), "%s/active_jobs", host);

      int active_jobs_fd = open(active_jobs_path, O_RDONLY|O_DIRECTORY);
      if (active_jobs_fd < 0) {
        if (errno != ENOENT)
          ERROR("cannot open `%s/%s': %m\n", execd_spool_path, active_jobs_path);
        continue;
      }

      char de_buf[1024];
      int de_n = syscall(SYS_getdents64, active_jobs_fd, de_buf, sizeof(de_buf));
      if (de_n < 0) {
        if (errno != ENOENT)
          ERROR("cannot read `%s/%s': %m\n", execd_spool_path, active_jobs_path);
        goto next;
      }

      const char *job_name = JOB_NONE;
      char *de_pos = de_buf, *de_end = de_buf + de_n;
      while (de_pos < de_end) {
        struct linux_dirent64 *job_de = (struct linux_dirent64 *) de_pos;
        if (job_de->d_name[0] != '.') {
          job_name = job_de->d_name;
          break;
        }
        de_pos += job_de->d_reclen;
      }

      fprintf(out_file, "%s %s\n", host, job_name);
    next:
      close(active_jobs_fd);
    }

    fflush(out_file);

    if (sleep_interval <= 0)
      break;

    rewinddir(execd_spool_dir);
  }

  closedir(execd_spool_dir);

  return 0;
}
