#ifndef _HOOKS_H_
#define _HOOKS_H_
#include <stdio.h>

extern int lltop_intvl;
extern const char *lltop_ssh_path;
extern const char *lltop_serv_path;
extern int (*lltop_get_host)(const char *addr, char *host, size_t host_size);
extern int (*lltop_get_job)(const char *host, char *job, size_t job_size);

int lltop_config(int argc, char *argv[], char ***serv_list, int *serv_count);
void lltop_free_serv_list(char **serv_list, int serv_count);
void lltop_print_header(FILE *file);
void lltop_print_name_stats(FILE *file, const char *name, long wr_B, long rd_B, long reqs);

#endif
