/* lltop hooks.c
 * Copyright 2010 by John L. Hammond <jhammond@ices.utexas.edu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <getopt.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "lltop.h"
#include "hooks.h"

int lltop_intvl = DEFAULT_LLTOP_INTVL;
const char *lltop_ssh_path = "/usr/bin/ssh";
const char *lltop_serv_path = "lltop-serv";
int (*lltop_get_host)(const char *addr, char *host, size_t host_size);
int (*lltop_get_job)(const char *host, char *job, size_t job_size);

static int serv_list_from_args = 0;
static int get_serv_list(const char *fs_name, char ***serv_list, int *serv_count);

static const char *get_host_path = NULL;
static int external_get_host(const char *addr, char *host, size_t host_size);

static int getnameinfo_use_fqdn = 0;
static int getnameinfo_get_host(const char *addr, char *host, size_t host_size);

static const char *get_job_path = NULL;
static int external_get_job(const char *host, char *job, size_t job_size);

static const char *execd_spool_path = "/share/sge6.2/execd_spool";
static int execd_spool_get_job(const char *host, char *job, size_t job_size);

static int print_header = 1;

static int usage(void)
{
  fprintf(stderr,
          "Usage: lltop [OPTION]... FILESYSTEM\n"
          "  or:  lltop [OPTION]... -l SERVER...\n"
          "Report load by job for Lustre FILESYSTEM or SERVER(s).\n"
          "\n"
          "Mandatory arguments to long options are mandatory for short options too.\n"
          "  -f, --fqdn               use fully qualified domain names for clients\n"
          "  -g, --get-host=COMMAND   use COMMAND for reverse DNS lookups\n"
          "  -h, --help               display this help and exit\n"
          "  -i, --interval=NUMBER    report load over NUMBER seconds\n"
          "  -j, --get-job=COMMAND    use COMMAND for job lookup\n"
          "  -l, --server-list        report load on servers given as arguments\n"
          "      --no-header          do not display header\n"
          "      --lltop-serv=PATH    use lltop-serv at PATH on servers\n"
          "      --remote-shell=PATH  use remote shell at PATH to execute lltop-serv\n"
          "      --execd-spool=PATH   use execd_spool directory PATH for job lookup\n"
          "\n"
          /* TODO Describe function, document default argument values. */
          /* TODO "Report lltop bugs to ...\n" */
          "lltop home page: <http://users.ices.utexas.edu/~jhammond/lltop/>\n");
  exit(1);
}

int lltop_config(int argc, char *argv[], char ***serv_list, int *serv_count)
{
  lltop_get_host = &getnameinfo_get_host;
  lltop_get_job = &execd_spool_get_job;

  struct option opts[] = {
    { "fqdn",        0, 0, 'f' }, /* Set getnameinfo_use_fqdn. */
    { "get-host",    1, 0, 'g' }, /* get_host_path */
    { "help",        0, 0, 'h' }, /* Call usage(). */
    { "interval",    1, 0, 'i' }, /* lltop_intvl */
    { "get-job",     1, 0, 'j' }, /* get_job_path */
    { "server-list", 0, 0, 'l' }, /* Set serv_list_from_args. */
    { "no-header",   0, &print_header, 0 }, /* Unset print_header. */
    { "lltop-serv",  1, 0, 256 }, /* lltop_serv_path */
    { "ssh",         1, 0, 257 }, /* lltop_ssh_path */
    { "execd-spool", 1, 0, 258 },
    { 0, 0, 0, 0, },
  };

  int c;
  while ((c = getopt_long(argc, argv, "fg:hi:j:l", opts, 0)) != -1) {
    switch (c) {
    case 'f':
      getnameinfo_use_fqdn = 1;
      break;
    case 'g':
      get_host_path = optarg;
      lltop_get_host = &external_get_host;
      break;
    case 'h':
      usage();
      break;
    case 'i':
      lltop_intvl = atoi(optarg);
      if (lltop_intvl <= 0)
        FATAL("invalid sleep interval \"%s\"\n", optarg);
      break;
    case 'j':
      get_job_path = optarg;
      lltop_get_job = &external_get_job;
      break;
    case 'l':
      serv_list_from_args = 1;
      break;
    case 256:
      lltop_serv_path = optarg;
      break;
    case 257:
      lltop_ssh_path = optarg;
      break;
    case 258:
      execd_spool_path = optarg;
      lltop_get_job = &execd_spool_get_job;
      break;
    case '?':
      fprintf(stderr, "Try `%s --help' for more information.\n", program_invocation_short_name);
      exit(1);
    }
  }

  if (optind >= argc)
    FATAL("missing filesystem or server list argument(s)\n"
          "Try `%s --help' for more information.\n",
          program_invocation_short_name);

  if (serv_list_from_args) {
    *serv_list = argv + optind;
    *serv_count = argc - optind;
  } else if (get_serv_list(argv[optind], serv_list, serv_count) < 0) {
    FATAL("cannot get server list for %s: %m\n", argv[optind]);
  }

  return 0;
}

static int get_serv_list(const char *fs_name, char ***serv_list, int *serv_count)
{
  /* Get the server list for filesystem named fs_name and store in
   * *serv_list, assigning its length in *serv_count.
   *
   * Order does not matter, but make sure that the list returned is
   * free of duplicates.  Also, note that these will be passed to ssh
   * as the host argument, so it's OK to return IP addresses.  If you
   * need to specify a user name <user> to ssh then you should prepend
   * '<user>@' to each server name.
   *
   * Here we just switch on fs_name to get a hard coded range of
   * servers.  For eaxmple, scratch is mds3, mds4, oss23,..., oss72.
   * Pretty gross, huh? */

  int scratch_r[] = {  3,  4, 23, 72, };
  int share_r[]   = {  1,  2,  1,  6, };
  int work_r[]    = {  5,  6,  7, 20, };

  int *fs_r;
  if (strcmp(fs_name, "scratch") == 0)
    fs_r = scratch_r;
  else if (strcmp(fs_name, "share") == 0)
    fs_r = share_r;
  else if (strcmp(fs_name, "work") == 0)
    fs_r = work_r;
  else
    FATAL("%s: unknown filesystem \"%s\"\n", __func__, fs_name);

  *serv_count = 2 - fs_r[0] + fs_r[1] - fs_r[2] + fs_r[3];
  *serv_list = alloc(*serv_count * sizeof(char*));

  int i;
  char **s = *serv_list;
  for (i = fs_r[0]; i <= fs_r[1]; i++)
    asprintf(s++, "mds%d", i);

  for (i = fs_r[2]; i <= fs_r[3]; i++)
    asprintf(s++, "oss%d", i);

  return 0;
}

void lltop_free_serv_list(char **serv_list, int serv_count)
{
  /* Clean up the server list gotten by the last function.  Utterly
   * pointless since we'll be exiting soon anyway. */
  if (serv_list_from_args)
    return;

  int i;
  for (i = 0; i < serv_count; i++)
    free(serv_list[i]);

  free(serv_list);
}

void lltop_print_header(FILE *file)
{
  /* Called once before lltop_print_name_stats(). This is your chance
   * to make a pretty header for your data.  Try to keep field widths
   * consistent between header and stats. */
  if (print_header)
    fprintf(file, "%-16s %8s %8s %8s\n", "JOBID", "WR_MB", "RD_MB", "REQS");
}

void lltop_print_name_stats(FILE *file, const char *name, long wr_B, long rd_B, long reqs)
{
  /* Called for each job to be output by lltop.  Note we convert bytes
   * to MB.  Consider adding job owner (if applicable) to output. */

  fprintf(file, "%-16s %8lu %8lu %8lu\n", name, wr_B >> 20, rd_B >> 20, reqs);
}

static int command(const char *path, const char *arg, char *buf, size_t buf_size)
{
  /* Helper to do basic command substitution. */

  int fscanf_rc = -1, pclose_rc = -1;
  char *cmd = NULL;
  FILE *pipe = NULL;

  asprintf(&cmd, "%s %s", path, arg);
  pipe = popen(cmd, "r");
  if (pipe == NULL) {
    ERROR("cannot execute %s: %m\n", cmd);
    goto out;
  }

  /* Stupid fscanf() does not have direct support for varaible maximum
     field width specifications.  So we use snprintf() to create a
     format string with the right field width encoded.  For example,
     if job_size is 1234, then fscanf_fmt should be "%1234s".
     This is stupid, should just use getline().
     XXX -1 */
  char fscanf_fmt[3 + 3 * sizeof(size_t)];
  snprintf(fscanf_fmt, sizeof(fscanf_fmt), "%%%zus", buf_size - 1);

  /* CHECKME What is fscanf_rc when output is all whitespace? */
  fscanf_rc = fscanf(pipe, fscanf_fmt, buf);
  if (fscanf_rc != 1)
    buf[0] = '\0';

  if ((pclose_rc = pclose(pipe)) < 0)
    ERROR("cannot obtain termination status of %s: %m\n", cmd);

 out:
  free(cmd);
  return fscanf_rc == 1 && pclose_rc == 0 ? 0 : -1;
}

static int external_get_host(const char *addr, char *host, size_t host_size)
{
  return command(get_host_path, addr, host, host_size);
}

static int getnameinfo_get_host(const char *addr, char *host, size_t host_size)
{
  /* Find hostname for addr (a dotted-quad string) and store in buffer
   * host of size host_size.  Return 0 if host was written, -1
   * otherwise.  Note that host_size is the size of the buffer so you
   * can safely do snprintf(host, host_size, "%s", very_long_str). */

  if (host_size < NI_MAXHOST)
    ERROR("%s: warning host_size %zu is less than NI_MAXHOST %zu\n",
          __func__, host_size, (size_t) NI_MAXHOST);

  struct sockaddr_in sin = { .sin_family = AF_INET, };
  if (inet_pton(AF_INET, addr, &sin.sin_addr) < 1) {
    ERROR("%s: invalid IPv4 address \"%s\"\n", __func__, addr);
    return -1;
  }

  int ni_rc = getnameinfo((const struct sockaddr*) &sin, sizeof(sin),
                          host, host_size, NULL, 0, NI_NAMEREQD);
  if (ni_rc != 0) {
    if (ni_rc != EAI_NONAME)
      ERROR("%s: cannot get name info for address \"%s\": %s\n", __func__,
            addr, gai_strerror(ni_rc));
    return -1;
  }

  /* Setting NI_NOFQDN for the same effect doesn't seem to work here.
   * Maybe a problem with site config.  We shouldn't have to worry
   * about truncating numerical addresses since we set NI_NAMEREQD. */
  if (!getnameinfo_use_fqdn)
    chop(host, '.');

  return 0;
}

static int external_get_job(const char *host, char *job, size_t job_size)
{
  return command(get_job_path, host, job, job_size);
}

static int execd_spool_get_job(const char *host, char *job, size_t job_size)
{
  /* Find jobname for host and store in buffer job of size job_size.
   * Return 0 if job was written, -1 otherwise.  Note that job_size is
   * the size of the buffer so you can safely do snprintf(job,
   * job_size, "%s", very_long_str).
   *
   * This works for the current TACC Ranger SGE setup.  A running job
   * with jobid <job_id> on host <host> is associated with a directory
   * /share/sge6.2/execd_spool/<host>/active_jobs/<job_id>.<array_task>. */

  int rc = -1;
  char *jobs_dir_path = NULL;
  asprintf(&jobs_dir_path, "%s/%s/active_jobs", execd_spool_path, host);

  DIR *jobs_dir = opendir(jobs_dir_path);
  if (jobs_dir == NULL) {
    /* Cannot find an active_jobs directory for host.  This need not
       be an error. */
    if (errno != ENOENT)
      ERROR("%s: cannot open %s: %m\n", __func__, jobs_dir_path);

    static int access_checked;
    if (!access_checked && access(execd_spool_path, R_OK|X_OK) < 0)
      ERROR("%s: cannot access %s: %m\n", __func__, execd_spool_path);
    access_checked = 1;
    goto out;
  }

  struct dirent *ent;
  while ((ent = readdir(jobs_dir))) {
    if (ent->d_type == DT_DIR && ent->d_name[0] != '.') {
      /* Chop off '.<array_task> suffix.  OK to modify d_name, right? */
      snprintf(job, job_size, "%s", chop(ent->d_name, '.'));
      rc = 0;
      break;
    }
  }

 out:
  if (jobs_dir != NULL)
    closedir(jobs_dir);
  free(jobs_dir_path);

  return rc;
}
