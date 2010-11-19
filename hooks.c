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

const char *sge_execd_spool_path = "/share/sge6.2/execd_spool";
const char *lltop_ssh_path = "/usr/bin/ssh";
const char *lltop_serv_path = "/usr/local/bin/lltop-serv";

int lltop_use_fqdn = 0;

int lltop_get_serv_list(const char *fs_name, char ***serv_list, int *serv_count)
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

  int i;
  for (i = 0; i < serv_count; i++)
    free(serv_list[i]);

  free(serv_list);
}

int lltop_get_host(const char *addr, char *host, size_t host_size)
{
  /* Find hostname for addr (a dotted-quad string) and store in buffer
   * host of size host_size.  Return 0 if host was written, -1
   * otherwise.  Note thet host_size is the size of the buffer so you
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
  if (!lltop_use_fqdn)
    chop(host, '.');

  return 0;
}

int lltop_get_job(const char *host, char *job, size_t job_size)
{
  /* Find jobname for host and store in buffer job of size job_size.
   * Return 0 if job was written, -1 otherwise.  Note thet job_size
   * is the size of the buffer so you can safely do snprintf(job,
   * job, "%s", very_long_str).
   *
   * This works for the current TACC Ranger SGE setup.  A running job
   * with jobid <job_id> on host <host> is associated with a directory
   * /share/sge6.2/execd_spool/<host>/active_jobs/<job_id>.<array_task>. */

  char jobs_dir_path[160];
  snprintf(jobs_dir_path, sizeof(jobs_dir_path), "%s/%s/active_jobs",
           sge_execd_spool_path, host);

  DIR *jobs_dir = opendir(jobs_dir_path);
  if (jobs_dir == NULL) {
    /* Cannot find an active_jobs directory for host.  This need not
       be an error. */
    if (errno != ENOENT)
      FATAL("%s: cannot open %s: %m\n", __func__, jobs_dir_path);

    static int sge_access_checked;
    if (!sge_access_checked && access(sge_execd_spool_path, R_OK|X_OK) < 0)
      ERROR("%s: cannot open %s: %m\n", __func__, sge_execd_spool_path);
    sge_access_checked = 1;
    return -1;
  }

  int rc = -1;
  struct dirent *ent;
  while ((ent = readdir(jobs_dir))) {
    if (ent->d_type == DT_DIR && ent->d_name[0] != '.') {
      /* Chop off '.<array_task> suffix.  OK to modify d_name, right? */
      snprintf(job, job_size, "%s", chop(ent->d_name, '.'));
      rc = 0;
      break;
    }
  }
  closedir(jobs_dir);

  return rc;
}

void lltop_print_header(FILE *file)
{
  /* Called once before lltop_print_name_stats(), your chance to make
   * a pretty header for your data.  Try to keep field widths
   * consistent between header and stats. */

  fprintf(file, "%-16s %8s %8s %8s\n", "JOBID", "WR_MB", "RD_MB", "REQS");
}

void lltop_print_name_stats(FILE *file, const char *name, long wr_B, long rd_B, long reqs)
{
  /* Called for each job to be output by lltop.  Note we convert bytes
   * to MB.  Consider adding job owner (if applicable) to output. */

  fprintf(file, "%-16s %8lu %8lu %8lu\n", name, wr_B >> 20, rd_B >> 20, reqs);
}
