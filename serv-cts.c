/* lltop serv-cts.c
 * Copyright 2010 by John L. Hammond <jhammond@tacc.utexas.edu>
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
/* TODO Error messages should include hostname. */
#define _GNU_SOURCE
#include <dirent.h>
#include <getopt.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdarg.h>
#include <errno.h>
#include <netdb.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "string1.h"
#include "lltop.h"
#include "dict.h"

#define LLTOP_MSG_MAX 64000 /* UDP max minus stuff minus some other stuff. */
#define LLTOP_PORT "9907"
#define NR_CLIENTS_HINT 4096 /* Initial dict size. */

struct dict name_stats_dict;

#define NS_WR 0 /* MOVEME. */
#define NS_RD 1
#define NS_REQS 2

struct msg_buf {
  char *mb_buf;
  size_t mb_len, mb_size;
  int mb_fd;
};

int msg_buf_init(struct msg_buf *mb, int fd, char *buf, size_t size)
{
  mb->mb_fd = fd;
  mb->mb_len = 0;
  mb->mb_size = size;
  mb->mb_buf = buf;
  return 0;
}

int msg_buf_send(struct msg_buf *mb, const char *name, long wr, long rd, long reqs)
{
  size_t avail, need;

 again:
  avail = mb->mb_size - mb->mb_len;
  need = snprintf(mb->mb_buf + mb->mb_len, avail, "%s %ld %ld %ld\n", name, wr, rd, reqs);

  if (need >= avail) {
    if (mb->mb_len == 0) {
      errno = ENAMETOOLONG;
      return -1;
    }

    if (send(mb->mb_fd, mb->mb_buf, mb->mb_len, 0) < 0)
      return -1;

    mb->mb_len = 0;
    goto again;
  }

  mb->mb_len += need;

  return 0;
}

int msg_buf_flush(struct msg_buf *mb)
{
  if (mb->mb_len > 0) {
    if (send(mb->mb_fd, mb->mb_buf, mb->mb_len, 0) < 0)
      return -1;

    mb->mb_len = 0;
  }

  return 0;
}

struct name_stats {
  long ns_stats[2][3];
  unsigned int ns_gen;
  char ns_name[];
};

static inline struct name_stats *key_ns(char *key)
{
  /* As an exercise to the reader, figure out why offsetof() doesn't
     work here.  Or does it? */

  size_t ns_name_offset = ((struct name_stats *) NULL)->ns_name - (char *) NULL;
  return (struct name_stats *) (key - ns_name_offset);
}

struct lustre_target {
  char *name;
  char *export_dir_path;
};

size_t nr_targets = 0;
struct lustre_target *target_list = NULL;

int de_is_subdir(const struct dirent *de)
{
  return de->d_type == DT_DIR && de->d_name[0] != '.';
}

int get_target_list(struct lustre_target **list, size_t *nr, const char *dir_path)
{
  struct dirent **de = NULL;
  int i, j, new_nr, nr_de = 0, rc = -1;
  struct lustre_target *new_list = NULL;

  nr_de = scandir(dir_path, &de, &de_is_subdir, &alphasort);
  if (nr_de < 0) {
    ERROR("cannot scan `%s': %m\n", dir_path);
    goto out;
  }

  new_nr = *nr + nr_de;
  new_list = realloc(*list, new_nr * sizeof(*list[0]));
  if (new_list == NULL) {
    ERROR("cannot allocate target list: %m\n");
    goto out;
  }
  *list = new_list;

  for (i = *nr, j = 0; i < new_nr; i++, j++) {
    (*list)[i].name = strdup(de[j]->d_name);
    /* XXX */
    (*list)[i].export_dir_path = strf("%s/%s/exports", dir_path, de[j]->d_name);
    /* XXX */
  }

  *nr = new_nr;
  rc = 0;

 out:
  for (j = 0; j < nr_de; j++)
    free(de[j]);
  free(de);

  return rc;
}

int read_client_stats(const char *cli_name, unsigned int gen)
{
  char stats_path[80];
  FILE *stats_file = NULL;
  char stats_file_buf[BUFSIZ];
  char *line = NULL;
  size_t line_size = 0;
  long wr = 0, rd = 0, reqs = 0;

  TRACE("cli_name %s, gen %d\n", cli_name, gen);

  snprintf(stats_path, sizeof(stats_path), "%s/stats", cli_name);

  stats_file = fopen(stats_path, "r");
  if (stats_file == NULL) {
    ERROR("cannot open %s: %m\n", stats_path);
    goto out;
  }
  setvbuf(stats_file, stats_file_buf, _IOFBF, sizeof(stats_file_buf));

  /* Skip the helpful snapshot_time. */
  getline(&line, &line_size, stats_file);

  while (getline(&line, &line_size, stats_file) >= 0) {
    char ctr_name[80];
    long ctr_samples, ctr_sum = 0;

    /* XXX Do we need to check ctr_units? */
    if (sscanf(line, "%79s %ld samples [%*[^]]] %*d %*d %ld",
               ctr_name, &ctr_samples, &ctr_sum) < 2) {
      ERROR("invalid line \"%s\"\n", chop(line, '\n'));
      continue;
    }

    if (strcmp(ctr_name, "write_bytes") == 0) {
      wr = ctr_sum;
    } else if (strcmp(ctr_name, "read_bytes") == 0) {
      rd = ctr_sum;
    } else if (strcmp(ctr_name, "ping") != 0) { /* Ignore pings. */
      reqs += ctr_samples;
    }
  }

  /* Look up cli_name. */
  struct name_stats *ns = NULL;
  hash_t hash = dict_strhash(cli_name);
  struct dict_entry *de = dict_entry_ref(&name_stats_dict, hash, cli_name);
  long *s = NULL;

  if (de->d_key != NULL) {
    ns = key_ns(de->d_key);
    goto have_ns;
  }

  ns = alloc(sizeof(*ns) + strlen(cli_name) + 1);
  memset(ns, 0, sizeof(*ns));
  ns->ns_gen = gen;
  strcpy(ns->ns_name, cli_name);

  if (dict_entry_set(&name_stats_dict, de, hash, ns->ns_name) < 0)
    FATAL("dict_entry_set: %m\n");

 have_ns:
  s = ns->ns_stats[gen % 2];
  if (ns->ns_gen != gen) {
    memset(s, 0, 3 * sizeof(long));
    ns->ns_gen = gen;
  }

  s[NS_WR] += wr;
  s[NS_RD] += rd;
  s[NS_REQS] += reqs;

 out:
  free(line);
  if (stats_file != NULL)
    fclose(stats_file);

  return 0;
}

int read_target_stats(struct lustre_target *target, unsigned int gen)
{
  DIR *exp_dir = NULL;
  TRACE("target %s, gen %d\n", target->name, gen);

  if (chdir(target->export_dir_path) < 0) {
    ERROR("cannot chdir to `%s': %m\n", target->export_dir_path);
    /* TODO Invalidate target or something. */
    goto out;
  }

  exp_dir = opendir(".");
  if (exp_dir == NULL) {
    ERROR("cannot open `%s': %m\n", target->export_dir_path);
    goto out;
  }

  struct dirent *de;
  while ((de = readdir(exp_dir)) != NULL) {
    if (de_is_subdir(de))
      read_client_stats(de->d_name, gen);
  }

 out:
  if (exp_dir != NULL)
    closedir(exp_dir);

  return 0;
}

int main(int argc, char *argv[])
{
  int daemonize = 0;
  int send_all = 0;
  int intvl = DEFAULT_LLTOP_INTVL;
  char *host_arg = NULL, *port_arg = LLTOP_PORT;
  int sfd = -1;
  struct msg_buf mb;
  char mb_buf[LLTOP_MSG_MAX];

  struct option opts[] = {
    { "send-all", 0, NULL, 'a' },
    { "daemon", 0, NULL, 'd' },
    { "interval", 1, NULL, 'i' },
    { "port", 1, NULL, 'p' },
    { NULL, 0, NULL, 0 },
  };

  int c;
  while ((c = getopt_long(argc, argv, "adi:p:", opts, 0)) > 0) {
    switch (c) {
    case 'a':
      send_all = 1;
      continue;
    case 'd':
      daemonize = 1;
      continue;
    case 'i':
      intvl = atoi(optarg);
      if (intvl <= 0)
        FATAL("invalid sleep interval `%s'\n", optarg);
      continue;
    case 'p':
      port_arg = optarg;
      continue;
    case '?':
      FATAL("invalid option\n");
    }
  }

  if (argc - optind <= 0) {
    fprintf(stderr, "Usage: %s [OPTIONS] HOST\n", program_invocation_short_name);
    exit(1);
  }
  host_arg = argv[optind];

  struct addrinfo hints, *list, *info;
  hints = (struct addrinfo) {
    .ai_family = AF_INET, /* XXX */
    .ai_socktype = SOCK_DGRAM,
  };

  int gai_rc = getaddrinfo(host_arg, port_arg, &hints, &list);
  if (gai_rc != 0)
    FATAL("cannot resolve host `%s', service `%s': %s\n",
	  host_arg, port_arg, gai_strerror(gai_rc));

  for (info = list; info != 0; info = info->ai_next) {
    if ((sfd = socket(info->ai_family, info->ai_socktype, info->ai_protocol)) < 0)
      continue;
    if (connect(sfd, info->ai_addr, info->ai_addrlen) == 0)
      break;
    close(sfd);
    sfd = -1;
  }

  freeaddrinfo(list);

  if (sfd < 0)
    FATAL("cannot connect to host `%s', service `%s': %m\n", host_arg, port_arg);

  if (msg_buf_init(&mb, sfd, mb_buf, sizeof(mb_buf)) < 0)
    FATAL("cannot create message buffer: %m\n");

  if (get_target_list(&target_list, &nr_targets, "/proc/fs/lustre/mdt") < 0)
    exit(1);

  if (get_target_list(&target_list, &nr_targets, "/proc/fs/lustre/obdfilter") < 0)
    exit(1);

  if (nr_targets == 0)
    FATAL("no targets found\n");

  if (dict_init(&name_stats_dict, NR_CLIENTS_HINT) < 0)
    FATAL("cannot create client dictionary: %m\n");

  struct timespec intvl_spec;
  if (clock_gettime(CLOCK_MONOTONIC, &intvl_spec) < 0)
    FATAL("cannot get current time: %m\n");

  if (daemonize && daemon(0, 0) < 0)
    FATAL("cannot daemonize: %m\n");

  unsigned int gen;
  for (gen = 0; ; gen++) {
    int i;
    for (i = 0; i < nr_targets; i++)
      read_target_stats(&target_list[i], gen);

    if (daemonize)
      chdir("/");

    if (gen == 0)
      goto sleep;

    size_t de_iter = 0;
    struct dict_entry *de;
    while ((de = dict_for_each_ref(&name_stats_dict, &de_iter)) != NULL) {
      struct name_stats *ns = key_ns(de->d_key);
      long *s0, *s1, wr, rd, reqs;

      if (ns->ns_gen != gen) {
        TRACE("stale stats found for client `%s', removing\n", ns->ns_name);
        dict_entry_remv(&name_stats_dict, de, 0);
        free(ns);
        continue;
      }

      s0 = ns->ns_stats[(gen - 1) % 2];
      s1 = ns->ns_stats[gen % 2];
      wr = s1[NS_WR] - s0[NS_WR];
      rd = s1[NS_RD] - s0[NS_RD];
      reqs = s1[NS_REQS] - s0[NS_REQS];

      /* If any stats are negative then we assume that the client was
         evicted while we slept, so we skip it. */
      if (!send_all && (wr < 0 || rd < 0 || reqs < 0)) {
        TRACE("skipping %s %ld %ld %ld\n", ns->ns_name, wr, rd, reqs);
        continue;
      }

      /* Skip client if all stats are zero. */
      if (!send_all && wr == 0 && rd == 0 && reqs == 0) {
        TRACE("skipping %s %ld %ld %ld\n", ns->ns_name, wr, rd, reqs);
        continue;
      }

      if (msg_buf_send(&mb, ns->ns_name, wr, rd, reqs) < 0) {
	if (errno == ENAMETOOLONG)
	  ERROR("skipping client `%s': name too long\n", ns->ns_name);
	else
	  FATAL("cannot send to host `%s', service `%s': %m\n", host_arg, port_arg);
      }
    }

    if (msg_buf_flush(&mb) < 0)
      FATAL("cannot send to host `%s', service `%s': %m\n", host_arg, port_arg);

  sleep:
    intvl_spec.tv_sec += intvl;
    errno = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &intvl_spec, NULL);
    if (errno != 0)
      FATAL("cannot sleep: %m\n");
  }
}
