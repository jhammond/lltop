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
#include "lltop.h"
#include "dict.h"

#define NR_CLIENTS_HINT 4096
#define TIME_BUF_SIZE 80
char *time_fmt = "%T"; /* Must strftime() to less than TIME_BUF_SIZE
                          characters. */
int print_all = 1;

struct dict name_stats_dict;

#define NS_WR 0
#define NS_RD 1
#define NS_REQS 2

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

static inline char *strf(const char *fmt, ...)
{
  char *str = NULL;
  va_list args;

  va_start(args, fmt);
  if (vasprintf(&str, fmt, args) < 0)
    str = NULL;
  va_end(args);
  return str;
}

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
  int intvl = DEFAULT_LLTOP_INTVL;

  struct option opts[] = {
    { "interval", 1, NULL, 'i' },
    { "time-format", 1, NULL, 't' },
    { NULL, 0, NULL, 0 },
  };

  int c;
  while ((c = getopt_long(argc, argv, "i:t:", opts, 0)) > 0) {
    switch (c) {
    case 'i':
      intvl = atoi(optarg);
      if (intvl <= 0)
        FATAL("invalid sleep interval `%s'\n", optarg);
      continue;
    case 't':
      time_fmt = optarg;
      continue;
    case '?':
      FATAL("invalid option\n");
    }
  }

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

  unsigned int gen;
  for (gen = 0; ; gen++) {
    char time_buf[TIME_BUF_SIZE];
    time_t now = time(NULL);
    struct tm tm_now = *localtime(&now);
    strftime(time_buf, sizeof(time_buf), time_fmt, &tm_now);

    int i;
    for (i = 0; i < nr_targets; i++)
      read_target_stats(&target_list[i], gen);

    chdir("/");

    if (gen == 0)
      goto sleep;

    size_t de_iter = 0;
    struct dict_entry *de;
    while ((de = dict_for_each_ref(&name_stats_dict, &de_iter)) != NULL) {
      struct name_stats *ns = key_ns(de->d_key);
      long *s0, *s1, wr, rd, reqs;

      if (ns->ns_gen != gen) {
        TRACE("stale stats found for client `%s'\n", ns->ns_name);
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
      if (!print_all && (wr < 0 || rd < 0 || reqs < 0)) {
        TRACE("skipping %s %ld %ld %ld\n", ns->ns_name, wr, rd, reqs);
        continue;
      }

      /* Skip client if all stats are zero. */
      if (!print_all && wr == 0 && rd == 0 && reqs == 0) {
        TRACE("skipping %s %ld %ld %ld\n", ns->ns_name, wr, rd, reqs);
        continue;
      }

      printf("%s %s %ld %ld %ld\n", time_buf, ns->ns_name, wr, rd, reqs);
    }

  sleep:
    intvl_spec.tv_sec += intvl;
    errno = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &intvl_spec, NULL);
    if (errno != 0)
      FATAL("cannot sleep: %m\n");
  }
}
