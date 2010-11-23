/* lltop serv.c
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
#include "lltop.h"
#include "rbtree.h"

#define MDS_FILTER_PATH "/proc/fs/lustre/mds"
#define OSS_FILTER_PATH "/proc/fs/lustre/obdfilter"

struct name_stats {
  struct rb_node ns_node;
  long ns_wr, ns_rd, ns_reqs;
  char ns_name[];
};

struct rb_root name_stats_root = RB_ROOT;

int get_client_stats(const char *cli_name, const char *stats_path, int which)
{
  /* Parameter which is 0 or 1 depending on which pass we are in.  If
   * which is 0 then subtract wr/rd/reqs from stats, otherwise add. */
  TRACE("cli_name %s, stats_path %s, which %d\n", cli_name, stats_path, which);

  FILE* stats_file = fopen(stats_path, "r");
  if (stats_file == NULL) {
    ERROR("cannot open %s: %m\n", stats_path);
    return -1;
  }

  long wr = 0, rd = 0, reqs = 0;
  char *line = NULL;
  size_t line_size = 0;

  /* Skip first line with its busted snapshot_time. */
  getline(&line, &line_size, stats_file);

  while (getline(&line, &line_size, stats_file) >= 0) {
    char ctr_name[80];
    long ctr_samples, ctr_sum = 0;

    /* XXX Do we need to check ctr_units? */
    /* XXX Field widths. */
    if (sscanf(line, "%s %ld samples [%*[^]]] %*d %*d %ld",
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
  free(line);
  fclose(stats_file);

  /* Look up name_stats for cli_name. */
  struct name_stats *stats = NULL;
  struct rb_node **link, *parent;

  link = &(name_stats_root.rb_node);
  parent = NULL;

  while (*link != NULL) {
    stats = rb_entry(*link, struct name_stats, ns_node);
    parent = *link;

    int cmp = strcmp(cli_name, stats->ns_name);
    if (cmp < 0) {
      link = &((*link)->rb_left);
    } else if (cmp > 0) {
      link = &((*link)->rb_right);
    } else {
      goto have_stats;
    }
  }

  /* Create name_stats, link, and initialize. */
  stats = alloc(sizeof(*stats) + strlen(cli_name) + 1);
  memset(stats, 0, sizeof(*stats));
  rb_link_node(&stats->ns_node, parent, link);
  rb_insert_color(&stats->ns_node, &name_stats_root);
  strcpy(stats->ns_name, cli_name);

 have_stats:
  stats->ns_wr += which ? wr : -wr;
  stats->ns_rd += which ? rd : -rd;
  stats->ns_reqs += which ? reqs : -reqs;
  return 0;
}

int get_target_stats(const char *tgt_dir_path, int which)
{
  /* tgt_dir_path is /proc/fs/lustre/{mds,obdfilter}/<tgt_name> */
  TRACE("tgt_dir_path %s, which %d\n", tgt_dir_path, which);

  char exp_dir_path[4096], stats_path[4096];
  snprintf(exp_dir_path, 4096, "%s/exports", tgt_dir_path);

  DIR *dir = opendir(exp_dir_path);
  if (dir == NULL) {
    ERROR("cannot open %s: %m\n", exp_dir_path);
    return -1;
  }

  struct dirent *ent;
  while ((ent = readdir(dir))) {
    if (ent->d_type == DT_DIR && ent->d_name[0] != '.') {
      char *cli_name = ent->d_name;
      snprintf(stats_path, 4096, "%s/%s/stats", exp_dir_path, cli_name);
      get_client_stats(cli_name, stats_path, which);
    }
  }
  closedir(dir);

  return 0;
}

int main(int argc, char *argv[])
{
  int intvl = DEFAULT_LLTOP_INTVL;

  struct option opts[] = {
    { "interval", 1, 0, 'i' },
    { 0, 0, 0, 0},
  };

  int c;
  while ((c = getopt_long(argc, argv, "i:", opts, 0)) > 0) {
    switch (c) {
    case 'i':
      intvl = atoi(optarg);
      if (intvl <= 0)
        FATAL("invalid sleep interval \"%s\"\n", optarg);
      continue;
    case '?':
      FATAL("invalid option\n");
    }
  }

  /* Set stdout line buffered so the lines from different lltop-servs
   * don't clobber each other.  Can't find a guarantee that ssh won't
   * break up writes, but it seems to work. */
  setlinebuf(stdout);

  const char *filter_path[2] = {
    MDS_FILTER_PATH,
    OSS_FILTER_PATH,
  };

  int which, type, found = 0;
  for (which = 0; which < 2; which++) {
    for (type = 0; type < 2; type++) {
      if (chdir(filter_path[type]) < 0) {
        if (errno != ENOENT)
          FATAL("cannot open %s: %m\n", filter_path[type]);
        continue;
      }
      found++;

      DIR *dir = opendir(".");
      if (dir == NULL)
        FATAL("cannot open %s: %m\n", filter_path[type]);

      struct dirent *ent;
      while ((ent = readdir(dir))) {
        if (ent->d_type == DT_DIR && ent->d_name[0] != '.')
          get_target_stats(ent->d_name, which);
      }
      closedir(dir);
    }

    /* At the end of pass 0, if neither dir exists then we bail. */
    if (found == 0) {
      errno = ENOENT;
      FATAL("cannot access %s or %s: %m\n", filter_path[0], filter_path[1]);
    }

    if (which == 0) {
      TRACE("sleeping for %d seconds\n", intvl);
      sleep(intvl);
    }
  }

  struct rb_node *node;
  for (node = rb_first(&name_stats_root); node != NULL; node = rb_next(node)) {
    struct name_stats *s = rb_entry(node, struct name_stats, ns_node);

    /* If any stats are negative then we assume that the client was
       evicted while we slept, so we skip it. */
    if (s->ns_wr < 0 || s->ns_rd < 0 || s->ns_reqs < 0) {
      TRACE("skipping %s %ld %ld %ld\n", s->ns_name, s->ns_wr, s->ns_rd, s->ns_reqs);
      continue;
    }

    /* As an optimization, skip this client if all stats are zero. */
    if (s->ns_wr == 0 && s->ns_rd == 0 && s->ns_reqs == 0) {
      TRACE("skipping %s %ld %ld %ld\n", s->ns_name, s->ns_wr, s->ns_rd, s->ns_reqs);
      continue;
    }

    printf("%s %ld %ld %ld\n", s->ns_name, s->ns_wr, s->ns_rd, s->ns_reqs);
  }

#ifdef DEBUG
  rb_destroy(&name_stats_root, offsetof(struct name_stats, ns_node), &free);
#endif

  return 0;
}
