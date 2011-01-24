/* lltop main.c
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
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "lltop.h"
#include "hooks.h"
#include "rbtree.h"

struct name_stats {
  struct rb_node ns_node;
  long ns_wr, ns_rd, ns_reqs;
  char ns_name[];
};

struct cache_struct {
  struct rb_node c_node;
  struct name_stats *c_stats;
  char c_name[];
};

struct rb_root addr_cache_root = RB_ROOT;
struct rb_root host_cache_root = RB_ROOT;
struct rb_root name_stats_root = RB_ROOT;
int name_stats_count = 0;

static struct cache_struct *lookup(struct rb_root *root, const char *name, int create)
{
  struct cache_struct *cache;
  struct rb_node **link, *parent;

  link = &root->rb_node;
  parent = NULL;

  while (*link != NULL) {
    cache = rb_entry(*link, struct cache_struct, c_node);
    parent = *link;

    int cmp = strcmp(name, cache->c_name);
    if (cmp < 0)
      link = &((*link)->rb_left);
    else if (cmp > 0)
      link = &((*link)->rb_right);
    else
      return cache;
  }

  if (!create)
    return NULL;

  cache = alloc(sizeof(*cache) + strlen(name) + 1);
  memset(cache, 0, sizeof(*cache));
  rb_link_node(&cache->c_node, parent, link);
  rb_insert_color(&cache->c_node, root);
  strcpy(cache->c_name, name);
  return cache;
}

static struct name_stats *get_name_stats(const char *name)
{
  struct name_stats *stats;
  struct rb_node **link, *parent;

  link = &name_stats_root.rb_node;
  parent = NULL;

  while (*link != NULL) {
    stats = rb_entry(*link, struct name_stats, ns_node);
    parent = *link;

    int cmp = strcmp(name, stats->ns_name);
    if (cmp < 0)
      link = &((*link)->rb_left);
    else if (cmp > 0)
      link = &((*link)->rb_right);
    else
      return stats;
  }

  stats = alloc(sizeof(*stats) + strlen(name) + 1);
  memset(stats, 0, sizeof(*stats));
  rb_link_node(&stats->ns_node, parent, link);
  rb_insert_color(&stats->ns_node, &name_stats_root);
  strcpy(stats->ns_name, name);
  name_stats_count++;

  return stats;
}

void lltop_set_job(const char *host, const char *job)
{
  struct cache_struct *cache;

  cache = lookup(&host_cache_root, host, 1);
  cache->c_stats = get_name_stats(job);
}

#if 0
static struct name_stats *get_host_stats(const char *host)
{
  struct cache_struct *host_cache;
  char job[MAXNAME + 1];

  if (lltop_job_map != NULL) {
    host_cache = lookup(&host_cache_root, host, 0);
    if (host_cache != NULL)
      return host_cache->c_stats;
    return get_name_stats(host);
  }

  if (lltop_get_job == NULL || (*lltop_get_job)(host, job, sizeof(job)) < 0)
    return get_name_stats(host);
  return get_name_stats(job);
}

static struct name_stats *get_addr_stats(const char *addr)
{
  struct name_stats *stats = NULL;
  struct cache_struct *addr_cache;
  char host[MAXNAME + 1];

  addr_cache = lookup(&addr_cache_root, addr, 1);
  if (addr_cache->c_stats != NULL)
    return addr_cache->c_stats;

  if (lltop_get_host == NULL || (*lltop_get_host)(addr, host, sizeof(host)) < 0) {
    stats = get_name_stats(addr);
    goto have_stats;
  }

  stats = get_host_stats(host);

 have_stats:
  addr_cache->c_stats = stats;
  return stats;
}
#endif

static void account(const char *addr, long wr, long rd, long reqs)
{
  struct name_stats *stats = NULL;
  struct cache_struct *addr_cache;
  struct cache_struct *host_cache;
  char host[MAXNAME + 1];
  char job[MAXNAME + 1];

  addr_cache = lookup(&addr_cache_root, addr, 1);
  if (addr_cache->c_stats != NULL) {
    stats = addr_cache->c_stats;
    goto have_stats;
  }

  if (lltop_get_host == NULL || (*lltop_get_host)(addr, host, sizeof(host)) < 0) {
    stats = get_name_stats(addr);
    goto have_stats;
  }

  host_cache = lookup(&host_cache_root, host, 0);
  if (host_cache != NULL) {
    stats = host_cache->c_stats;
    goto have_stats;
  }

  if (lltop_get_job == NULL || (*lltop_get_job)(host, job, sizeof(job)) < 0) {
    stats = get_name_stats(host);
    goto have_stats;
  }

  stats = get_name_stats(job);

 have_stats:
  addr_cache->c_stats = stats;
  stats->ns_wr += wr;
  stats->ns_rd += rd;
  stats->ns_reqs += reqs;
}

static int name_stats_cmp(const struct name_stats **s1, const struct name_stats **s2)
{
  /* Sort descending by writes, then reads, then requests. */
  /* TODO Make sort rank configurable. */
  long wr = (*s1)->ns_wr - (*s2)->ns_wr;
  if (wr != 0)
    return wr > 0 ? -1 : 1;

  long rd = (*s1)->ns_rd - (*s2)->ns_rd;
  if (rd != 0)
    return rd > 0 ? -1 : 1;

  long reqs = (*s1)->ns_reqs - (*s2)->ns_reqs;
  if (reqs != 0)
    return reqs > 0 ? -1 : 1;

  return 0;
}

int main(int argc, char *argv[])
{
  char **serv_list = NULL;
  int serv_count = 0;
  if (lltop_config(argc, argv, &serv_list, &serv_count) < 0)
    FATAL("lltop_config() failed\n");

  char intvl_arg[80];
  snprintf(intvl_arg, sizeof(intvl_arg), "--interval=%d", lltop_intvl);

  close(0);
  open("/dev/null", O_RDONLY);

  int fdv[2];
  if (pipe(fdv) < 0)
    FATAL("cannot create pipe for lltop-serv subprocesses: %m\n");

  TRACE("starting lltop-serv subprocesses\n");

  int i;
  for (i = 0; i < serv_count; i++) {
    pid_t pid = fork();
    if (pid < 0) {
      FATAL("cannot fork: %m\n");
    } else if (pid == 0) {
      /* Close read end of pipe, redirect stdout to write end. */
      close(fdv[0]);
      dup2(fdv[1], 1);
      close(fdv[1]);
      execl(lltop_ssh_path, lltop_ssh_path, serv_list[i],
            lltop_serv_path, intvl_arg, (char*) NULL);
      FATAL("cannot exec '%s': %m\n", lltop_ssh_path);
    }
  }
  lltop_free_serv_list(serv_list, serv_count);
  close(fdv[1]);

  if (lltop_job_map != NULL && (*lltop_job_map)() < 0)
    FATAL("cannot get job map: %m\n");

  TRACE("reading lltop-serv output\n");

  FILE *stats_pipe = fdopen(fdv[0], "r");
  if (stats_pipe == NULL)
    FATAL("cannot create pipe: %m\n");

  char *line = NULL;
  size_t line_size = 0;
  int line_count = 0;

  while (getline(&line, &line_size, stats_pipe) >= 0) {
    if (line_count++ == 0)
      TRACE("got first line from lltop-serv\n");

#if MAXNAME != 1024
#error MAXNAME != 1024 may break sscanf().
#endif
    char addr[MAXNAME + 1];
    long wr, rd, reqs;
    /* lltop-serv output is <ipv4-addr>@<net> <wr> <rd> <reqs>. */
    if (sscanf(line, "%1024s %ld %ld %ld", addr, &wr, &rd, &reqs) != 4) {
      ERROR("invalid line \"%s\"\n", chop(line, '\n'));
      continue;
    }

    /* Chop off '@<net>' and account. */
    account(chop(addr, '@'), wr, rd, reqs);
  }
  free(line);

  TRACE("read %d lines from lltop-serv\n", line_count);

  if (ferror(stats_pipe))
    ERROR("error reading from pipe: %m\n");

  if (fclose(stats_pipe) < 0)
    ERROR("error closing pipe: %m\n");

  TRACE("sorting and printing stats\n");

  /* OK, done reading, now sort and print. */
  struct name_stats **stats_vec;
  stats_vec = alloc(name_stats_count * sizeof(struct name_stats*));

  i = 0;
  struct rb_node *node;
  for (node = rb_first(&name_stats_root); node != NULL; node = rb_next(node))
    stats_vec[i++] = rb_entry(node, struct name_stats, ns_node);

  qsort(stats_vec, name_stats_count, sizeof(struct name_stats*),
        (int (*)(const void*, const void*)) &name_stats_cmp);

  lltop_print_header(stdout);

  for (i = 0; i < name_stats_count; i++) {
    struct name_stats *s = stats_vec[i];
    lltop_print_name_stats(stdout, s->ns_name, s->ns_wr, s->ns_rd, s->ns_reqs);
  }

  /* Cleanup is somewhat pointless since we're exiting right away. */
#ifdef DEBUG
  rb_destroy(&addr_cache_root, offsetof(struct cache_struct, c_node), &free);
  rb_destroy(&host_cache_root, offsetof(struct cache_struct, c_node), &free);
  rb_destroy(&name_stats_root, offsetof(struct name_stats, ns_node), &free);
  free(stats_vec);
#endif

  return 0;
}
