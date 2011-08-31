#define _GNU_SOURCE
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ncurses.h>
#include <termios.h>
#include <ev.h>
#include "string1.h"
#include "lltop.h"
#include "dict.h"
#include "list.h"

const char *job_mapper_cmd = "cat /tmp/lltop/mapper-fifo";
const char *nid_file_path = "/tmp/lltop/client-nids";

#define BIND_HOST "0.0.0.0" /* INADDR_ANY */
#define BIND_PORT "9909"
#define NR_STATS 3
#define NR_CLIENTS_HINT 4096
#define NR_JOBS_HINT 256
#define NR_SERVS_HINT 128
#define RX_BUF_SIZE 8096
#define JOB_NONE "0"
#define FE_AGE_LIMIT 32
#define REFRESH_INTERVAL 10.0
#define SERV_INTERVAL 10.0

static size_t nr_jobs;
static struct dict name_job_dict;
static struct dict name_client_dict;
static struct dict name_serv_dict;
static struct dict nid_client_dict;

struct pair {
  void *p_value;
  char p_key[];
};

struct rx_buf {
  char *r_buf;
  size_t r_seen, r_count, r_buf_size;
  unsigned int r_overflow:1;
};

struct job_mapper {
  struct ev_child jm_child_w;
  struct ev_io jm_io_w;
  struct ev_timer jm_timer_w; /* TODO */
  struct rx_buf jm_rx_buf;
  const char *jm_cmd; /* Or cmdline. */
  pid_t jm_pid;
};

struct job_struct {
  long j_stats[NR_STATS];
  struct list_head j_client_list;
  struct list_head j_frame_list;
  char *j_owner, *j_dir;
  hash_t j_hash;
  char j_name[];
};

struct client_struct {
  struct job_struct *c_job;
  struct list_head c_job_link;
  char c_name[];
};

struct frame_entry {
  struct job_struct *fe_job;
  struct list_head fe_job_link;
  long fe_stats[2][NR_STATS];
  unsigned int fe_gen;
  char fe_name[];
};

/* TODO Add s_fs. */
struct serv_struct {
  struct ev_io s_io_w;
  struct ev_timer s_timer_w;
  struct rx_buf s_rx_buf;
  struct dict s_frame;  
  struct sockaddr_storage s_addr;
  socklen_t s_addrlen;
  /* TODO long s_stats[NR_STATS]; */
  unsigned int s_gen;
  unsigned int s_connected:1;
  char s_name[];
};

#define OOM() FATAL("cannot allocate memory\n");

#define may_ignore_errno() (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)

#define GET_NAMED(ptr,member,name) \
  (ptr) = (typeof(ptr)) (((char *) name) - offsetof(typeof(*ptr), member))

/* TODO Try using offsetof() in malloc() below. */

#define ALLOC_NAMED(ptr,member,name) do {		\
    ptr = malloc(sizeof(*ptr) + strlen(name) + 1);	\
    if (ptr == NULL)					\
      OOM();						\
    memset(ptr, 0, sizeof(*ptr));			\
    strcpy(ptr->member, name);				\
  } while (0)

static inline void fd_set_nonblock(int fd)
{
  int flags = fcntl(fd, F_GETFL);
  fcntl(fd, F_SETFL, flags|O_NONBLOCK);
}

int rx_buf_init(struct rx_buf *rb, size_t size)
{
  memset(rb, 0, sizeof(*rb));

  rb->r_buf = malloc(size);
  if (rb->r_buf == NULL)
    return -1;

  rb->r_buf_size = size;
  return 0;
}

ssize_t rx_buf_read(int fd, struct rx_buf *rb)
{
  char *read_pos;
  ssize_t nr_avail, nr_read;

  if (rb->r_seen > 0) {
    rb->r_count -= rb->r_seen;
    memmove(rb->r_buf, rb->r_buf + rb->r_seen, rb->r_count);
    rb->r_seen = 0;
  }

 again:
  read_pos = rb->r_buf + rb->r_count;
  nr_avail = rb->r_buf_size - rb->r_count;

  if (nr_avail <= 0) {
    rb->r_count = 0;
    rb->r_overflow = 1;
    goto again;
  }

  nr_read = read(fd, read_pos, nr_avail);
  if (nr_read > 0)
    rb->r_count += nr_read;

  return nr_read;
}

char *rx_buf_iter(struct rx_buf *rb)
{
  char *pos, *sep, *end;

 again:
  pos = rb->r_buf + rb->r_seen;
  end = rb->r_buf + rb->r_count;
  sep = memchr(pos, '\n', end - pos); /* XXX '\n' */
  
  if (sep == NULL)
    return NULL;

  rb->r_seen += sep - pos + 1;

  if (rb->r_overflow) {
    rb->r_overflow = 0;
    goto again;
  }

  *sep = 0;

  return pos;
}

struct job_struct *job_lookup(const char *name, int create)
{
  struct job_struct *job;
  hash_t hash = dict_strhash(name);
  struct dict_entry *de = dict_entry_ref(&name_job_dict, hash, name);
  if (de->d_key != NULL) {
    GET_NAMED(job, j_name, de->d_key);
    return job;
  }

  if (!create)
    return NULL;

  TRACE("creating job `%s'\n", name);
  ALLOC_NAMED(job, j_name, name);
  INIT_LIST_HEAD(&job->j_client_list);
  INIT_LIST_HEAD(&job->j_frame_list);
  job->j_hash = hash;
  /* TODO owner, workdir. */

  if (dict_entry_set(&name_job_dict, de, hash, job->j_name) < 0)
    OOM();

  nr_jobs++;

  return job;
}

void job_put(struct job_struct *job)
{
  struct dict_entry *de;

  if (!list_empty(&job->j_client_list))
    return;

  if (!list_empty(&job->j_frame_list))
    return;

  TRACE("freeing job `%s'\n", job->j_name);
  de = dict_entry_ref(&name_job_dict, job->j_hash, job->j_name);
  dict_entry_remv(&name_job_dict, de, 1); /* XXX Resize. */
  free(job);

  nr_jobs--;
}


struct client_struct *client_lookup_by_name(const char *name, int create)
{
  struct client_struct *cli;
  hash_t hash = dict_strhash(name);
  struct dict_entry *de = dict_entry_ref(&name_client_dict, hash, name);
  if (de->d_key != NULL) {
    GET_NAMED(cli, c_name, de->d_key);
    return cli;
  }

  if (!create)
    return NULL;

  ALLOC_NAMED(cli, c_name, name); /* XXX */
  INIT_LIST_HEAD(&cli->c_job_link);

  if (dict_entry_set(&name_client_dict, de, hash, cli->c_name) < 0)
    OOM();

  return cli;
}

void client_set_job_by_name(struct client_struct *cli, const char *job_name)
{
  struct job_struct *cur_job, *new_job;

  if (strcmp(job_name, JOB_NONE) == 0)
    job_name = cli->c_name;

  cur_job = cli->c_job;
  if (cur_job != NULL && strcmp(job_name, cur_job->j_name) == 0)
    return;

  TRACE("adding client `%s' to job `%s'\n", cli->c_name, job_name);
  new_job = job_lookup(job_name, 1);
  if (new_job == NULL)
    OOM();

  list_move(&cli->c_job_link, &new_job->j_client_list);
  cli->c_job = new_job;

  if (cur_job != NULL)
    job_put(cur_job);
}

static inline struct job_struct *
client_get_job(struct client_struct *cli, int create)
{
  if (cli->c_job == NULL && create) /* Use client name as job name. */
    client_set_job_by_name(cli, cli->c_name);

  return cli->c_job;
}

void client_add_nid(struct client_struct *cli, const char *nid)
{
  struct pair *p;
  hash_t hash = dict_strhash(nid);
  struct dict_entry *de = dict_entry_ref(&nid_client_dict, hash, nid);
  if (de->d_key != NULL) {
    GET_NAMED(p, p_key, de->d_key);
    if (cli != p->p_value)
      ERROR("NID `%s' assigned to clients `%s' and `%s'\n", 
	    nid, ((struct client_struct *) (p->p_value))->c_name, cli->c_name);
    p->p_value = cli; /* Most recent wins. */
    return;
  }

  TRACE("adding NID `%s' to client `%s'\n", nid, cli->c_name);
  ALLOC_NAMED(p, p_key, nid);
  p->p_value = cli;

  if (dict_entry_set(&nid_client_dict, de, hash, p->p_key) < 0)
    OOM();
}

struct client_struct *client_lookup_by_nid(const char *nid, int create)
{
  struct pair *p;
  hash_t hash = dict_strhash(nid);
  struct dict_entry *de = dict_entry_ref(&nid_client_dict, hash, nid);
  if (de->d_key != NULL) {
    GET_NAMED(p, p_key, de->d_key);
    goto have_p;
  }

  if (!create)
    return NULL;

  /* Fake a client, using NID for name. */
  ALLOC_NAMED(p, p_key, nid);
  p->p_value = client_lookup_by_name(nid, 1);
  if (p->p_value == NULL)
    OOM();

  if (dict_entry_set(&nid_client_dict, de, hash, p->p_key) < 0)
    OOM();

 have_p:
  return p->p_value;
}

static void job_mapper_child_cb(EV_P_ ev_child *w, int revents)
{
  /* TODO Check revents. */

  struct job_mapper *jm = container_of(w, struct job_mapper, jm_child_w);

  ev_child_stop(EV_A_ w);
  ERROR("job mapper `%s', pid %d exited with status %x\n",
	jm->jm_cmd, w->rpid, w->rstatus); /* TODO WIFEXITED, ... */

  jm->jm_pid = 0;
}

static void job_mapper_io_cb(EV_P_ ev_io *w, int revents)
{
  /* TODO EV_ERROR. */

  struct job_mapper *jm = container_of(w, struct job_mapper, jm_io_w);
  struct rx_buf *rb = &jm->jm_rx_buf;

  ssize_t nr_read = rx_buf_read(w->fd, rb);
  if (nr_read < 0) {
    if (may_ignore_errno())
      return;
    /* TODO Add pid. Restart. */
    FATAL("cannot read from job mapper `%s': %m\n", jm->jm_cmd);
  }

  char *msg, *cli_name, *job_name;
  while ((msg = rx_buf_iter(rb)) != NULL) {
    cli_name = wsep(&msg);
    job_name = wsep(&msg);

    if (cli_name == NULL || job_name == NULL)
      continue;

    struct client_struct *cli = client_lookup_by_name(cli_name, 1);
    if (cli == NULL)
      OOM();

    client_set_job_by_name(cli, job_name);
  }
}

int job_mapper_init(EV_P_ struct job_mapper *jm, const char *cmd)
{
  int pfd[2];
  if (pipe(pfd) < 0)
    FATAL("cannot create pipe: %m\n");

  pid_t pid = fork();
  if (pid < 0)
    FATAL("cannot start job mapper: %m\n");

  if (pid == 0) {
    close(pfd[0]);
    dup2(pfd[1], 1);
    signal(SIGPIPE, SIG_DFL);
    setpgid(0, 0);
    execl("/bin/sh", "sh", "-c", cmd, (char *) NULL);
    ERROR("cannot execute command `%s': %m\n", cmd);
    exit(255);
  }

  close(pfd[1]);

  fd_set_nonblock(pfd[0]);

  memset(jm, 0, sizeof(*jm));

  ev_io_init(&jm->jm_io_w, &job_mapper_io_cb, pfd[0], EV_READ);
  ev_io_start(EV_A_ &jm->jm_io_w);

  ev_child_init(&jm->jm_child_w, &job_mapper_child_cb, pid, 0);
  ev_child_start(EV_A_ &jm->jm_child_w);

  if (rx_buf_init(&jm->jm_rx_buf, RX_BUF_SIZE) < 0)
    OOM();

  jm->jm_cmd = cmd;
  jm->jm_pid = pid;

  return 0;
}

static void serv_io_cb(EV_P_ ev_io *w, int revents);
static void serv_timer_cb(EV_P_ ev_timer *w, int revents);

struct serv_struct *
serv_create(const char *name, ev_tstamp offset, ev_tstamp interval)
{
  struct serv_struct *serv;
  hash_t hash = dict_strhash(name);
  struct dict_entry *de = dict_entry_ref(&name_serv_dict, hash, name);
  if (de->d_key != NULL) {
    GET_NAMED(serv, s_name, de->d_key);
    ERROR("server struct `%s' already exists\n", name);
    return serv;
  }

  TRACE("creating serv `%s', offset %f, interval %f\n", name, offset, interval);
  ALLOC_NAMED(serv, s_name, name);
  ev_init(&serv->s_io_w, &serv_io_cb); /* Don't start IO. */
  ev_timer_init(&serv->s_timer_w, &serv_timer_cb, offset, interval);

  if (rx_buf_init(&serv->s_rx_buf, RX_BUF_SIZE) < 0)
    OOM();
  if (dict_init(&serv->s_frame, NR_JOBS_HINT) < 0)
    OOM();

  return serv;
}

static void serv_disconnect(EV_P_ struct serv_struct *serv)
{
  if (!serv->s_connected)
    return;

  TRACE("disconnection server `%s'\n", serv->s_name);
  ev_io_stop(EV_A_ &serv->s_io_w);
  close(serv->s_io_w.fd);
  serv->s_io_w.fd = -1;
  /* Clear frames? */
  /* ... */

  serv->s_connected = 0;
}

static struct serv_struct *
serv_lookup(const char *name, int create)
{
  struct serv_struct *serv = NULL;
  hash_t hash = dict_strhash(name);
  struct dict_entry *de = dict_entry_ref(&name_serv_dict, hash, name);
  if (de->d_key != NULL) {
    GET_NAMED(serv, s_name, de->d_key);
    goto have_serv;
  }

  /* TODO port. */
  if (create)
    serv = serv_create(name, 0., SERV_INTERVAL);

 have_serv:
  return serv;
}

static void serv_connect(EV_P_ struct serv_struct *serv,
			 int sfd, struct sockaddr *addr, socklen_t addrlen)
{
  serv_disconnect(EV_A_ serv);
  TRACE("connecting server `%s'\n", serv->s_name);
  memcpy(&serv->s_addr, addr, addrlen);
  serv->s_addrlen = addrlen;
  ev_io_set(&serv->s_io_w, sfd, EV_READ);
  ev_io_start(EV_A_ &serv->s_io_w);
  ev_timer_start(EV_A_ &serv->s_timer_w);
  serv->s_connected = 1;
}

static void serv_error(EV_P_ struct serv_struct *serv)
{
  ERROR("event error from server `%s': %m\n", serv->s_name);
  serv_disconnect(EV_A_ serv);
}

static void serv_msg(struct serv_struct *serv, char *msg)
{
  char *cli_nid = wsep(&msg);
  if (cli_nid == NULL || msg == NULL)
    return;

  /* TODO Use NR_STATS, strtol(). */
  long stats[NR_STATS];
  if (sscanf(msg, "%ld %ld %ld", &stats[0], &stats[1], &stats[2]) != 3)
    return;

  struct client_struct *cli = client_lookup_by_nid(cli_nid, 1);
  if (cli == NULL)
    OOM();

  struct job_struct *job = client_get_job(cli, 1);
  if (job == NULL)
    OOM();

  struct dict_entry *de;
  struct frame_entry *fe;
  de = dict_entry_ref(&serv->s_frame, job->j_hash, job->j_name);
  if (de->d_key != NULL) {
    GET_NAMED(fe, fe_name, de->d_key);
    goto have_fe;
  }

  ALLOC_NAMED(fe, fe_name, job->j_name);
  fe->fe_job = job;
  list_add(&fe->fe_job_link, &job->j_frame_list);
  fe->fe_gen = serv->s_gen;

  if (dict_entry_set(&serv->s_frame, de, job->j_hash, fe->fe_name) < 0)
    FATAL("dict_entry_set: %m\n");

 have_fe:
  TRACE("serv `%s', s_gen %u, job `%s', fe_gen %u\n",
	serv->s_name, serv->s_gen, fe->fe_job->j_name, fe->fe_gen);
  if (fe->fe_gen == serv->s_gen)
    /* OK */;
  else if (fe->fe_gen == serv->s_gen - 1)
    memset(fe->fe_stats[serv->s_gen % 2], 0, NR_STATS * sizeof(long));
  else
    memset(fe->fe_stats, 0, 2 * NR_STATS * sizeof(long));

  fe->fe_gen = serv->s_gen;

  int i;
  for (i = 0; i < NR_STATS; i++)
    fe->fe_stats[fe->fe_gen % 2][i] += stats[i];

  TRACE("fe_stats %ld %ld %ld\n",
	fe->fe_stats[fe->fe_gen % 2][0],
	fe->fe_stats[fe->fe_gen % 2][1],
	fe->fe_stats[fe->fe_gen % 2][2]);
}

static void serv_io_cb(EV_P_ ev_io *w, int revents)
{
  struct serv_struct *serv = container_of(w, struct serv_struct, s_io_w);
  struct rx_buf *rb = &serv->s_rx_buf;

  if (revents & EV_ERROR) {
    /* ... */
    serv_error(EV_A_ serv);
    return;
  }

  ssize_t nr_read = rx_buf_read(w->fd, rb);
  if (nr_read < 0) {
    if (may_ignore_errno())
      return;
    ERROR("cannot read from server `%s': %m\n", serv->s_name);
    serv_error(EV_A_ serv);
    return;
  }

  char *msg;
  while ((msg = rx_buf_iter(rb)) != NULL)
    serv_msg(serv, msg);
}

static void serv_timer_cb(EV_P_ ev_timer *w, int revents)
{
  struct serv_struct *serv = container_of(w, struct serv_struct, s_timer_w);
  if (!serv->s_connected)
    /* TODO */;

  size_t de_iter = 0;
  struct dict_entry *de;
  while ((de = dict_for_each_ref(&serv->s_frame, &de_iter)) != NULL) {
    struct frame_entry *fe;
    GET_NAMED(fe, fe_name, de->d_key);

    int fe_age = serv->s_gen - fe->fe_gen;
    if (fe_age > FE_AGE_LIMIT) {
      list_del(&fe->fe_job_link);
      job_put(fe->fe_job);
      dict_entry_remv(&serv->s_frame, de, 0);
      free(fe);
      continue;
    }

    long *s_prev = fe->fe_stats[(serv->s_gen - 1) % 2];
    long *s_next = fe->fe_stats[(serv->s_gen - 0) % 2];

    int i;
    for (i = 0; i < NR_STATS; i++) {
      if (fe_age == 0)
	fe->fe_job->j_stats[i] += s_next[i] - s_prev[i];
      else if (fe_age == 1)
	fe->fe_job->j_stats[i] -= s_prev[i];
    }
  }
  dict_allow_resize(&serv->s_frame, NR_JOBS_HINT);

  serv->s_gen++;
}

static int 
job_stats_cmp(const void *v1, const void *v2)
{
  const struct job_struct **j1 = (void *) v1, **j2 = (void *) v2;
  const long *s1 = (*j1)->j_stats, *s2 = (*j2)->j_stats;

  /* Sort descending by writes, then reads, then requests. */
  /* TODO Make sort rank configurable. */
  int i;
  for (i = 0; i < NR_STATS; i++) { /* XXX ORDER */
    long diff = s1[i] - s2[i];
    if (diff != 0)
      return diff > 0 ? -1 : 1;
  }

  return 0;
}

static void refresh_display(void)
{
  TRACE("refresh LINES %d, COLS %d\n", LINES, COLS);

  struct job_struct **job_list = NULL;
  job_list = calloc(nr_jobs, sizeof(job_list[0]));
  if (job_list == NULL)
    OOM();

  size_t i = 0, j = 0;
  char *name;
  while ((name = dict_for_each(&name_job_dict, &i)) != NULL && j < nr_jobs)
    GET_NAMED(job_list[j++], j_name, name);

  if (j != nr_jobs)
    FATAL("internal error: j %zu, nr_jobs %zu\n", j, nr_jobs);

  qsort(job_list, nr_jobs, sizeof(job_list[0]), &job_stats_cmp);

  for (j = 0; j < nr_jobs && j < LINES; j++) {
    struct job_struct *job = job_list[j];

    char buf[4096];
    snprintf(buf, sizeof(buf), "%s %ld %ld %ld\n", job->j_name,
	     job->j_stats[0], job->j_stats[1], job->j_stats[2]);
    mvaddnstr(j, 0, buf, -1);
  }

  free(job_list);

  refresh();
}

int read_nid_file(const char *path)
{
  int rc = -1;
  FILE *file = NULL; 
  char *line = NULL;
  size_t line_size = 0;
  int line_nr = 0;

  file = fopen(path, "r");
  if (file == NULL) {
    ERROR("cannot open `%s': %m\n", path);
    goto out;
  }

  while (getline(&line, &line_size, file) >= 0) {
    char *iter, *name, *nid;
    struct client_struct *cli;

    line_nr++;
    iter = line;
    name = wsep(&iter);
    if (name == NULL)
      continue;

    cli = client_lookup_by_name(name, 1);
    if (cli == NULL)
      continue;

    while ((nid = wsep(&iter)) != NULL)
      client_add_nid(cli, nid);
  }
  rc = 0;

 out:
  free(line);
  if (file != NULL)
    fclose(file);

  return rc;
}

static void listen_cb(EV_P_ ev_io *w, int revents)
{
  /* TODO Handle EV_ERROR in revents. */
  int sfd;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);
  struct serv_struct *serv;

  sfd = accept(w->fd, (struct sockaddr *) &addr, &addrlen);
  if (sfd < 0) {
    if (may_ignore_errno())
      return;
    FATAL("cannot accept connections: %m\n"); /* ... */
  }
  fd_set_nonblock(sfd);

  char name[NI_MAXHOST], port[NI_MAXSERV];
  int gni_rc = getnameinfo((struct sockaddr *) &addr, addrlen,
			   name, sizeof(name), port, sizeof(port), 0);
  if (gni_rc != 0) {
    ERROR("cannot get name info for server connection: %s\n", gai_strerror(gni_rc));
    goto err;
  }

  TRACE("received connection from host `%s', port `%s'\n", name, port);
  serv = serv_lookup(name, 1); /* XXX create. Port? */
  if (serv == NULL)
    goto err;

  serv_connect(EV_A_ serv, sfd, (struct sockaddr *) &addr, addrlen);
  return;

 err:
  close(sfd);
}

static void stdin_cb(EV_P_ ev_io *w, int revents)
{
  int c = getch();
  if (c == ERR)
    return;

  TRACE("got `%c' from stdin\n", c);
  switch (c) {
  case ' ':
  case '\n':
    refresh_display();
    break;
  case 'q':
    ev_break(EV_A_ EVBREAK_ALL);
    break;
  default:
    ERROR("unknown command `%c': try `h' for help\n", c); /* TODO help. */
    break;
  }
}

static void refresh_cb(EV_P_ ev_timer *w, int revents)
{
  refresh_display();
}

static void sigint_cb(EV_P_ ev_signal *w, int revents)
{
  TRACE("handling signal %d `%s'\n", w->signum, strsignal(w->signum));
  ev_break(EV_A_ EVBREAK_ALL);
}

static void sigwinch_cb(EV_P_ ev_signal *w, int revents)
{
  TRACE("handling signal %d `%s'\n", w->signum, strsignal(w->signum));
  struct winsize ws;

  int fd = open("/dev/tty", O_RDWR);
  if (fd < 0) {
    ERROR("cannot open `/dev/tty': %m\n");
    goto out;
  }

  if (ioctl(fd, TIOCGWINSZ, &ws) < 0) {
    ERROR("cannot get window size: %m\n");
    goto out;
  }

  LINES = ws.ws_row;
  COLS = ws.ws_col;

  refresh_display();
 out:
  if (fd >= 0)
    close(fd);
}

int main(int argc, char *argv[])
{
  const char *bind_host = BIND_HOST, *bind_port = BIND_PORT;
  int listen_backlog = 128; /* XXX */
  struct job_mapper mapper;

  signal(SIGPIPE, SIG_IGN);

  if (dict_init(&name_client_dict, NR_CLIENTS_HINT) < 0)
    OOM();
  if (dict_init(&name_job_dict, NR_JOBS_HINT) < 0)
    OOM();
  if (dict_init(&name_serv_dict, NR_SERVS_HINT) < 0)
    OOM();
  if (dict_init(&nid_client_dict, NR_CLIENTS_HINT) < 0)
    OOM();

  if (read_nid_file(nid_file_path) < 0)
    FATAL("cannot read NID file `%s'\n", nid_file_path);

  if (job_mapper_init(EV_DEFAULT_ &mapper, job_mapper_cmd) < 0)
    FATAL("cannot start job mapper `%s'\n", job_mapper_cmd);

  /* Begin curses magic. */
  /* setlocale(LC_ALL, ""); */
  initscr();
  cbreak();
  noecho();
  nonl();
  intrflush(stdscr, 0);
  keypad(stdscr, 1);
  nodelay(stdscr, 1);

  struct addrinfo *info, *list, hints = {
    .ai_family = AF_INET, /* Still needed. */
    .ai_socktype = SOCK_STREAM,
    .ai_flags = AI_PASSIVE, /* Ignored if bind_host != NULL. */
  };

  int gai_rc = getaddrinfo(bind_host, bind_port, &hints, &list);
  if (gai_rc != 0)
      FATAL("cannot resolve host `%s', service `%s': %s\n",
            bind_host, bind_port, gai_strerror(gai_rc));

  int lfd = -1;
  for (info = list; info != NULL; info = info->ai_next) {
    lfd = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (lfd < 0)
      continue;

    if (bind(lfd, info->ai_addr, info->ai_addrlen) == 0)
      break;

    close(lfd);
    lfd = -1;
  }
  freeaddrinfo(info);

  if (lfd < 0)
    FATAL("cannot bind to host `%s', service `%s': %m\n", bind_host, bind_port);

  fd_set_nonblock(lfd); /* SOCK_NONBLOCK */

  if (listen(lfd, listen_backlog) < 0)
    FATAL("cannot listen on `%s', service `%s': %m\n", bind_host, bind_port);

  struct ev_io listen_w;
  ev_io_init(&listen_w, &listen_cb, lfd, EV_READ);
  ev_io_start(EV_DEFAULT_ &listen_w);

  struct ev_io stdin_w;
  ev_io_init(&stdin_w, &stdin_cb, 0, EV_READ);
  ev_io_start(EV_DEFAULT_ &stdin_w);

  struct ev_timer refresh_w;
  ev_timer_init(&refresh_w, &refresh_cb, 0, REFRESH_INTERVAL);
  ev_timer_start(EV_DEFAULT_ &refresh_w);

  struct ev_signal sigint_w;
  ev_signal_init(&sigint_w, &sigint_cb, SIGINT);
  ev_signal_start(EV_DEFAULT_ &sigint_w);

  struct ev_signal sigwinch_w;
  ev_signal_init(&sigwinch_w, &sigwinch_cb, SIGWINCH);
  ev_signal_start(EV_DEFAULT_ &sigwinch_w);

  ev_run(EV_DEFAULT_ 0);

  if (mapper.jm_pid > 0 && killpg(mapper.jm_pid, SIGTERM) < 0)
    ERROR("cannot kill job mapper `%s', pid %d: %m\n",
	  mapper.jm_cmd, mapper.jm_pid);

  if (lfd > 0)
    shutdown(lfd, SHUT_RDWR);

  /* ... */

  endwin(); /* TODO Call on OOM(). */

  return 0;
}
