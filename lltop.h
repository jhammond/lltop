#ifndef _LLTOP_H_
#define _LLTOP_H_
#define _GNU_SOURCE
#include <errno.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_LLTOP_INTVL 10

#ifdef DEBUG
#define ERROR(fmt,arg...) \
  fprintf(stderr, "%s:(%s:%d:%s) " fmt, program_invocation_short_name, \
          __FILE__, __LINE__, __func__, ##arg)
#define TRACE ERROR
#else
#define ERROR(fmt,arg...) \
  fprintf(stderr, "%s: " fmt, program_invocation_short_name, ##arg)
#define TRACE(fmt,arg...) ((void) 0)
#endif

#define FATAL(fmt,arg...) do { \
    ERROR(fmt, ##arg);         \
    exit(1);                   \
  } while (0)

static inline char *chop(char *s, int c)
{
  char *p = strchr(s, c);
  if (p != NULL)
    *p = 0;
  return s;
}

static inline void *alloc(size_t size)
{
  void *addr = malloc(size);

  if (size != 0 && addr == NULL)
    FATAL("out of memory\n");

  return addr;
}

#endif
