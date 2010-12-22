CC = gcc
CPPFLAGS = $(CDEBUG)
CFLAGS = -Wall 
lltop_objects = main.o hooks.o rbtree.o
lltop_serv_objects = serv.o rbtree.o

all: lltop lltop-serv

lltop: $(lltop_objects)
	$(CC) $(CFLAGS) $^ -o $@ 

lltop-serv: $(lltop_serv_objects)
	$(CC) $(CFLAGS) $^ -o $@ -lrt

clean:
	rm -f lltop $(lltop_objects) lltop-serv $(lltop_serv_objects)
