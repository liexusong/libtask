#include <stdlib.h>
#include <unistd.h>
#include "taskimpl.h"

/**
 * Async IO implement
 * @author: liexusong (c) liexusong at qq dot com
 */

#define IO_READ   1
#define IO_WRITE  2

struct IOTask {
    Task *owner;
    int fd;
    int op;
    char *iobuf;
    int size;
    int nbyes;
    struct iotask *next;
};

typedef struct {
    struct IOTask *wq;  /* wait queue */
    struct IOTask *rq;  /* ready queue */
    pthread_mutex_t wlock;
    pthread_mutex_t rlock;
    pthread_cond_t cond;
} AsyncIO;


static AsyncIO aio_ctx;


static void _async_io_thread(void *arg)
{
    struct IOTask *io;

    for ( ;; ) {
        pthread_mutex_lock(&aio_ctx.wlock);

        while (aio_ctx.wq == NULL) {
            pthread_cond_wait(&aio_ctx.cond);
        }

        io = aio_ctx.wq;
        aio_ctx.wq = io->next;

        pthread_mutex_unlock(&aio_ctx.wlock);

        switch (io->op) {
        case IO_READ:
            io->nbytes = read(io->fd, io->iobuf, io->size);
            break;
        case IO_WRITE:
            io->nbytes = write(io->fd, io->iobuf, io->size);
            break;
        }

        pthread_mutex_lock(&aio_ctx.rlock);

        io->next = aio_ctx.rq;
        aio_ctx.rq = io;

        pthread_mutex_unlock(&aio_ctx.rlock);
    }
}


static int _async_io(int op, int fd, char *buf, size_t size)
{
    struct IOTask *io;
    int nbytes; 
    
    io = malloc(sizeof(*io));
    if (!io) {
        return -1;
    }

    io->owner = taskrunning;
    io->fd = fd;
    io->op = op;
    io->buf = buf;
    io->size = size;

    pthread_mutex_lock(&aio_ctx.wlock);

    io->next = aio_ctx.wq;
    aio_ctx.wq = io;

    pthread_cond_signal(&aio_ctx.cond);

    pthread_mutex_unlock(&aio_ctx.wlock);

	taskswitch();  /* yeild */

	nbytes = io->nbytes;

	free(io);

    return nbytes;
}


int qread(int fd, char *buf, size_t size)
{
    return _async_io(IO_READ, fd, buf, size);
}


int qwrite(int fd, char *buf, size_t size)
{
    return _async_io(IO_WRITE, fd, buf, size);
}


void qio_running_ready()
{
    struct IOTask *io;

    pthread_mutex_lock(&aio_ctx.rlock);

    io = aio_ctx.rq;
    if (io) {
        aio_ctx.rq = io->next;
    }

    pthread_mutex_unlock(&aio_ctx.rlock);

    if (io) {
        taskready(io->owner);
    }
}


int qio_init(int io_threads)
{
    pthread_t tid;
    int ret;

    if (pthread_mutex_init(&aio_ctx.wlock, NULL) == -1 ||
        pthread_mutex_init(&aio_ctx.rlock, NULL) == -1 ||
        pthread_cond_init(&aio_ctx.cond, NULL) == -1)
    {
        return -1;
    }

    if (io_threads <= 0) {
        io_threads = 5;
    }

    for (; io_threads > 0; io_threads--) {
        if (pthread_create(&tid, NULL, _async_io_thread, NULL) == -1) {
        	return -1;
        }
    }

    return 0;
}

