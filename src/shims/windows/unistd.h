#ifndef UNISTD_H
#define UNISTD_H

#ifdef _WIN64
typedef long long ssize_t;
#else
typedef int ssize_t;
#endif

typedef unsigned int useconds_t;
int usleep(useconds_t usec);
int getppid();

#endif /* UNISTD_H */
