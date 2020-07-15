#ifndef UNISTD_H
#define UNISTD_H

#ifdef _WIN64
typedef long long ssize_t;
#else
typedef int ssize_t;
#endif

int getppid();

#endif /* UNISTD_H */
