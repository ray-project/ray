#ifndef GETOPT_H
#define GETOPT_H

extern char *optarg;
extern int optind, opterr, optopt;
int getopt(int nargc, char *const nargv[], const char *ostr);

#endif /* GETOPT_H */
