/* Stub file: should be created in configuration phase */
/* This configuration was taken from an Ubuntu i386 installation. */

/* Define to 1 if you have the `setproctitle' function. */
/* #undef HAVE_SETPROCTITLE */

/* Define to 1 if the PS_STRINGS thing exists. */
/* #undef HAVE_PS_STRINGS */

/* Define to 1 if you have the <sys/pstat.h> header file. */
/* #undef HAVE_SYS_PSTAT_H */

/* Define to 1 if you have the <sys/prctl.h> header file. */
/* #undef HAVE_SYS_PRCTL_H */

/* GCC 4.0 and later have support for specifying symbol visibility */
#if __GNUC__ >= 4 && !defined(__MINGW32__)
#  define HIDDEN __attribute__((visibility("hidden")))
#else
#  define HIDDEN
#endif

