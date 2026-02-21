/*-------------------------------------------------------------------------
 *
 * spt_status.h
 *
 * Declarations for spt_status.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef SPT_STATUS_H
#define SPT_STATUS_H

#include "c.h"

HIDDEN extern bool update_process_title;

HIDDEN extern char **save_ps_display_args(int argc, char **argv);

HIDDEN extern void init_ps_display(const char *initial_str);

HIDDEN extern void set_ps_display(const char *activity, bool force);

HIDDEN extern const char *get_ps_display(size_t *displen);

HIDDEN extern void set_thread_title(const char *title);

HIDDEN extern void get_thread_title(char *title);

#endif   /* SPT_STATUS_H */

