VOID: rig failure, not a result.

The kill arm produced zero samples and bailed with 'no gcs_server found'.
Cause: gcs_procs() called open() on /proc/<pid>/cwd, which is a symlink to a
DIRECTORY, so open() raises IsADirectoryError; the except OSError branch then
discarded every candidate process including the real gcs_server. The arm
therefore never killed anything and never started measuring.

Retained because the nokill arm in the SAME run is a valid and clean
NC-outage control: work and durability both rose monotonically throughout
(exec 5->400, writes 49->401), 400 units committed, contiguous, no dupes,
one live actor row, epoch stayed 1, zero sampler errors. That control is
reused as the baseline for the repaired run.

Nothing may be concluded about C7 or C4 from this run.
