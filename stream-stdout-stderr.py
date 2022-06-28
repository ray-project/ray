#!/usr/bin/env python3

import sys
import subprocess
import select

process = subprocess.Popen(['./test-stream-stdout-stderr'])
process.wait()
print(process.communicate())
#process = subprocess.Popen(['./test-stream-stdout-stderr'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#while True:
#    timeout_seconds = 0.1
#    ready_reads, _, _ = select.select([process.stderr, process.stdout], [], [], timeout_seconds)
#    
#    #print(ready_reads)
#    rval = process.poll()
#    #print('rval', rval)
#    if rval != None:
#        break
#
#    all_empty = True
#    for stream in ready_reads:
#        read_one = stream.read1().decode('utf-8')
#
#        if read_one:
#            all_empty = False
#
#        if stream is process.stderr:
#            sys.stderr.write(read_one)
#        else:
#            sys.stdout.write(read_one)
#
#    #if all_empty:
#    #    print('all empty')
#
#    #stdout = process.stdout.read(1)
#    #if stdout == '' and process.poll() != None:
#    #    break
#    #if out != '':
#    #    sys.stdout.write(stdout)
#    #    sys.stdout.flush()
#
##proc.communicate()
#
