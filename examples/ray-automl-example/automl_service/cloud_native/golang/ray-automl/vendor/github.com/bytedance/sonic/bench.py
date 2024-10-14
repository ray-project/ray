#!/usr/bin/env python3

# Copyright 2022 ByteDance Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tempfile
import os
import subprocess
import argparse

gbench_prefix = "SONIC_NO_ASYNC_GC=1 go test -benchmem -run=none "

def run(cmd):
    print(cmd)
    if os.system(cmd):
        print ("Failed to run cmd: %s"%(cmd))
        exit(1)

def run_s(cmd):
    print (cmd)
    try:
        res = os.popen(cmd)
    except subprocess.CalledProcessError as e:
        if e.returncode:
            print (e.output)
            exit(1)
    return res.read()

def run_r(cmd):
    print (cmd)
    try:
        cmds = cmd.split(' ')
        data = subprocess.check_output(cmds, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        if e.returncode:
            print (e.output)
            exit(1)
    return data.decode("utf-8") 

def compare(args):
    # detech current branch.
    # result = run_r("git branch")
    current_branch = run_s("git status | head -n1 | sed 's/On branch //'")
    # for br in result.split('\n'):
    #     if br.startswith("* "):
    #         current_branch = br.lstrip('* ')
    #         break

    if not current_branch:
        print ("Failed to detech current branch")
        return None
    
    # get the current diff
    (fd, diff) = tempfile.mkstemp()
    run("git diff > %s"%diff)

    # early return if currrent is main branch.
    print ("Current branch: %s"%(current_branch))
    if current_branch == "main":
        print ("Cannot compare at the main branch.Please build a new branch")
        return None

    # benchmark current branch    
    (fd, target) = tempfile.mkstemp(".target.txt")
    run("%s %s ./... 2>&1 | tee %s" %(gbench_prefix, args, target))

    # trying to switch to the latest main branch
    run("git checkout -- .")
    if current_branch != "main":
        run("git checkout main")
    run("git pull --allow-unrelated-histories origin main")

    # benchmark main branch
    (fd, main) = tempfile.mkstemp(".main.txt")
    run("%s %s ./... 2>&1 | tee %s" %(gbench_prefix, args, main))

    # diff the result
    # benchstat = "go get golang.org/x/perf/cmd/benchstat && go install golang.org/x/perf/cmd/benchstat"
    run( "benchstat -sort=delta %s %s"%(main, target))
    run("git checkout -- .")

    # restore branch
    if current_branch != "main":
        run("git checkout %s"%(current_branch))
    run("patch -p1 < %s" % (diff))
    return target

def main():
    argparser = argparse.ArgumentParser(description='Tools to test the performance. Example: ./bench.py -b Decoder_Generic_Sonic -c')
    argparser.add_argument('-b', '--bench', dest='filter', required=False,
        help='Specify the filter for golang benchmark')
    argparser.add_argument('-c', '--compare', dest='compare', action='store_true', required=False,
        help='Compare with the main benchmarking')
    argparser.add_argument('-t', '--times', dest='times', required=False,
        help='benchmark the times')
    argparser.add_argument('-r', '--repeat_times', dest='count', required=False,
        help='benchmark the count')
    args = argparser.parse_args()
    
    if args.filter:
        gbench_args = "-bench=%s"%(args.filter)
    else:
        gbench_args = "-bench=."
        
    if args.times:
        gbench_args += " -benchtime=%s"%(args.times)
        
    if args.count:
        gbench_args += " -count=%s"%(args.count)
    else:
        gbench_args += " -count=10"

    if args.compare:
        target = compare(gbench_args)
    else:
        target = None

    if not target:
        (fd, target) = tempfile.mkstemp(".target.txt")
        run("%s %s ./... 2>&1 | tee %s" %(gbench_prefix, gbench_args, target))

if __name__ == "__main__":
    main()
