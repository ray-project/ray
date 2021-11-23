#! /bin/bash

BASE_DIR=/home/ubuntu/ray/OSDI22
LOG_DIR=data
LOG_PATH=$BASE_DIR/$LOG_DIR


function BaseRuns()
{
	TEST_FILE=$1
	RESULT_FILE=$2
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	echo "base_std, ray_std, base_var, ray_var, working_set_ratio, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$RESULT_FILE
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 #8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "$TEST_FILE -w $w -o $o -os $os\n"
				python $TEST_FILE -w $w -o $o -os $os -r $RESULT_FILE 
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

function ParallelShuffleTest()
{
	BaseRuns test_parallel_shuffle.py $1
}

function ScatterGatherTest()
{
	BaseRuns test_scatter_gather.py $1
}

function ShuffleTest()
{
	RESULT_FILE=$1
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	echo "base_std, ray_std, base_var, ray_var, working_set_ratio, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$RESULT_FILE
	for w in 1 2 
	do
		for o in 1000000000 4000000000 
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_shuffle.py -w $w -o $o -os $os\n"
				python test_shuffle.py -w $w -o $o -os $os -r $RESULT_FILE 
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

function PipelineTest()
{
	RESULT_FILE=$1
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	echo "base_std, ray_std, base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$RESULT_FILE
	for w in 1 2 4 8 
	do
		for ns in 1 2 4
		do
			for o in 1000000000 4000000000 
			do
				for ((os=10000000; os<=160000000; os *= 2))
				do
					echo -n -e "test_pipeline.py -w $w -o $o -os $os -ns $ns\n"
					python test_pipeline.py -w $w -o $o -os $os -r $RESULT_FILE -ns $ns
					rm -rf /tmp/ray/session_2*
				done
			done
		done
	done
}

pushd $BASE_DIR
mkdir -p $LOG_DIR
popd

#production
PipelineTest $LOG_PATH/pipeline_production.csv
ShuffleTest $LOG_PATH/shuffle_production.csv
ScatterGatherTest $LOG_PATH/scatter_gather_production.csv
ParallelShuffleTest $LOG_PATH/parallel_shuffle_production.csv

#Scheduled
./../scripts/install_scheduler_ray.sh
PipelineTest $LOG_PATH/pipeline_scheduled.csv
ShuffleTest $LOG_PATH/shuffle_scheduled.csv
ScatterGatherTest $LOG_PATH/scatter_gather_scheduled.csv
ParallelShuffleTest $LOG_PATH/parallel_shuffle_scheduled.csv
