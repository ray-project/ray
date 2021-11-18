#! /bin/bash

BASE_DIR=/home/ubuntu/workspace/ray/OSDI22
LOG_DIR=data
LOG_PATH=$BASE_DIR/$LOG_DIR

function shuffleTest()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 #8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				python test_shuffle.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

function pipelineTest()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 #8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				python test_pipeline.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

function pipelineTest()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 #8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				python test_pipeline.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

function 1Test()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 #8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				RAY_object_spilling_threshold=0.8 RAY_block_tasks_threshold=0.8 RAY_enable_BlockTasks=true python shuffle.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

function 2Test()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 #8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				RAY_enable_BlockandEvictTasks=true python shuffle.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

pushd $BASE_DIR
mkdir -p $LOG_DIR
popd

# Pipeline Test
#./../script/install_scheduler_ray.sh
#pipelineTest $LOG_PATH/pipeline_scheduled.csv
#./../script/install_production_ray.sh
#pipelineTest $LOG_PATH/pipeline_production.csv

#Shuffle Test
#shuffleTest $LOG_PATH/shuffle_production.csv
#./../script/install_scheduler_ray.sh
#shuffleTest $LOG_PATH/shuffle_scheduled.csv

1Test $LOG_PATH/shuffle_block80.csv
2Test $LOG_PATH/shuffle_blockevict.csv
