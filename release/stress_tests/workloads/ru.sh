set -e

for i in {1..100}
do
  python ./test_placement_group.py
##  python ~/ray/release/stress_tests/workloads/test_placement_group2.py
done

#ray stop  
#python ~/test/test_placement_group1.py
