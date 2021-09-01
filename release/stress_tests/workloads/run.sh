set -e

for i in {1..100}
do
  python ./test_placement_group.py
done

#ray stop  
#python ~/test/test_placement_group1.py
