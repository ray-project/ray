#! /bin/sh
echo "######## The script running... ########"
echo "input args are:"
for arg in "$@"
do
    echo "    " $arg
done
# Please add your shell
ray start "$@"
echo "########    End of sleep ...   ########"
sleep 1h
