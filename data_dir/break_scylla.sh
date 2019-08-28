#! /bin/bash

KEYSPACE=$1
n=3    # Number of file sets to delete
s=1000 # Size of Data files to delete
c=0    # Counter to avoid infinite loop

files=`find /var/lib/scylla/data/$KEYSPACE/*/ -maxdepth 1 -type f -size "+"$s"M" -exec ls -latr {} + | head -$n | awk '{print$9}' | cut -f 1,2,3,4,5 -d '-'`
echo "Chosen files are: $files"
while [ "$files" = "" ] && [ $c -lt 10 ]
do
        echo "Empty!, looking for files half the size"
        s=$((s / 2))
        echo "size to find is:" $s
        files=`find /var/lib/scylla/data/$KEYSPACE/*/ -maxdepth 1 -type f -size "+"$s"M" -exec ls -latr {} + | head -$n | awk '{print$9}' | cut -f 1,2,3,4,5 -d '-'`
	echo "Chosen files are: $files"
        c=$((c+1))
        if [ $c -eq 10 ]
        then
                echo "Max retries reached - exiting"
        fi
done

for f in $files
do
    # Changed because of name format seems was changed.
    # And now there is no db files named as ".db-*"
    f="$f*"

	echo "About to delete $f"
        rm -f $f
        echo "Deletion exit code is" $?
done
