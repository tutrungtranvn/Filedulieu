#!/usr/bin/env bash
if [ -d "result" ]
then
	echo "Removing existing directory"
	rm -r "result"
	rm -r "result1"
	echo ""
else
	echo "Executing script"
	echo ""
fi
spark-shell -i ratingsdemo.scala
echo ""
echo ""
cat "result/part-00000"
cat result1/part*.csv
echo ""
