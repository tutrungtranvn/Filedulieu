#!/usr/bin/env bash
if [ -d "result" ]
then
	echo "Removing existing directory"
	rm -r "result"
	rm -r "result1"
	rm -r "result2"
	echo ""
else
	echo "Executing script"
	echo ""
fi
spark-shell -i test.scala
echo ""
echo ""
echo "Nhung phim co nguoi dung danh gia nhieu nhat"
cat result/part*.csv
echo "Co bao nhieu phim cho moi muc xep hang?"
cat result1/part*.csv
echo "Nhung phim co diem Rating cao nhat"
cat result2/part*.csv
echo ""
