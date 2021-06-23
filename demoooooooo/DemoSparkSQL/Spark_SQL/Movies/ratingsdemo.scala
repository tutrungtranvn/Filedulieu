val movies_rdd=sc.textFile("../../Movielens/movies.dat")
val movies_DF=movies_rdd.toDF.createOrReplaceTempView("movies_view")
spark.sql(""" select
split(value,'::')[0] as movieid,
split(value,'::')[1] as moviename,
substring(split(value,'::')[1],length(split(value,'::')[1])-4,4) as year
from movies_view """).createOrReplaceTempView("movies");
//List nhung phim cu nhat
var result=spark.sql("Select * from movies m1 where m1.year=(Select min(m2.year) from movies m2)").repartition(1).rdd.saveAsTextFile("result")
//So luong phim moi nam
var result1=spark.sql("Select distinct(year),count(year) as Number_of_Movies from movies group by year")
result1.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("result1")
System.exit(0);
