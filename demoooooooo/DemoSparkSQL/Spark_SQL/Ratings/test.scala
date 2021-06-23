// Chuyen RDD co san sang DF
val movies_rdd=sc.textFile("../../Movielens/ratings.dat")
// Tao mot view hoac 1 table de truy xuat du lieu
val movies_DF=movies_rdd.toDF.createOrReplaceTempView("ratings_view")
spark.sql("""Select
split(value,'::')[0] as userid,
split(value,'::')[1] as movieid,
split(value,'::')[2] as rating,
split(value,'::')[3] as timestamp
from ratings_view """).createOrReplaceTempView("ratings");
// Co bao nhieu phim cho moi muc xep hang?
var result1=spark.sql("""Select rating as Muc_xep_hang,count(rating) as So_luong_phim
from ratings 
group by rating
order by rating asc""")
result1.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("result")
// Nhung phim co nguoi dung danh gia nhieu
var result=spark.sql("""Select movieid, count(userid) as So_nguoi_danh_gia
from ratings 
group by movieid
order by cast(movieid as int) asc
""")
result.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("result1")
// Nhung phim co diem Rating cao?
var result2=spark.sql("""Select movieid,
sum(rating) as Tong_luot_danh_gia
from ratings 
group by movieid
order by cast(movieid as int) asc
""")
result2.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("result2")

System.exit(0);
