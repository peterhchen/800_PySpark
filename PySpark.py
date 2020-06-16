from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.json ("data/people.json")
df.filter("age > 21").select ("name", "age").show()
df.createOrReplaceTempView("people")
print('spark.sql("SELECT * FROM people where age > 21").show():')
print(spark.sql("SELECT * FROM people where age > 21").show())
