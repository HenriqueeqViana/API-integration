import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql  import Window

spark = SparkSession.builder \
    .appName("E-Rate Analysis") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


csv_file_path = 'validation/E-Rate_FCC_Form_470_Tool_Data_20250207.csv'


df = spark.read.csv(csv_file_path, header=True, inferSchema=True)




df = df.filter(F.col('Form Version').isin(['Current', 'Original']))


window_spec = Window.partitionBy('Application Number').orderBy(F.when(F.col('Form Version') == 'Current', 1).otherwise(2))


df = df.withColumn('rank', F.row_number().over(window_spec))


df = df.filter(F.col('rank') == 1).drop('rank')
df_filtered = df   



total_files = df_filtered.count()

distinct_count = df_filtered.select('Application Number').distinct().count()


df_filtered = df_filtered.withColumn('month', F.month(F.col('Certified Date/Time')))


monthly_summary = df_filtered.groupBy('month').agg(
    F.round(F.avg('Quantity'), 2).alias('avg_quantity')
)


monthly_summary = monthly_summary.withColumn('month_name', 
    F.expr("CASE month " +
    "WHEN 1 THEN 'January' " +
    "WHEN 2 THEN 'February' " +
    "WHEN 3 THEN 'March' " +
    "WHEN 4 THEN 'April' " +
    "WHEN 5 THEN 'May' " +
    "WHEN 6 THEN 'June' " +
    "WHEN 7 THEN 'July' " +
    "WHEN 8 THEN 'August' " +
    "WHEN 9 THEN 'September' " +
    "WHEN 10 THEN 'October' " +
    "WHEN 11 THEN 'November' " +
    "WHEN 12 THEN 'December' END"))


summary = {
    "ano": '2024',
    "distinct":distinct_count,
    "total_files_created": total_files,
    "rfps_analyzed": distinct_count,
}


monthly_counts = {row['month_name']: row['avg_quantity'] for row in monthly_summary.collect()}
summary.update(monthly_counts)


print("\nResumo:")
print(summary)

spark.stop()