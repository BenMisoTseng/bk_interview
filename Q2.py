# coding=UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create a spark session
spark = SparkSession.builder \
         .appName("land_data") \
         .getOrCreate()

# read csv data to spark and use chinese header in csv as dataframe column name
taipei_raw_df = spark.read.csv('file:///land_data/a_lvr_land_a.csv', header=True)
taichung_raw_df = spark.read.csv('file:///land_data/b_lvr_land_a.csv', header=True)
kaohsiung_raw_df = spark.read.csv('file:///land_data/e_lvr_land_a.csv', header=True)
newtaipei_raw_df = spark.read.csv('file:///land_data/f_lvr_land_a.csv', header=True)
taoyuan_raw_df = spark.read.csv('file:///land_data/h_lvr_land_a.csv', header=True)

# add column with city name
taipei_city_df = taipei_raw_df.withColumn('city', lit('台北市'))
taichung_city_df = taichung_raw_df.withColumn('city', lit('台中市'))
kaohsiung_city_df = kaohsiung_raw_df.withColumn('city', lit('高雄市'))
newtaipei_city_df = newtaipei_raw_df.withColumn('city', lit('新北市'))
taoyuan_city_df = taoyuan_raw_df.withColumn('city', lit('桃園市'))

# combine different city dataframe to one and delete header in english
city_df = taipei_city_df.union(taichung_city_df) \
                        .union(kaohsiung_city_df) \
                        .union(newtaipei_city_df) \
                        .union(taoyuan_city_df) \
                        .where(~col("交易年月日").like("%year%"))

# get column we need and change name
select_df = city_df.select("city", col("交易年月日").alias("date"), \
                                   col("鄉鎮市區").alias("district"), \
                                   col("建物型態").alias("building_state") \
                          )

# create a tmp table with table name for spark sql
select_df.createOrReplaceTempView('land_data')

# use spark sql to get a nested json with list
group_df =  spark.sql("""
    SELECT city, collect_list(struct(date, events)) as time_slots 
    FROM (
        SELECT city, date, collect_list(struct(district, building_state)) as events 
        FROM land_data 
        GROUP BY city, date
        ORDER BY city, date DESC
    ) as tmp 
    GROUP BY city
""")

# random split data in two
result_df = group_df.randomSplit([1.0,1.0])

# write two result data in folder
result_df[0].coalesce(1).write.format('json').save('file:///land_data/result_part1')
result_df[1].coalesce(1).write.format('json').save('file:///land_data/result_part2')

# get json file and rename to the name which are assigned in examination question
result_part1 = fnmatch.filter(os.listdir('/land_data/result_part1/'), '*.json')[0]
result_part2 = fnmatch.filter(os.listdir('/land_data/result_part2/'), '*.json')[0]
os.rename('/land_data/result_part1/' + result_part1,'/land_data/result_part1.json')
os.rename('/land_data/result_part2/' + result_part2,'/land_data/result_part2.json')


