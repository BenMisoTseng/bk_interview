# coding=UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# define aws elasticsearch hostname
es_host = 'https://search-cathaybk-interview-u26i3vipab4ngibgxt4v7kud6a.ap-northeast-1.es.amazonaws.com'

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
city_raw_df = taipei_city_df.union(taichung_city_df) \
                            .union(kaohsiung_city_df) \
                            .union(newtaipei_city_df) \
                            .union(taoyuan_city_df) \
                            .where(~col("交易年月日").like("%year%"))

# get all column name for sql select
cols =  list(set(city_raw_df.columns))

# parse date time and turn the year of "Republic Era" to "A.D."
# then make date column to string data type for elasticsearch date detection
# and change some column name which are assigned in examination question
city_df = city_raw_df.withColumn('date', to_date(((col('交易年月日') + 19110000).cast('int')).cast('string'),'yyyyMMdd').cast('string')) \
                     .select([col('交易標的').alias('transaction_sign'), \
                              col('鄉鎮市區').alias('district'), \
                              col('建物型態').alias('building_state'), \
                              col('date')] + cols)

# write data to index "cathaybk-interview" with type name "land_data" 
# in elasticsearch in cloud cluster with ssl type
city_df.write.format("org.elasticsearch.spark.sql") \
       .option("es.nodes.wan.only", "true") \
       .option("es.net.ssl","true") \
       .option("es.nodes", es_host) \
       .option("es.port","443") \
       .mode("Overwrite") \
       .save("cathaybk-interview/land_data")


