# Import Libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf,monotonically_increasing_id
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
country = {"US": "USA", "GB": "Great Britain", "DE": "Germany", "CA": "canada", "FR": "France","RU": "Russia", "MX": "Mexico", "KR": "South Korea", "JP": "Japan", "IN": "India", }


def proccess_category_data(spark):
    """_summary_
        - Read Category File Data
        - Split Data Bazed On ":"
        - Create New Two Columns Contain Category_id Category_name
        - Delete Withe Space And Convert Cloumn To Integer and String Type and Add New Alias Name
        - Select Data And Reorganiz Data
        - Write Data To Hive Table
    Args:
        spark (Object): Spark Object
    """
    # Read Category File Data
    cat_data = spark.read.csv("hdfs:///user/maria_dev/youtuob/category_file.txt", header=True)
    # Split Data Bazed On ":"
    cat_df = cat_data.withColumn("cat", F.split("Category_id      Category_name", ":"))
    # Create New Two Columns Contain Category_id Category_name
    cat_df = cat_df.withColumn("cat_id", cat_df.cat[0]).withColumn("cat_name", cat_df.cat[1])
    # Delete Withe Space And Convert Cloumn To Integer and String Type and Add New Alias Name
    cat_data = cat_df.select(F.trim(F.col("cat_id")).cast("int").alias("category_id"),F.trim(F.col("cat_name")).cast("string").alias("category_name"))
    # Select Data And Reorganiz Data
    cat_data = cat_data.select("category_id", "category_name").orderBy(F.asc("category_id"))
    # Write Data To Hive Table
    cat_data.write.format("hive").saveAsTable('youtuob.youtuob_category')


def get_country_name(file_path):
    """_summary_
        - Return Country Name Bazed On Country Key
    Args:
        file_path (String): File Path Name Contain File Name
    Returns:
        String: Country Name
    """
    # Split File Path  And Get The File Name
    # File Path "|hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/youtuob/RUvideos.csv"
    file_name = file_path.split('/')[-1]
    # Get Country Key From File Name
    country_key = file_name[:2]
    # Get Country Name Bazed on Country Key From Country Variable
    for key in country:
        # Country Name
        country_name = country[key]
    return country_name


def proccess_channel_data(YTData):
    """_summary_
        - Proccess Channel Data 
        - Select Channel Data
        - Add Channel id Column
    Args:
        YTData (Object): Youtuob Data 
    """
    # Select Channel Title
    channel_data = YTData.select("channel_title", "country").distinct()
    # Add chennal_id Column And Generate Id
    channel_data=channel_data.withColumn("channel_id", monotonically_increasing_id())
    # Reorganize Data
    channel_data = channel_data.select("channel_id", "channel_title", "country")
    # Write Data To Table ON Hive
    channel_data.write.partitionBy("country").format('hive').saveAsTable("youtuob.channel")


def proccess_videos_data(YTData):
    """_summary_
        - Select Videos Data
        - Write Data To Hive Table
    Args:
        YTData (Object): Youtuob Data 
    """
    # Select Videos Data
    video_data = YTData.select("video_id",'title', 'thumbnail_link', 'comments_disabled','ratings_disabled', 'video_error_or_removed', 'description')
    # Write Data To Hive Table
    video_data.write.format("hive").saveAsTable("youtuob.videos")


def proccess_puplish_time(YTData):
    """_summary_
         - Extract time columns from publish_time column
    Args:
         YTData (Object): Youtuob Data 
    """
    time = YTData.select("publish_time").withColumn('hour', F.hour('publish_time')).withColumn("minute", F.minute('publish_time')).withColumn("second", F.minute('publish_time')).withColumn('day', F.dayofmonth('publish_time')).withColumn('week', F.weekofyear('publish_time')).withColumn('month', F.month('publish_time')).withColumn('year', F.year('publish_time')).withColumn('weekday', F.date_format("publish_time", "E"))
    # Write Data To Time Table
    time.write.format("hive").saveAsTable("youtuob.publish_time")


def proccess_videos_tags(YTData):
    """_summary_
        - Select Video_id With Tags
        - fIlter Tags
        -  Delete Two First Char From Tag
        - Select Video_id and new_tag as tag
        - Write Tags with video_idf
    Args:
         YTData (Object): Youtuob Data 
    """
    # Select Video_id With Tags
    tag = YTData.select("video_id",F.explode(F.split("tags", "\\|.*$")).alias("tag"))
    # fIlter Tags
    tag = tag.filter((tag['tag'] != "") & (tag['tag'] != "[none]"))
    # Delete Two First Char From Tag
    tag = tag.withColumn('new_tag',F.expr("substring(tag, 2, length(tag))"))
    # Select Video_id and new_tag as tag
    tag = tag.selectExpr("video_id", "new_tag as tag")
    # Write Tags with video_idf
    tag.write.format("hive").saveAsTable("youtuob.video_tag")


def proccess_tags(spark):
    """_summary_
      - Write Tags Data To Tags Tale
      - Select Distinct Tags from tag_video Table
    Args:
        spark (Oject): Spark Object
    """
    # Select Distinct Tags from tag_video Table
    tags = spark.sql("SELECT DISTINCT tag FROM youtuob.video_tag")
    # Write Tags Data To Tags Tale
    tags.write.format("hive").saveAsTable("youtuob.tags")


def proccess_videos_fact_table(spark, YTData):
    """_summary_
        - Select Channel Data To Make Join To Get Channel ID
        - Join Data To Get Channel ID
        - Select Fact Data
        - Write Data To Fact Tale
    Args:
         spark (Oject): Spark Object
         YTData (Object): Youtuob Data
    """
    # Select Channel Data To Make Join To Get Channel ID
    channel_Data = spark.sql("SELECT channel_id , channel_title FROM youtuob.channel")
    # Join Data To Get Channel ID
    fact_data = YTData.join(channel_Data, channel_Data['channel_title'] == YTData['channel_title'], 'inner')
    # Select Fact Data
    fact_data = fact_data.select('video_id', 'channel.channel_title','trending_date', 'publish_time', 'views', 'likes', 'dislikes', 'comment_count')
    # Write Data To Fact Tale
    fact_data.write.format("hive").saveAsTable("youtuob.videos_fact")


def main():
    # Create New Spark Object With Hive Support
    spark = (SparkSession.builder
             .appName("Youtuob Data Warehouse Spark and Hive")
             .enableHiveSupport()
             .getOrCreate())
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    # Read All CSV File Data
    YoutuobDATA = spark.read.csv('hdfs:///user/maria_dev/youtuob/*.csv', header=True).withColumn('file', F.input_file_name())
    # Convert get_country_name Function To Use With Spark
    country_name = udf(get_country_name)
    # Add New Column Contain Country Name
    YoutuobDATA = YoutuobDATA.withColumn("country", country_name(YoutuobDATA['file']))

    print("Start Proccess and Load Category Data to youtuob_category Table...")
    proccess_category_data(spark)

    print("Start Proccess and Load channel Data to Channel Table...")
    proccess_channel_data(YoutuobDATA)

    print("Start Proccess and Load videos Data to videos Table...")
    proccess_videos_data(YoutuobDATA)

    print("Start Proccess and Load publish_time Data to publish_time Table...")
    proccess_puplish_time(YoutuobDATA)

    print("Start Proccess and Load video_tag Data to video_tag Table...")
    proccess_videos_tags(YoutuobDATA)

    print("Start Proccess and Load tags Data to tags Table...")
    proccess_tags(spark)

    print("Start Proccess and Load videos_fact Data to videos_fact Table...")
    proccess_videos_fact_table(spark,YoutuobDATA)

   

if __name__ == "__main__":
    main()