from pyspark.sql import *
from lib.logger import Log4J
if __name__ =="__main__":
    print("Starting Hello Spark.")
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting HelloSpark")

    logger.info("Finished HelloSpark")

    spark.stop()