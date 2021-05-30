from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ =="__main__":
    conf = get_spark_app_config()
    print("Starting Hello Spark.")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting HelloSpark")

    conf_out = spark.sparkContext.getConf()

    # Print out all the configuration
    logger.info(conf_out.toDebugString())

    logger.info("Finished HelloSpark")

    spark.stop()