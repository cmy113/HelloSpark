import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df

if __name__ =="__main__":
    conf = get_spark_app_config()
    print("Starting Hello Spark.")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting HelloSpark")

    # # Print out all the configuration
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    if len(sys.argv) != 2:
        logger.error("usage: HelloSpark <filename>")
        sys.exit(-1)

    survey_df = load_survey_df(spark,sys.argv[1])
    survey_df.show()

    logger.info("Finished HelloSpark")

    spark.stop()