import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

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

    # Force it to repartition into two since local machine only have 1 partition
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    logger.info(count_df.collect())

    # Pause the program so that we can see the UI in localhost:4040
    input("Press Enter")

    logger.info("Finished HelloSpark")

    spark.stop()