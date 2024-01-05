from pyspark import SparkConf
from pyspark.sql import SparkSession
from load import load_data
from extract import extract_data
import argparse
from pyspark.sql import Row



def get_spark_utils():
    """
    !!!DO NOT TOUCH!!!
    This function returns spark context object and spark session object.
    These are the entry point into all functionality in Spark.
    :return: SparkContext, SparkSession
    """
    conf = SparkConf().setAppName("Covid"). \
        set("spark.mongodb.input.uri", "mongodb://127.0.0.1"). \
        set("spark.mongodb.output.uri", "mongodb://127.0.0.1"). \
        set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"). \
        set("spark.sql.debug.maxToStringFields", 1000)
    spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    return sc, spark



if __name__ == '__main__':
    """
    This is your main function and this contains your flow.get_spark_utils function provide
    necessary variables for you like spark context.You should 
    call extract, transform?? and load functions respectively from their modules.
    
    Hint**: You may convert extracted data to RDD after that convert it to Dataframe.
    
    """

    sc, spark = get_spark_utils()

    # Extract data
    ind_file = 'WLD_2023_SYNTH-SVY-IND-EN_v01_M.csv'
    hld_file = 'WLD_2023_SYNTH-SVY-HLD-EN_v01_M.csv'
    ind_data, hld_data = extract_data(ind_file, hld_file)

    inv_rdd = spark.sparkContext.parallelize(ind_data)
    hld_rdd = spark.sparkContext.parallelize(hld_data)
    print(inv_rdd.collect())
    print(hld_rdd.collect())

    # Stop the SparkContext
    sc.stop()


	
