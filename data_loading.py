from pyspark.sql import SparkSession
spark = SparkSession.builder.\
         appName('Spark project').getOrCreate()


def Create_dataframe(Data,ishead):
    df = spark.read.csv(Data, header=ishead, inferSchema=True)
    return df