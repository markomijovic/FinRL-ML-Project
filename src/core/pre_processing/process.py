import pandas as pd
from pyspark.sql import SparkSession


def process_pandas(pandasDF):
    pandasDF = remove_adj_close(pandasDF)
    pandasDF = filter_low_volume(pandasDF)
    pandasDF = label_data(pandasDF)
    return pandasDF


def filter_low_volume(pandasDF):
    avgVol=  pandasDF["Volume"].mean()
    pandasDF = pandasDF[pandasDF["Volume"] > avgVol * 0.25]
    return pandasDF


def label_data(pandasDF):
    pandasDF["Label"] = (pandasDF["Close"] - pandasDF["Open"]).apply(lambda x: "P" if x > 0 else "N")
    return pandasDF


def remove_adj_close(pandasDF):
    pandasDF = pandasDF.drop(columns=["Adj Close"])
    return pandasDF


def create_spark_session():
    spark = SparkSession.builder.master(
        "local[1]").appName("ENSF612Project").getOrCreate()
    return spark


def pandas_to_spark(pandasDF):
    spark = create_spark_session()
    sparkDF = spark.createDataFrame(pandasDF)
    return sparkDF


if __name__ == "__main__":
    process_pandas(None)
