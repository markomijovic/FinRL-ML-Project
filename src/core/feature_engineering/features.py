import datetime
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType, StringType, DoubleType

HIGH = "High"
LOW = "Low"
OPEN = "Open"
CLOSE = "Close"
VOLUME = "Volume"
DATE = "Date"


@udf(DoubleType())
def range(h,l):
    return h - l


@udf(DoubleType())
def daily_movement(o,c):
    return c - o


@udf(DoubleType())
def hlm_divv(m,v):
    return m /v


@udf(DoubleType())
def dailym_divv(m,v):
    return m /v

def engineer_new_features(sparkDF):
    sdf = sparkDF.select("*",
                         range(HIGH, LOW).alias("Range"),
                         daily_movement(OPEN, CLOSE).alias("dDay"),
                         ).sort("Date")
    sdf = sdf.select("*", hlm_divv("Range", CLOSE).alias("Range/Close"),
                     dailym_divv("dDay", CLOSE).alias("dDay/Close")
                     )
    return sdf