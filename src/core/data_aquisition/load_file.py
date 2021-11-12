from pyspark.sql import SparkSession
import pandas as pd

def load_csv(TICKER: str):
    df = pd.read_csv("./src/core/data_aquisition/data/"+TICKER+".csv")
    return df

if __name__ == "__main__":
    df = load_csv("GOOGL")