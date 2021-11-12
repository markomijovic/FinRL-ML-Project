from core.data_aquisition import load_api, load_file
from core.pre_processing.process import *
from core.feature_engineering.features import *


class ManagementUtility:
    def __init__(self, ticker) -> None:
        self.ticker = ticker

    def execute(self):
        pandasDF = load_file.load_csv(self.ticker)
        pandasDF = process_pandas(pandasDF)
        sparkDF = pandas_to_spark(pandasDF)
        sparkDF = engineer_new_features(sparkDF)


def execute_from_util():
    utility = ManagementUtility("GOOGL")
    utility.execute()
