import configparser
import os
import pandas as pd
import sys
import traceback

from logger import Logger

SHOW_LOG = True

class DataMaker():

    def __init__(self) -> None:
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.config = configparser.ConfigParser()
        
        self.project_path = os.getcwd()
        self.data_path = os.path.join(self.project_path, 'data')
        self.train_path = os.path.join(self.data_path, 'fashion-mnist_train.csv')
        self.test_path = os.path.join(self.data_path, 'fashion-mnist_test.csv')

        self.X_train_path = os.path.join(self.data_path, 'X_train.csv')
        self.X_test_path = os.path.join(self.data_path, 'X_test.csv')
        self.y_train_path = os.path.join(self.data_path, 'y_train.csv')
        self.y_test_path = os.path.join(self.data_path, 'y_test.csv')
        
        self.log.info("DataMaker is ready")

    def Xy_split(self) -> bool:
        train = pd.read_csv(self.train_path)
        test = pd.read_csv(self.test_path)

        y_train = train['label']
        X_train = train.drop('label', axis=1)
        y_train.to_csv(self.y_train_path)
        X_train.to_csv(self.X_train_path)

        y_test = test['label']
        X_test = test.drop('label', axis=1)
        y_test.to_csv(self.y_test_path)
        X_test.to_csv(self.X_test_path)

        if os.path.isfile(self.train_path) and os.path.isfile(self.test_path) \
            and os.path.isfile(self.X_train_path) and os.path.isfile(self.y_train_path) \
            and os.path.isfile(self.X_test_path) and os.path.isfile(self.y_test_path):
            
            self.config["DATA"] = {'train_path': self.train_path, 'test_path': self.test_path}
            self.log.info("Train test data is ready")

            self.config["SPLIT_DATA"] = {
                'X_train_path': self.X_train_path,
                'X_test_path': self.X_test_path,
                'y_train_path': self.y_train_path,
                'y_test_path': self.y_test_path,
            }
            with open('config.ini', 'w') as configfile:
                self.config.write(configfile)
            self.log.info("X y data is ready")

            return True
        else:
            self.log.error("Data is NOT ready")
            return False

if __name__ == "__main__":
    data_maker = DataMaker()
    data_maker.Xy_split()
