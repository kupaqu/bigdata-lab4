import configparser
import os
import pandas as pd
import pickle
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
import sys
import traceback

from logger import Logger

SHOW_LOG = True

class MultiModel():

    def __init__(self) -> None:
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.config = configparser.ConfigParser()
        self.config.read("config.ini")
        
        self.X_train = pd.read_csv(
            self.config["SPLIT_DATA"]["X_train_path"], index_col=0)
        self.y_train = pd.read_csv(
            self.config["SPLIT_DATA"]["y_train_path"], index_col=0)['label']
        self.X_test = pd.read_csv(
            self.config["SPLIT_DATA"]["X_test_path"], index_col=0)
        self.y_test = pd.read_csv(
            self.config["SPLIT_DATA"]["y_test_path"], index_col=0)['label']
        
        self.project_path = os.getcwd()
        self.experiments_path = os.path.join(self.project_path, 'experiments')
        self.models_path = os.path.join(self.project_path, 'models')
        os.makedirs(self.models_path, exist_ok=True)
        self.lr_path = os.path.join(self.models_path, 'lr.sav')

        self.log.info("MultiModel is ready")

    def lr(self, predict=False) -> bool:
        classifier = LogisticRegression()
        try:
            classifier.fit(self.X_train, self.y_train)
        except Exception:
            self.log.error(traceback.format_exc())
            sys.exit(1)
        if predict:
            y_pred = classifier.predict(self.X_test)
            print(f'Accuracy: {accuracy_score(self.y_test, y_pred)}')
        params = {'path': self.lr_path}
        return self.save_model(classifier, self.lr_path, "LR", params)

    def save_model(self, classifier, path: str, name: str, params: dict) -> bool:
        self.config[name] = params
        os.remove('config.ini')
        with open('config.ini', 'w') as configfile:
            self.config.write(configfile)
        with open(path, 'wb') as f:
            pickle.dump(classifier, f)

        self.log.info(f'{path} is saved')
        return os.path.isfile(path)

if __name__ == "__main__":
    multi_model = MultiModel()
    multi_model.lr()
