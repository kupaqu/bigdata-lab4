import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import clickhouse_connect
import pandas as pd
from typing import Dict
from logger import Logger

SHOW_LOG = True

class Database():
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.log.info("Connecting to ClickHouse...")
        # TODO: написать креды вручную
        host = os.getenv('DB_HOST')
        port = int(os.getenv('DB_PORT'))
        username = os.getenv('DB_USR')
        password = os.getenv('DB_PW')

        self.client = clickhouse_connect.get_client(host=host, username=username, port=port, password=password)
        self.log.info("Connected.")

    def create_database(self, database_name):
        self.client.command(f"""CREATE DATABASE IF NOT EXISTS {database_name};""")
        self.log.info(f"Created {database_name} database.")

    def create_table(self, table_name: str, columns: Dict):
        cols = ""
        for k, v in columns.items():
            cols += f"`{k}` {v}, "
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {table_name} 
            (
                {cols}
                `timestamp` DateTime('UTC') DEFAULT now(),
            ) ENGINE = MergeTree
            ORDER BY tuple();  -- No specific order needed
        """)
        self.log.info(f"Created {table_name} table.")

    def insert(self, tablename: str, X, predictions):
        try:
            X_values = X
            predictions_values = predictions

            query = f"INSERT INTO {tablename} (X, predictions, timestamp) VALUES (%s, %s, NOW())"

            self.client.command(query, (X_values, predictions_values))
            
            self.log.info(f"Inserted data into {tablename}")
        except Exception as e:
            self.log.info(f"Exception insert data into {tablename}: {e}")

    def read_table(self, tablename: str) -> pd.DataFrame:
        self.log.info(f"Reading table {tablename}...")
        return self.client.query_df(f'SELECT * FROM {tablename}')

    def table_exists(self, table_name: str):
        self.log.info(f"Checking if table {table_name} exists...")
        return self.client.query_df(f'EXISTS {table_name}')