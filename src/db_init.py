import database
import yaml
import os

project_path = os.getcwd()
with open(os.path.join(project_path, 'secrets.yml')) as f:
    secrets = yaml.safe_load(f)
# print('='*10)
# print(secrets)
# print('='*10)
db = database.Database(secrets=secrets)
db.create_database("lab2")
db.create_table("predictions", {'X': 'Array(Int32)', 'predictions': 'Int32'})