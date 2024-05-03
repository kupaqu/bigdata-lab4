import database
import yaml


with open('secrets.yaml') as f:
    secrets = yaml.safe_load(f)
db = database.Database(secrets=secrets)
db.create_database("lab2")
db.create_table("predictions", {'X': 'Array(Int32)', 'predictions': 'Int32'})