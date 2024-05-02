import database

db = database.Database()
db.create_database("lab2")
db.create_table("predictions", {'X': 'Array(Int32)', 'predictions': 'Int32'})