import sqlite3

class Database:
    def __init__(self, database_name):
        self.conn = sqlite3.connect(f"./{database_name}")
        self.cursor = self.conn.cursor()
    
    def close_database(self):
        self.conn.close()
    

if __name__ == "__main__":
    print("----- Done -----")