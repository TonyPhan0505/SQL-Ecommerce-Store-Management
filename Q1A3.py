############### Imports ###############
import sqlite3
######################################


############################# Class ##############################
class Database:
    def __init__(self, database_name):
        self.conn = sqlite3.connect(f"./{database_name}")
        self.cursor = self.conn.cursor()
    
    def close_database(self):
        self.conn.close()

    def save_database(self):
        self.conn.commit()

    def uninformed(self):
        self.cursor.executescript('''
            ALTER TABLE Customers
            DROP PRIMARY KEY;

            ALTER TABLE Sellers
            DROP PRIMARY KEY;

            ALTER TABLE Order_items
            DROP PRIMARY KEY;

            ALTER TABLE Orders
            DROP PRIMARY KEY;
        ''')
###################################################################


############################# Main ##############################
if __name__ == "__main__":
    print("----- Done -----")
################################################################