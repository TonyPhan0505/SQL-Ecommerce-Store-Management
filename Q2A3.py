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
            PRAGMA automatic_index = FALSE;

            ALTER TABLE Customers RENAME TO Old_Customers;
            ALTER TABLE Sellers RENAME TO Old_Sellers;
            ALTER TABLE Orders RENAME TO Old_Orders;
            ALTER TABLE Order_items RENAME TO Old_Order_items;

            CREATE TABLE Customers ( 
                customer_id TEXT,
                customer_postal_code INTEGER
            );
            INSERT INTO Customers SELECT * FROM Old_Customers;

            CREATE TABLE Sellers ( 
                seller_id TEXT,
                seller_postal_code INTEGER
            );
            INSERT INTO Sellers SELECT * FROM Old_Sellers;

            CREATE TABLE Orders ( 
                order_id TEXT,
                customer_id TEXT
            );
            INSERT INTO Orders SELECT * FROM Old_Orders;

            CREATE TABLE Order_items ( 
                order_id TEXT,
                order_item_id INTEGER,
                product_id TEXT,
                seller_id TEXT
            );
            INSERT INTO Order_items SELECT * FROM Old_Order_items;

            DROP TABLE Old_Order_items;
            DROP TABLE Old_Orders;
            DROP TABLE Old_Customers;
            DROP TABLE Old_Sellers;
        ''')
    
    def self_optimized(self):
        self.cursor.executescript('''
            PRAGMA automatic_index = TRUE;

            ALTER TABLE Customers RENAME TO Old_Customers;
            ALTER TABLE Sellers RENAME TO Old_Sellers;
            ALTER TABLE Orders RENAME TO Old_Orders;
            ALTER TABLE Order_items RENAME TO Old_Order_items;

            CREATE TABLE Customers ( 
                customer_id TEXT,
                customer_postal_code INTEGER,
                PRIMARY KEY(customer_id) 
            );
            INSERT INTO Customers SELECT * FROM Old_Customers;

            CREATE TABLE Sellers ( 
                seller_id TEXT,
                seller_postal_code INTEGER,
                PRIMARY KEY(seller_id) 
            );
            INSERT INTO Sellers SELECT * FROM Old_Sellers;

            CREATE TABLE Orders ( 
                order_id TEXT,
                customer_id TEXT,
                PRIMARY KEY(order_id),
                FOREIGN KEY(customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE
            );
            INSERT INTO Orders SELECT * FROM Old_Orders;

            CREATE TABLE Order_items ( 
                order_id TEXT,
                order_item_id INTEGER,
                product_id TEXT,
                seller_id TEXT,
                PRIMARY KEY(order_id,order_item_id,product_id,seller_id), 
                FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id) ON DELETE CASCADE, 
                FOREIGN KEY(order_id) REFERENCES Orders(order_id) ON DELETE CASCADE
            );
            INSERT INTO Order_items SELECT * FROM Old_Order_items;

            DROP TABLE Old_Order_items;
            DROP TABLE Old_Orders;
            DROP TABLE Old_Customers;
            DROP TABLE Old_Sellers;
        ''')
    
    def user_optimized(self):
        self.cursor.executescript('''
            CREATE INDEX customer_postal_code_index
            ON Customers (customer_postal_code);

            CREATE INDEX seller_postal_code_index
            ON Sellers (seller_postal_code);
        ''')
###################################################################


############################# Main ##############################
if __name__ == "__main__":
    print("----- Done -----")
################################################################

# --Create view
# CREATE VIEW OrderSize AS
# SELECT order_id AS oid, COUNT(item_id) AS size
# FROM order_items
# GROUP BY order_id;

# --Orders that have items more than the average number of items in the orders.
# WITH avg_items AS (
#   SELECT AVG(size) AS avg_size
#   FROM OrderSize
# )
# SELECT COUNT(DISTINCT o.order_id)
# FROM orders o
# JOIN OrderSize os ON o.order_id = os.oid
# JOIN avg_items ai
# WHERE o.customer_id IN (
#   SELECT customer_id
#   FROM Customers
#   WHERE customer_postal_code = [random_customer_postal_code]
# )
# AND os.oid IN (
#   SELECT order_id
#   FROM order_items
#   GROUP BY order_id
#   HAVING COUNT(*) > ai.avg_size
# );


# A3Small.db
uniformed_A3Small = Database("A3Small.db")
uniformed_A3Small.uninformed()

self_A3Small = Database("A3Small.db")
self_A3Small.self_optimized()

user_A3Small = Database("A3Small.db")
user_A3Small.user_optimized()

# A3Medium.db
A3Medium = Database("A3Medium.db")

# A3Large.db
A3Large = Database("A3Large.db")