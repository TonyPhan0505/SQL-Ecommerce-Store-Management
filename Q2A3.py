############### Imports ###############
import sqlite3
import time
######################################


############################# Class ##############################
class Database:
    def __init__(self, database_name):
        self.database_name = database_name
        self.conn = sqlite3.connect(f"./{self.database_name}")
        self.cursor = self.conn.cursor()

    def close_database(self):
        self.conn.close()

    def reconnect_database(self):
        self.conn = sqlite3.connect(f"./{self.database_name}")
        self.cursor = self.conn.cursor()

    def save_database(self):
        self.conn.commit()

    def run_query(self, script):
        self.cursor.executescript(script)
        self.save_database()
    
    def fetch_one(self):
        return self.cursor.fetchone()
    
    def fetch_all(self):
        return self.cursor.fetchall()

    def uninformed(self):
        script = '''
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
        '''
        self.run_query(script)
    
    def self_optimized(self):
        script = '''
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
        '''
        self.run_query(script)
    
    def user_optimized(self):
        script = '''
            CREATE INDEX customer_postal_code_index
            ON Customers (customer_postal_code);

            CREATE INDEX seller_postal_code_index
            ON Sellers (seller_postal_code);
        '''
        self.run_query(script)
###################################################################


#####################Solution Functions############################
def solution(DATABASE):
    '''
    1. Create view
    2. Find average number of items in orders
    3. Find orders that have items more than the average number of items in the orders.
    '''
    script = f'''
    CREATE VIEW OrderSize AS
    SELECT order_id AS oid, COUNT(order_item_id) AS size
    FROM Order_items
    GROUP BY order_id;

    WITH avg_items AS (
    SELECT AVG(size) AS avg_size
    FROM OrderSize
    )
    SELECT COUNT(DISTINCT o.order_id)
    FROM Orders o
    JOIN OrderSize os ON o.order_id = os.oid
    JOIN avg_items ai
    WHERE o.customer_id IN (
    SELECT customer_id
    FROM Customers
    WHERE customer_postal_code = [random_customer_postal_code]
    )
    AND os.oid IN (
    SELECT order_id
    FROM Order_items
    GROUP BY order_id
    HAVING COUNT(*) > ai.avg_size
    );
    '''
    DATABASE.run_query(script)
    return DATABASE.fetch_one()

def run_solution(DATABASE):
    start_time = time.time()
    for _ in range(50):
        result = solution(DATABASE)
        print('Number of orders that have items more than the average number of items in the orders =', result)
    end_time = time.time()
    return end_time - start_time

def run_Scenarios(DATABASE):
    print("-- Uninformed Scenario:")
    DATABASE.uninformed()
    run_solution(DATABASE)
    DATABASE.close_database()
    DATABASE.reconnect_database()
    print("-- Self-optimized Scenario:")
    DATABASE.self_optimized()
    run_solution(DATABASE)
    DATABASE.close_database()
    DATABASE.reconnect_database()
    print("-- User-optimized Scenario")
    DATABASE.user_optimized()
    run_solution(DATABASE)
    DATABASE.close_database()
    
###################################################################


############################# Main ##############################
if __name__ == "__main__":
    ##### Initialization #####
    A3Small = Database('A3Small.db')
    A3Medium = Database('A3Medium.db')
    A3Large = Database('A3Large.db')
    print("-------------------- Q2: --------------------")

    ##### Run Scenarios #####
    print('- A3Small:')
    run_Scenarios(A3Small)
    print('- A3Medium:')
    run_Scenarios(A3Medium)
    print('- A3Large:')
    run_Scenarios(A3Large)

    ##### Termination #####
    print("-------------------- Finished --------------------")
################################################################