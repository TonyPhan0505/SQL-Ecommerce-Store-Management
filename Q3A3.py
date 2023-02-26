############### Imports ###############
import sqlite3
import time
import matplotlib.pyplot as plt
import numpy as np
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

    def run_script_query(self, script):
        self.cursor.executescript(script)
        self.save_database()
    
    def run_single_query(self, query):
        self.cursor.execute(query)
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
        self.run_script_query(script)
    
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
        self.run_script_query(script)
    
    def user_optimized(self):
        script = '''
            CREATE INDEX customer_postal_code_index
            ON Customers (customer_postal_code);

            CREATE INDEX seller_postal_code_index
            ON Sellers (seller_postal_code);
        '''
        self.run_script_query(script)
###################################################################


######################## Solution Functions #######################
def solution(DATABASE, customer_postal_code):
    """
        1. Select the customers with the customer_postal_code in the Customers table
        2. Inner join the result with the Orders table based on customer_id
        3. Inner join the result with the Order_items table based on order_id
        4. Group the result by order_ids associated with more than the average order items
    """
    script = f'''
        SELECT Orders.order_id AS oid, COUNT(*) AS size
        FROM Customers, Orders, Order_items
        WHERE Customers.customer_postal_code = {customer_postal_code} 
            AND Orders.customer_id = Customers.customer_id
            AND Orders.order_id = Order_items.order_id
        GROUP BY (Orders.order_id)
        HAVING COUNT(*) > (
            SELECT AVG(s) FROM (
                SELECT COUNT(*) AS s
                FROM Orders, Order_items
                WHERE Orders.order_id = Order_items.order_id
                GROUP BY (Orders.order_id)
            )
        );
    '''
    DATABASE.run_single_query(script)
    return DATABASE.fetch_all()

def run_solution(DATABASE, customer_postal_code):
    start_time = time.time()
    for _ in range(50):
        result = solution(DATABASE, customer_postal_code)
        print(list(result))
    end_time = time.time()
    return (end_time - start_time) * (10**3)

def fill_weight_counts(scenario, weight_counts, time_taken):
    if weight_counts[scenario][0] == None:
        weight_counts[scenario][0] = time_taken
    elif weight_counts[scenario][1] == None:
        weight_counts[scenario][1] = time_taken
    else:
        weight_counts[scenario][2] = time_taken

def run_Scenarios(DATABASE, customer_postal_code, weight_counts):
    print("-- Uninformed Scenario:")
    DATABASE.uninformed()
    time_taken = run_solution(DATABASE, customer_postal_code)
    DATABASE.close_database()
    DATABASE.reconnect_database()
    print('Time taken =', time_taken)
    fill_weight_counts("Uninformed", weight_counts, time_taken)

    print("-- Self-optimized Scenario:")
    DATABASE.self_optimized()
    time_taken = run_solution(DATABASE, customer_postal_code)
    DATABASE.close_database()
    DATABASE.reconnect_database()
    print('Time taken =', time_taken)
    fill_weight_counts("Self-optimized", weight_counts, time_taken)

    print("-- User-optimized Scenario")
    DATABASE.user_optimized()
    time_taken = run_solution(DATABASE, customer_postal_code)
    DATABASE.close_database()
    print('Time taken =', time_taken)
    fill_weight_counts("User-optimized", weight_counts, time_taken)
##################################################################


####################### Chart Plotting Function #######################
def plot_chart(species, weight_counts, width, ax, bottom, title):
    for boolean, weight_count in weight_counts.items():
        p = ax.bar(species, weight_count, width, label=boolean, bottom=bottom)
        bottom += weight_count
    ax.set_title(title)
    ax.legend(loc="upper center")
    plt.show()
#######################################################################


############################# Main ##############################
if __name__ == "__main__":
    ##### Initialization #####
    A3Small = Database('A3Small.db')
    A3Medium = Database('A3Medium.db')
    A3Large = Database('A3Large.db')
    customer_postal_code = 14409
    species = (
        "SmallDB",
        "MediumDB",
        "LargeDB",
    )
    weight_counts = {
        "Uninformed": [None,None,None],
        "Self-optimized": [None,None,None],
        "User-optimized": [None,None,None]
    }
    width = 0.5
    fig, ax = plt.subplots()
    bottom = np.zeros(3)
    print("-------------------- Question 3: --------------------")

    ##### Run Scenarios #####
    print('- A3Small:')
    run_Scenarios(A3Small, customer_postal_code, weight_counts)
    print('\n- A3Medium:')
    run_Scenarios(A3Medium, customer_postal_code, weight_counts)
    print('\n- A3Large:')
    run_Scenarios(A3Large, customer_postal_code, weight_counts)

    ##### Termination #####
    plot_chart(species, weight_counts, width, ax, bottom, "Query 1 (runtime in ms)")
    print("-------------------- Finished --------------------\n")
################################################################