import sqlalchemy as sa
import psycopg2
from io import open
import pandas as pd
import csv


class Transaction:
  def __init__(self , date, customerId, transactionId, productCategoryId, SKU, quantity, salesAmount,salesPerson ):
    self.date = date
    self.customerId = customerId
    self.transactionId = transactionId
    self.productCategoryId = productCategoryId
    self.SKU = SKU
    self.quantity = quantity
    self.salesAmount = salesAmount
    self.salesPerson = salesPerson

    
  def __str__(self):
     return f'{self.date}, {self.customerId}, {self.transactionId}, {self.productCategoryId}, {self.SKU}, {self.quantity}, {self.salesAmount}';
     
constring: sa.engine.url.URL = sa.engine.URL.create(
    drivername="postgresql",
    username="postgres",
    password="postgres",
    host="localhost",
    port=5432,
    database="Megamart_Data"
)

dbEngine= sa.create_engine(
    url=constring,

)

try:
    with  psycopg2.connect("dbname=Megamart_Data user=postgres") as cur:
        conn = cur.cursor()
        conn.execute("""SELECT sales_person_id, sum(sales_amount)
                        FROM salesperson
                        JOIN transaction_data td on salesperson.product_category_id = td.product_category_id
                        GROUP BY sales_person_id
                        ORDER BY sum(sales_amount) DESC""")
        records = conn.fetchall();

        print("Print each row and it's columns values")
        
        listOfTransactions = []
        
        # 1. Open a new CSV file
        with open('sales_amount_per_person.csv', 'w', newline='') as file:
        # 2. Create a CSV writer
         writer = csv.writer(file)
        # 3. Write data to the file
        
         sales_amount_per_person_header = ['sales_person_id', 'sum(sales_amount)']
         writer.writerow(sales_amount_per_person_header)
         for row in records:
            writer.writerow([row[0], row[1]])

                             
        print("Engine valid")
except Exception as e:
    print(f"Engine invalid: {e}")