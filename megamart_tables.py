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
        conn.execute("""SELECT count(quantity), sum(sales_amount),
                            CASE
                                WHEN date >= '2020-01-01' AND date <= '2020-01-31' THEN 'january'
                                WHEN date >= '2020-02-01' AND date <= '2020-02-29' THEN 'february'
                                WHEN date >= '2020-03-01' AND date <= '2020-03-31' THEN 'march'
                                WHEN date >= '2020-04-01' AND date <= '2020-04-30' THEN 'april'
                                WHEN date >= '2020-05-01' AND date <= '2020-05-31' THEN 'may'
                                WHEN date >= '2020-06-01' AND date <= '2020-06-30' THEN 'june'
                                WHEN date >= '2020-07-01' AND date <= '2020-07-31' THEN 'july'
                                WHEN date >= '2020-08-01' AND date <= '2020-08-31' THEN 'august'
                                WHEN date >= '2020-09-01' AND date <= '2020-09-30' THEN 'september'
                                WHEN date >= '2020-10-01' AND date <= '2020-10-31' THEN 'october'
                                WHEN date >= '2020-11-01' AND date <= '2020-11-30' THEN 'november'
                                WHEN date >= '2020-12-01' AND date <= '2020-12-31' THEN 'december'
                                ELSE 'others'
                                END AS monthly_sales
                        FROM transaction_data
                        COUNT(date)
                        GROUP BY monthly_sales

                                            """)
        records = conn.fetchall();

        print("Print each row and it's columns values")
        
        listOfTransactions = []
        
        # 1. Open a new CSV file
        with open('monthly_sales.csv', 'w', newline='') as file:
        # 2. Create a CSV writer
         writer = csv.writer(file)
        # 3. Write data to the file
        
         monthly_sales_header = ['quantity_sold', 'total_sales', 'month']
         writer.writerow(monthly_sales_header)
         for row in records:
            writer.writerow([row[0], row[1], row[2]])

                             
        print("Engine valid")
except Exception as e:
    print(f"Engine invalid: {e}")