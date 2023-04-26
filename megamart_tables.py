import sqlalchemy as sa
import psycopg2
from io import open
import pandas as pd


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
        conn.execute("""SELECT s.product_category_id, sum(sales_amount)
                        FROM transaction_data
                        JOIN salesperson s
                        on transaction_data.product_category_id = s.product_category_id
                        WHERE sales_person_id = 'Tarun Goodwin'
                        GROUP BY s.product_category_id
                        ORDER BY sum(sales_amount) DESC """)
        records = conn.fetchall();

        print("Print each row and it's columns values")
        
        listOfTransactions = []
        
        
        
        print ('product_category_id; sum(sales_amount)')
        for row in records:
            print(row)
            # listOfTransactions.append(Transaction(
            #   date= row[0],
            #   customerId= row[1],
            #   transactionId= row[2],
            #   productCategoryId= row[3],
            #   SKU= row[4],
            #   quantity= row[5],
            #   salesAmount= row[6]
              

             
              
            #  ))

        for transaction in listOfTransactions:
            print(str(transaction))         
            
        print("Engine valid")
except Exception as e:
    print(f"Engine invalid: {e}")