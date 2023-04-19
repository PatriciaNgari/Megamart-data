import sqlalchemy as sa
import psycopg2
from io import open
import pandas as pd

class Transaction:
  def __init__(self, date, customerId, transactionId,productCategoryId,sku, quantity,salesAmount ):
    self.date = date
    self.customerId = customerId
    self.transactionId = transactionId
    self.productCategoryId = productCategoryId
    self.sku = sku
    self.quantity = quantity
    self.salesAmount = salesAmount
    
  def __str__(self):
     return f'date: {self.date}, customerId: {self.customerId}, transactionId: {self.transactionId}, productCategoryId: {self.productCategoryId}, sku: {self.sku}, quantity: {self.quantity}, salesAmount: {self.salesAmount}'

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
        conn.execute("""SELECT * FROM  transaction_data
                     WHERE product_category_id = '0H2'""")
        records = conn.fetchall();

        print("Print each row and it's columns values")
        
        listOfTransactions = []
        
        for row in records:
            listOfTransactions.append(Transaction(
              date= row[0],
              customerId= row[1], 
              transactionId= row[2],
              productCategoryId= row[3],
              sku= row[4], 
              quantity= row[5],
              salesAmount= row[6]
            ))

        for transaction in listOfTransactions:
            print(str(transaction))         
            
        print("Engine valid")
except Exception as e:
    print(f"Engine invalid: {e}")