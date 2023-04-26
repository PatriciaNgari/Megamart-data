import sqlalchemy as sa
import psycopg2
from io import open
import pandas as pd

class Transaction:
  def __init__(self , productCategoryId, productCatName ):
    self.productCategoryId = productCategoryId
    self.productCatName = productCatName

    
  def __str__(self):
     return f'productCategoryId: {self.productCategoryId}, productCatName: {self.productCatName}';

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
        conn.execute("""SELECT * 
                     FROM product""")
        records = conn.fetchall();

        print("Print each row and it's columns values")
        
        listOfTransactions = []
        
        for row in records:
            listOfTransactions.append(Transaction(
           
              productCategoryId= row[0],
              productCatName= row[1]

             
              
             ))

        for transaction in listOfTransactions:
            print(str(transaction))         
            
        print("Engine valid")
except Exception as e:
    print(f"Engine invalid: {e}")