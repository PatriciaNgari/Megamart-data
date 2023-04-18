import sqlalchemy as sa
import psycopg2
from io import open
import pandas as pd

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
        
        f = open(r'C:\Users\User\Downloads\product.csv')
        
        conn.copy_from(f, 'salesperson', sep=";", null="")
        cur.commit() 
    
        print("Engine valid")
except Exception as e:
    print(f"Engine invalid: {e}")