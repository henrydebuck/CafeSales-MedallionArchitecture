import pandas as pd
from sqlalchemy import schema, text
import uuid
from scripts.databaseConnection import connect_to_database

def calcul_total_spent(quantity, priceUnit):
   if(quantity is not None and priceUnit is not None):
      return quantity * priceUnit
   return None
   
def save_to_database(schema_name, table_name, dataframe):
   print(f"Saving {schema_name} layer in database...")
   try:
      con.execute(schema.CreateSchema(schema_name, if_not_exists=True))
      con.commit()
      dataframe.to_sql(table_name, engine, schema=schema_name, if_exists='replace')
      query = f"ALTER TABLE {schema_name}.{table_name} DROP COLUMN index"
      con.execute(text(query))
      con.commit()
   except:
      print(f"An error occured while saving table {table_name} in schema {schema_name} in database")

if __name__ == '__main__':      
   DIRTY_COLUMNS = ['ERROR', 'UNKNOWN', float("nan")]
   DATASET_PATH = 'datasets/dirty_cafe_sales.csv'
   
   # Connection to the database
   engine, con = connect_to_database()

   # ------------------------------------
   # 1. Extract
   # ------------------------------------
   print("\nExtract processing...")
   _extract = pd.read_csv(DATASET_PATH)

   print(list(_extract['Item'].values.unique()))
   print(_extract.groupby('Item')['Price Per Unit'].unique())
   print(list(_extract['Quantity'].values.unique()))
   print(list(_extract['Price Per Unit'].values.unique()))
   print(list(_extract['Total Spent'].values.unique()))
   print(list(_extract['Payment Method'].values.unique()))
   print(list(_extract['Location'].values.unique()))
   
   # Save into database
   save_to_database("bronze", "cafe_sales", _extract)
   print("Sucessfully process Extract step!")

   # ------------------------------------
   # 2. Transform
   # ------------------------------------
   print("\nTransform processing...")
   _transform = _extract.copy()
   
   # Normalize columns name
   _transform = _transform.rename(columns={
      'Transaction ID':'id',
      'Item': 'item',
      'Quantity': 'quantity',
      'Price Per Unit': 'price_per_unit',
      'Total Spent': 'total_spent',
      'Payment Method': 'payment_method',
      'Location': 'location',
      'Transaction Date': 'transaction_date'
   })

   # Normalize data
   _transform['id'] = _transform['id'].replace(DIRTY_COLUMNS, None)
   _transform['item'] = _transform['item'].replace(DIRTY_COLUMNS, None)
   _transform['quantity'] = _transform['quantity'].replace(DIRTY_COLUMNS, None)
   _transform['price_per_unit'] = _transform['price_per_unit'].replace(DIRTY_COLUMNS, None)
   _transform['total_spent'] = _transform['total_spent'].replace(DIRTY_COLUMNS, None)
   _transform['payment_method'] = _transform['payment_method'].replace(DIRTY_COLUMNS, None)
   _transform['location'] = _transform['location'].replace(DIRTY_COLUMNS, None)
   _transform['location'] = _transform['location'].replace("In-store", "On site")
   _transform['location'] = _transform['location'].replace("Takeaway", "Take away")
   _transform['transaction_date'] = _transform['transaction_date'].replace(DIRTY_COLUMNS, None)

   # Change actual ID
   _transform['id'] = _transform.apply(lambda _: uuid.uuid4(), axis=1)

   # Transform column in right type
   _transform['quantity'] = pd.to_numeric(_transform['quantity'])
   _transform['price_per_unit'] = pd.to_numeric(_transform['price_per_unit'])
   _transform['total_spent'] = pd.to_numeric(_transform['total_spent'])
   _transform['transaction_date'] = pd.to_datetime(_transform['transaction_date'])
   
   # Calcul total spent
   for id, totalSpent in enumerate(_transform['total_spent']):
      if(totalSpent is None):
         quantity = _transform['quantity'][id]
         priceUnit = _transform['price_per_unit'][id]
         _transform.loc[_transform.index == id, 'total_spent'] = calcul_total_spent(quantity, priceUnit)
   
   # Remove rows that contains "None"
   _transform = _transform.dropna().dropna(axis=1)
   
   # Add column for year, month and day
   _transform['year'] = pd.DatetimeIndex(_transform['transaction_date']).year
   _transform['month'] = pd.DatetimeIndex(_transform['transaction_date']).month
   _transform['day'] = pd.DatetimeIndex(_transform['transaction_date']).day

   # Save into the database   
   save_to_database("silver", "cafe_sales", _transform)
   print("Sucessfully process Transform step!")

   # ------------------------------------
   # 3. Load
   # ------------------------------------
   print("\nLoad processing...")
   _load = _transform.copy()
   
   # Save into the database
   save_to_database("gold", "cafe_sales", _load)
   print("Sucessfully process Load step!")

   # Save into csv
   _load.to_csv("gold_layer.csv", index=False)