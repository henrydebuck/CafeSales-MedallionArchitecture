import pandas as pd
from sqlalchemy import schema, text
import uuid
from scripts.databaseConnection import connect_to_database

def calcul_total_spent(quantity, priceUnit):
   # Putting a IF already look at if the value is empty or not
    return quantity * priceUnit if quantity and priceUnit else None
   
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
   DIRTY_COLUMNS = ['ERROR', 'UNKNOWN', float("nan")] # not reliable, use .isna() instead, also this is hardcoding the wrong values. You could use a function to look at the values and search for outliers / dirty elements. Also the name is confusing, this is dirty values not columns.
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
   _transform['id'] = _transform.apply(lambda _: uuid.uuid4(), axis=1) # Why changing the ID? Is it a displayable ID for the user or not?

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
   """
      Dropping in the silver layer is too aggressive, also this way of removing is very destructive. You can lose a lot of data silently.
      Define critical columns where none is not accepted (for example, 'location' could be empty but not 'Item', 'Quantity', 'Price Per Unit').

      Depending of the philosophy you could either flag bad data and fill some data with means or medians.
      You could also generate from this layer a Data Quality report that filter out these lines and show it to key users for them to clean them.
      This kind of decisions should also be documented for better reading and understanding of the report.
   """
   
   # Add column for year, month and day
   _transform['year'] = pd.DatetimeIndex(_transform['transaction_date']).year
   _transform['month'] = pd.DatetimeIndex(_transform['transaction_date']).month
   _transform['day'] = pd.DatetimeIndex(_transform['transaction_date']).day
   # Why do you add datetime index into silver?
   
   # Save into the database   
   save_to_database("silver", "cafe_sales", _transform)
   print("Sucessfully process Transform step!")

   # ------------------------------------
   # 3. Load
   # ------------------------------------
   print("\nLoad processing...")
   _load = _transform.copy()

   """
      Gold should be ready to use data, not copy/pasted from silver.
      For example here, you could add the 104-107 lines and split it into a new table named 'dim_date' where you do store all of the dates.
      This table could looks like that:
         (dim_date)
            - id_date
            - datetime
            - year
            - month
            - day
            - label_month
            - label_day
            - year_month
            - label_year_month
            - ... UTC feature for different locations?

      The idea of gold table is ready to use and you should already in it prepare some calculated columns or metrics as much as possible to gain performances in your BI analysis tool.
      Also use the naming convention dim_ and fact_ (or d_ and f_) for Dimensions and Fact, to help the BI analyst to better read the schema.
   """
   # Save into the database
   save_to_database("gold", "cafe_sales", _load)
   print("Sucessfully process Load step!")

   # Save into csv
   _load.to_csv("gold_layer.csv", index=False)

   """
      You could split the layers into differents files, that is what we do if we have multiple files and multiple sources, even here this is harder to do and will cost you useless effort because you only have 1 source.
   """
