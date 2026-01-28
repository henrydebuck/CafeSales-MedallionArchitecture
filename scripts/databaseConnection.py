import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Raise exception if var is none
def is_env_var_none(var):
   if var == None:
      raise Exception("One or more env variable are null")

# First connection to the database
def connect_to_database():
   print("connecting to the database...")
   engine = None
   con = None
   try:
      engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
      con = engine.connect()
   except:
      print("Error while connecting to the database.")
   print("Sucessfully connected to the database!")
   return engine, con

# Load .env variables
load_dotenv()

# Define main variables
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

# Raise exception if one or more variable equal to None
is_env_var_none(POSTGRES_DB)
is_env_var_none(POSTGRES_USER)
is_env_var_none(POSTGRES_PASSWORD)
is_env_var_none(POSTGRES_HOST)
is_env_var_none(POSTGRES_PORT)