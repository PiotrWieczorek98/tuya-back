import os
import sqlitecloud
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DATABASE_URL = os.environ.get("DATABASE_URL") or ""

def get_db_connection():
    return sqlitecloud.connect(DATABASE_URL)