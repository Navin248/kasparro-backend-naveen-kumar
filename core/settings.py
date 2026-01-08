import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
if not DB_USER:
    raise ValueError("DB_USER is not set")

DB_PASSWORD = os.getenv("DB_PASSWORD")
if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD is not set")

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise ValueError("DB_NAME is not set")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
