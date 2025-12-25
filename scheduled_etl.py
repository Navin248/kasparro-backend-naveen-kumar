from ingestion.api_coinpaprika import fetch_coinpaprika
from ingestion.api_coingecko import fetch_coingecko
from ingestion.csv_loader import load_csv_data
from services.etl_runner import normalize_data

print("🔥 Scheduled ETL Started")

fetch_coinpaprika()
fetch_coingecko()
load_csv_data()
normalize_data()

print("✅ Scheduled ETL Completed")
