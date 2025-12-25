from services.etl_runner import normalize_data
from ingestion.api_coinpaprika import fetch_coinpaprika
from core.db import SessionLocal
from schemas.models import NormalizedCoin

def test_etl_process():
    db = SessionLocal()

    fetch_coinpaprika()
    normalize_data()

    count = db.query(NormalizedCoin).count()

    assert count > 0
