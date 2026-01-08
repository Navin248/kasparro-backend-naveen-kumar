from services.etl_runner import normalize_data
from core.db import SessionLocal
from schemas.models import NormalizedCoin, RawCoinPaprika, RawCoinGecko
from sqlalchemy import text

def test_normalization_merges_duplicates():
    db = SessionLocal()
    # Cleanup
    db.execute(text("TRUNCATE TABLE normalized_coins RESTART IDENTITY CASCADE"))
    db.execute(text("TRUNCATE TABLE raw_coinpaprika RESTART IDENTITY CASCADE"))
    db.execute(text("TRUNCATE TABLE raw_coingecko RESTART IDENTITY CASCADE"))
    db.commit()

    # Insert Raw Data simulating different sources for same coin
    paprika_coin = RawCoinPaprika(data=[
        {"id": "btc-bitcoin", "name": "Bitcoin", "symbol": "BTC"}
    ])
    gecko_coin = RawCoinGecko(data=[
        {"id": "btc-bitcoin", "name": "Bitcoin (Gecko)", "symbol": "BTC"}
    ])
    
    db.add(paprika_coin)
    db.add(gecko_coin)
    db.commit()

    # Run Normalization
    normalize_data()

    # Verify
    coins = db.query(NormalizedCoin).all()
    assert len(coins) == 1
    assert coins[0].coin_id == "btc-bitcoin"
    # It should pick up the latest one or merge (logic overwrites)
    # Since we iterate paprika then gecko, gecko should overwrite name
    assert coins[0].name == "Bitcoin (Gecko)" 
