from core.db import engine
from schemas.models import Base

def create_all_tables():
    Base.metadata.create_all(bind=engine)
    print("Tables created in database")

if __name__ == "__main__":
    create_all_tables()
