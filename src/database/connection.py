"""
Database connection — single source of truth for all DB sessions.

Usage:
    from src.database import get_db_session

    with get_db_session() as session:
        runs = session.query(ValidationRun).all()
"""

import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

load_dotenv()  # reads .env file automatically

DATABASE_URL: str = os.environ["DATABASE_URL"]
# Example value in .env:
#   DATABASE_URL=postgresql://datanexus:secret@localhost:5432/datanexus

engine = create_engine(
    DATABASE_URL,
    pool_size=5,          # keep 5 connections open
    max_overflow=10,      # allow 10 extra connections under load
    pool_pre_ping=True,   # test each connection before using it
    echo=False,           # set True to log every SQL query (useful for debugging)
)

_SessionFactory = sessionmaker(bind=engine, autoflush=False, autocommit=False)


@contextmanager
def get_db_session() -> Session:
    """
    Context manager that opens a database session and closes it cleanly.

    Always use this — never create a session directly.

    Example:
        with get_db_session() as session:
            session.add(my_object)
            session.commit()
    """
    session: Session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
