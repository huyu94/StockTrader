from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backend.src.services.settings import Settings


_ENGINE = None
_SESSION_FACTORY = None


def _init_engine() -> None:
    global _ENGINE, _SESSION_FACTORY
    settings = Settings.from_env()
    _ENGINE = create_engine(settings.database_url, pool_pre_ping=True)
    _SESSION_FACTORY = sessionmaker(bind=_ENGINE, autocommit=False, autoflush=False)


def get_session_factory() -> sessionmaker:
    if _SESSION_FACTORY is None:
        _init_engine()
    return _SESSION_FACTORY


def get_session() -> Generator[Session, None, None]:
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
