"""PostgreSQL connection helper (SQLAlchemy + psycopg2) for the healthcare ETL pipeline."""

import logging
import os
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
DDL_DIR = SQL_DIR / "ddl"
TRANSFORM_DIR = SQL_DIR / "transformations"
ANALYTICS_DIR = SQL_DIR / "analytics"

# Let psycopg2 bind NumPy scalars (pandas/NumPy values) directly.
register_adapter(np.int64, AsIs)
register_adapter(np.int32, AsIs)
register_adapter(np.float64, AsIs)
register_adapter(np.float32, AsIs)
register_adapter(np.bool_, lambda v: AsIs(str(bool(v)).upper()))

_PARAM_PATTERN = r"(?<!:):{name}\b"


class DatabaseConnection:
    """Thin wrapper around a SQLAlchemy engine for PostgreSQL.

    Settings come from arguments or the environment / .env file:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        load_dotenv(PROJECT_ROOT / ".env")
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = int(port or os.getenv("DB_PORT", 5432))
        self.database = database or os.getenv("DB_NAME", "healthcare_dw")
        self.user = user or os.getenv("DB_USER", "postgres")
        self.password = password or os.getenv("DB_PASSWORD", "")
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            url = URL.create(
                "postgresql+psycopg2",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
            )
            self._engine = create_engine(url, pool_pre_ping=True)
        return self._engine

    def test_connection(self) -> bool:
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connected to %s@%s:%s/%s", self.user, self.host, self.port, self.database)
        return True

    # ---- SQL file helpers -------------------------------------------------

    @staticmethod
    def read_sql_text(path: Path) -> str:
        return Path(path).read_text(encoding="utf-8")

    def run_sql_file(self, path: Path) -> None:
        """Execute a (possibly multi-statement) SQL file in one transaction."""
        sql = self.read_sql_text(path)
        with self.engine.begin() as conn:
            # Raw DBAPI cursor: runs multi-statement scripts without parameter interpolation.
            cursor = conn.connection.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()
        logger.info("Executed %s", Path(path).name)

    def run_sql_dir(self, directory: Path) -> None:
        """Execute every .sql file in a directory in filename order."""
        for path in sorted(Path(directory).glob("*.sql")):
            self.run_sql_file(path)

    def read_sql_file(self, path: Path, params: Optional[dict] = None) -> pd.DataFrame:
        """Run a SELECT stored in a SQL file and return a DataFrame.

        Only the named parameters (:name) that appear in the file are bound.
        """
        sql = self.read_sql_text(path)
        used = {
            k: v for k, v in (params or {}).items()
            if re.search(_PARAM_PATTERN.format(name=re.escape(k)), sql)
        }
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=used)

    # ---- DataFrame helpers ------------------------------------------------

    def write_dataframe(
        self, df: pd.DataFrame, table: str, if_exists: str = "append", chunksize: int = 1000
    ) -> int:
        """Append a DataFrame to a table (NaN/NaT become NULL). Returns rows written."""
        if df.empty:
            return 0
        clean = df.astype(object).where(df.notna(), None)
        with self.engine.begin() as conn:
            clean.to_sql(table, conn, if_exists=if_exists, index=False,
                         method="multi", chunksize=chunksize)
        logger.info("Wrote %d rows to %s", len(df), table)
        return len(df)

    def upsert_dataframe(self, df: pd.DataFrame, sql_path: Path, page_size: int = 1000) -> int:
        """Execute an upsert SQL file once per DataFrame row, in one transaction.

        The SQL file uses pyformat parameters (%(column)s) matching the DataFrame's
        column names. NaN / NaT / <NA> are sent as NULL. Returns rows sent.
        """
        if df.empty:
            return 0
        records = df.astype(object).where(df.notna(), None).to_dict("records")
        sql = self.read_sql_text(sql_path)
        with self.engine.begin() as conn:
            cursor = conn.connection.cursor()
            try:
                execute_batch(cursor, sql, records, page_size=page_size)
            finally:
                cursor.close()
        logger.info("Upserted %d rows via %s", len(records), Path(sql_path).name)
        return len(records)

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
