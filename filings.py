from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import time
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
import requests
from sec_api import QueryApi, XbrlApi
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine, URL

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

try:
    from .api_keys import gptdb, sec_api_key
except ImportError:
    from market_data.api_keys import gptdb, sec_api_key
# from api_keys import sec_api_key


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
GPTDB_MYSQL_USER = "gptdb"
FORM13F_DB = "form13f"
FORM4_DB = "form4"
EXECUTIVE_COMPENSATION_DB = "executive_compensation"
SEC_API_BASE_URL = "https://api.sec-api.io"
FORM4_INSIDER_TRADING_ENDPOINT = f"{SEC_API_BASE_URL}/insider-trading"
FORM13F_COVER_PAGES_ENDPOINT = f"{SEC_API_BASE_URL}/form-13f/cover-pages"
FORM13F_HOLDINGS_ENDPOINT = f"{SEC_API_BASE_URL}/form-13f/holdings"
EXECUTIVE_COMPENSATION_ENDPOINT = f"{SEC_API_BASE_URL}/compensation"
SEC_API_FLOAT_ENDPOINT = f"{SEC_API_BASE_URL}/float"
SEC_API_XBRL_ENDPOINT = f"{SEC_API_BASE_URL}/xbrl-to-json"
SEC_API_EXTRACTOR_ENDPOINT = f"{SEC_API_BASE_URL}/extractor"
SEC_API_FULL_TEXT_SEARCH_ENDPOINT = f"{SEC_API_BASE_URL}/full-text-search"
SEC_API_FORM_D_ENDPOINT = f"{SEC_API_BASE_URL}/form-d"
DEFAULT_13F_THRESHOLD = 1_000_000_000
DEFAULT_13F_START_PERIOD = "2013-01-01"
DEFAULT_EXEC_COMP_START_YEAR = 2005
DEFAULT_FORM4_LOOKBACK_DAYS = 365
DEFAULT_DILUTION_LOOKBACK_DAYS = 365 * 5
SEC_API_FORM4_PAGE_SIZE = 50
SEC_API_FORM4_FROM_LIMIT = 10_000
SEC_API_13F_PAGE_SIZE = 50
SEC_API_13F_FROM_LIMIT = 10_000
SEC_API_EXEC_COMP_PAGE_SIZE = 50
SEC_API_EXEC_COMP_FROM_LIMIT = 10_000
SEC_API_DILUTION_PAGE_SIZE = 50
SEC_API_DILUTION_FROM_LIMIT = 10_000
SEC_API_FULL_TEXT_PAGE_SIZE = 100

PERIODIC_DILUTION_FORMS = ("10-Q", "10-Q/A", "10-K", "10-K/A")
CURRENT_DILUTION_EVENT_FORMS = ("8-K", "8-K/A")
OFFERING_DILUTION_FORMS = (
    "S-1",
    "S-1/A",
    "S-1MEF",
    "S-3",
    "S-3/A",
    "S-3ASR",
    "S-3MEF",
    "424B2",
    "424B3",
    "424B4",
    "424B5",
    "424B7",
    "EFFECT",
    "POS AM",
    "POSASR",
    "RW",
)
COMPENSATION_DILUTION_FORMS = ("S-8", "S-8 POS", "DEF 14A", "PRE 14A")
PRIVATE_OFFERING_DILUTION_FORMS = ("D", "D/A")
MERGER_DILUTION_FORMS = ("S-4", "S-4/A", "DEFM14A")
DILUTION_8K_ITEM_CODES = ("1.01", "2.03", "3.02", "3.03", "5.03", "7.01", "8.01", "9.01")
DILUTION_8K_EXTRACTOR_ITEMS = ("1-1", "2-3", "3-2", "3-3", "5-3", "7-1", "8-1", "9-1")
PERIODIC_DILUTION_EXTRACTOR_ITEMS = {
    "10-K": ("5", "8", "15"),
    "10-K/A": ("5", "8", "15"),
    "10-Q": ("part1item1", "part2item2", "part2item6"),
    "10-Q/A": ("part1item1", "part2item2", "part2item6"),
}
DILUTION_KEYWORD_QUERY = (
    '"common stock" OR warrant* OR convertible OR "at-the-market" OR ATM OR '
    '"equity line" OR "registered direct" OR PIPE OR "shares reserved" OR '
    '"selling stockholder" OR "employee stock purchase" OR "incentive plan" OR '
    '"authorized shares" OR "reverse stock split"'
)
DILUTION_QUANTITY_CATEGORIES = (
    "outstanding",
    "newly_issued",
    "sold",
    "issuable",
    "registered",
    "reserved",
    "authorized",
    "underlying_warrants",
    "underlying_convertibles",
    "selling_stockholder",
    "repurchased",
    "cancelled",
)
DILUTION_SHARE_COUNT_COLUMNS = (
    "symbol",
    "cik",
    "period",
    "share_class",
    "shares_outstanding",
    "period_of_report",
    "reported_at",
    "source_accession_no",
    "actual_dilution_pct",
    "raw_json",
)
DILUTION_EVENT_COLUMNS = (
    "symbol",
    "cik",
    "accession_no",
    "form_type",
    "filed_at",
    "period_of_report",
    "company_name",
    "source_endpoint",
    "source_url",
    "source_section",
    "category",
    "quantity",
    "amount",
    "security_type",
    "confidence",
    "snippet",
    "raw_match",
)
DILUTION_CANDIDATE_COLUMNS = (
    "symbol",
    "cik",
    "accession_no",
    "form_type",
    "filed_at",
    "period_of_report",
    "company_name",
    "source_endpoint",
    "source_url",
    "source_section",
    "matched_keywords",
    "snippet",
    "parse_status",
)


def mysql_identifier(name: str) -> str:
    """Return a MySQL-safe quoted identifier."""
    return f"`{name.replace('`', '``')}`"


def chunked(items: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def parse_date(value: Any) -> dt.date | None:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text_value = str(value).strip()
    for date_format in ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return dt.datetime.strptime(text_value[:10], date_format).date()
        except ValueError:
            continue
    return dt.date.fromisoformat(text_value[:10])


def parse_datetime(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(str(value).replace(",", ""))


def parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except InvalidOperation:
        return None


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def first_mapping(*values: Any) -> Mapping[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return value
    return {}


def first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def without_key(mapping: Mapping[str, Any], key_to_remove: str) -> dict[str, Any]:
    """Return a shallow copy of `mapping` without one potentially large key."""
    return {key: value for key, value in mapping.items() if key != key_to_remove}


@dataclass
class Form13FImportStats:
    """Summary returned by 13F import methods."""

    cover_files: int = 0
    holdings_files: int = 0
    periods: int = 0
    cover_requests: int = 0
    holdings_requests: int = 0
    cover_rows: int = 0
    holding_rows: int = 0
    deleted_holding_rows: int = 0
    skipped_holding_rows: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "cover_files": self.cover_files,
            "holdings_files": self.holdings_files,
            "periods": self.periods,
            "cover_requests": self.cover_requests,
            "holdings_requests": self.holdings_requests,
            "cover_rows": self.cover_rows,
            "holding_rows": self.holding_rows,
            "deleted_holding_rows": self.deleted_holding_rows,
            "skipped_holding_rows": self.skipped_holding_rows,
            "errors": self.errors,
        }


class SearchResultLimitError(RuntimeError):
    """Raised when a sec-api search query reaches the 10,000 result cap."""

    def __init__(self, query: str, total: int) -> None:
        super().__init__(f"Query matched {total:,} records and exceeds the sec-api from-limit: {query}")
        self.query = query
        self.total = total


class Form13FDatabase:
    """
    MySQL storage layer for sec-api.io Form 13F cover pages and holdings.

    The storage model intentionally uses exactly two tables:
    `cover_pages` at one row per accession number and `holdings` at one row per
    security line item. Nested source data is retained in JSON columns.
    """

    COVER_PAGE_COLUMNS = (
        "accession_no",
        "form_type",
        "period_of_report",
        "filed_at",
        "cik",
        "company_name",
        "crd_number",
        "sec_file_number",
        "form_13f_file_number",
        "report_type",
        "provide_info_for_instruction5",
        "table_entry_total",
        "table_entry_total_as_reported",
        "table_value_total",
        "table_value_total_as_reported",
        "other_included_managers_count",
        "filing_manager_name",
        "filing_manager_street1",
        "filing_manager_street2",
        "filing_manager_city",
        "filing_manager_state_or_country",
        "filing_manager_zip_code",
        "signer_name",
        "signer_title",
        "signer_phone",
        "signer_signature",
        "signer_city",
        "signer_state_or_country",
        "signature_date",
        "other_managers_reporting_for_this_manager_json",
        "other_included_managers_json",
        "document_format_files_json",
        "raw_json",
        "source_key",
        "source_updated_at",
    )
    HOLDING_COLUMNS = (
        "accession_no",
        "line_number",
        "form_type",
        "period_of_report",
        "filed_at",
        "cik",
        "company_name",
        "name_of_issuer",
        "title_of_class",
        "cusip",
        "ticker",
        "issuer_cik",
        "value",
        "ssh_prnamt",
        "ssh_prnamt_type",
        "put_call",
        "investment_discretion",
        "other_manager",
        "voting_sole",
        "voting_shared",
        "voting_none",
        "shrs_or_prn_amt_json",
        "voting_authority_json",
        "raw_holding_json",
        "raw_filing_json",
        "source_key",
        "source_updated_at",
    )

    def __init__(
        self,
        database: str = FORM13F_DB,
        user: str = GPTDB_MYSQL_USER,
        password: str = gptdb,
        host: str = MYSQL_HOST,
        port: int = MYSQL_PORT,
        engine: Engine | None = None,
    ) -> None:
        self.database = database
        self.engine = engine or self._make_engine(database, user, password, host, port)

    def _make_engine(
        self,
        database: str,
        user: str,
        password: str,
        host: str,
        port: int,
    ) -> Engine:
        url = URL.create(
            "mysql+pymysql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})

    def setup(self) -> None:
        """Create the two Form 13F tables if they do not already exist."""
        with self.engine.begin() as conn:
            conn.execute(text(self._cover_pages_ddl()))
            conn.execute(text(self._holdings_ddl()))

    def ensure_holdings_foreign_key(self) -> None:
        """
        Add the holdings-to-cover-pages foreign key to an existing table.

        This is only needed when `holdings` was created before the MySQL user
        had REFERENCES privileges. MySQL requires ALTER privileges for this
        repair path; fresh `setup()` runs create the constraint in the table DDL.
        """
        with self.engine.begin() as conn:
            self._ensure_holdings_foreign_key(conn)

    def latest_filed_at(self) -> dt.datetime | None:
        query = text("SELECT MAX(filed_at) FROM cover_pages")
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return value

    def latest_holdings_filed_at(self) -> dt.datetime | None:
        query = text("SELECT MAX(filed_at) FROM holdings")
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return value

    def latest_synced_filed_at(self) -> dt.datetime | None:
        """
        Return the latest filing timestamp both tables appear to have reached.

        If either table is empty, return None so a default incremental import
        falls back to a full import instead of skipping the missing side.
        """
        cover_latest = self.latest_filed_at()
        holdings_latest = self.latest_holdings_filed_at()
        if cover_latest is None or holdings_latest is None:
            return None
        return min(cover_latest, holdings_latest)

    def latest_period_of_report(self) -> dt.date | None:
        query = text("SELECT MAX(period_of_report) FROM cover_pages")
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return value

    def latest_holdings_period_of_report(self) -> dt.date | None:
        query = text("SELECT MAX(period_of_report) FROM holdings")
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return value

    def latest_synced_period_of_report(self) -> dt.date | None:
        """
        Return the latest reporting period both tables appear to have reached.

        If either table is empty, return None so a default incremental import
        starts from the default historical period rather than skipping holdings.
        """
        cover_latest = self.latest_period_of_report()
        holdings_latest = self.latest_holdings_period_of_report()
        if cover_latest is None or holdings_latest is None:
            return None
        return min(cover_latest, holdings_latest)

    def row_counts(self) -> dict[str, int]:
        """Return current row counts for the two 13F tables."""
        with self.engine.connect() as conn:
            return {
                "cover_pages": int(conn.execute(text("SELECT COUNT(*) FROM cover_pages")).scalar() or 0),
                "holdings": int(conn.execute(text("SELECT COUNT(*) FROM holdings")).scalar() or 0),
            }

    def upsert_cover_pages(self, rows: list[dict[str, Any]], batch_size: int = 1000) -> int:
        if not rows:
            return 0

        statement = text(self._upsert_sql("cover_pages", self.COVER_PAGE_COLUMNS, ("accession_no",)))
        inserted = 0
        with self.engine.begin() as conn:
            for batch in chunked(rows, batch_size):
                conn.execute(statement, batch)
                inserted += len(batch)
        return inserted

    def replace_holdings_for_filings(
        self,
        rows: list[dict[str, Any]],
        batch_size: int = 2000,
    ) -> tuple[int, int, int]:
        """
        Delete then reload holdings for each accession represented by `rows`.

        Returns `(inserted_rows, deleted_rows, skipped_rows)`.
        """
        if not rows:
            return 0, 0, 0

        accessions = sorted({row["accession_no"] for row in rows if row.get("accession_no")})
        if not accessions:
            return 0, 0, len(rows)

        valid_accessions = self.existing_accessions(accessions)
        insertable_rows = [row for row in rows if row.get("accession_no") in valid_accessions]
        skipped_rows = len(rows) - len(insertable_rows)

        if not insertable_rows:
            return 0, 0, skipped_rows

        delete_statement = (
            text("DELETE FROM holdings WHERE accession_no IN :accession_nos")
            .bindparams(bindparam("accession_nos", expanding=True))
        )
        insert_statement = text(self._upsert_sql("holdings", self.HOLDING_COLUMNS, ("accession_no", "line_number")))

        inserted = 0
        deleted = 0
        with self.engine.begin() as conn:
            for batch in chunked(sorted(valid_accessions), 1000):
                result = conn.execute(delete_statement, {"accession_nos": batch})
                deleted += result.rowcount or 0
            for batch in chunked(insertable_rows, batch_size):
                conn.execute(insert_statement, batch)
                inserted += len(batch)

        return inserted, deleted, skipped_rows

    def existing_accessions(self, accession_nos: Iterable[str]) -> set[str]:
        accessions = [accession for accession in accession_nos if accession]
        if not accessions:
            return set()

        query = (
            text("SELECT accession_no FROM cover_pages WHERE accession_no IN :accession_nos")
            .bindparams(bindparam("accession_nos", expanding=True))
        )
        found: set[str] = set()
        with self.engine.connect() as conn:
            for batch in chunked([{"accession_no": value} for value in accessions], 1000):
                values = [row["accession_no"] for row in batch]
                found.update(str(row[0]) for row in conn.execute(query, {"accession_nos": values}))
        return found

    def _upsert_sql(self, table_name: str, columns: tuple[str, ...], key_columns: tuple[str, ...]) -> str:
        quoted_columns = ", ".join(mysql_identifier(column) for column in columns)
        values = ", ".join(f":{column}" for column in columns)
        update_columns = [column for column in columns if column not in key_columns]
        updates = ", ".join(
            f"{mysql_identifier(column)} = VALUES({mysql_identifier(column)})"
            for column in update_columns
        )
        updates = f"{updates}, loaded_at = CURRENT_TIMESTAMP(6)"
        return (
            f"INSERT INTO {mysql_identifier(table_name)} ({quoted_columns}) "
            f"VALUES ({values}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )

    def _ensure_holdings_foreign_key(self, conn) -> None:
        query = text(
            """
            SELECT COUNT(*)
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = DATABASE()
              AND TABLE_NAME = 'holdings'
              AND CONSTRAINT_NAME = 'fk_holdings_cover_pages'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
            """
        )
        constraint_exists = bool(conn.execute(query).scalar())
        if constraint_exists:
            return

        conn.execute(
            text(
                """
                ALTER TABLE holdings
                ADD CONSTRAINT fk_holdings_cover_pages
                    FOREIGN KEY (accession_no)
                    REFERENCES cover_pages (accession_no)
                    ON DELETE CASCADE
                """
            )
        )

    def _cover_pages_ddl(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS cover_pages (
            accession_no VARCHAR(25) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            form_type VARCHAR(20) NULL,
            period_of_report DATE NULL,
            filed_at DATETIME(6) NULL,
            cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            company_name VARCHAR(255) NULL,
            crd_number VARCHAR(32) NULL,
            sec_file_number VARCHAR(32) NULL,
            form_13f_file_number VARCHAR(32) NULL,
            report_type VARCHAR(64) NULL,
            provide_info_for_instruction5 BOOLEAN NOT NULL DEFAULT FALSE,
            table_entry_total INT UNSIGNED NULL,
            table_entry_total_as_reported INT UNSIGNED NULL,
            table_value_total DECIMAL(24, 0) NULL,
            table_value_total_as_reported DECIMAL(24, 0) NULL,
            other_included_managers_count INT UNSIGNED NULL,
            filing_manager_name VARCHAR(255) NULL,
            filing_manager_street1 VARCHAR(255) NULL,
            filing_manager_street2 VARCHAR(255) NULL,
            filing_manager_city VARCHAR(100) NULL,
            filing_manager_state_or_country VARCHAR(32) NULL,
            filing_manager_zip_code VARCHAR(32) NULL,
            signer_name VARCHAR(255) NULL,
            signer_title VARCHAR(255) NULL,
            signer_phone VARCHAR(64) NULL,
            signer_signature VARCHAR(255) NULL,
            signer_city VARCHAR(100) NULL,
            signer_state_or_country VARCHAR(32) NULL,
            signature_date DATE NULL,
            other_managers_reporting_for_this_manager_json JSON NULL,
            other_included_managers_json JSON NULL,
            document_format_files_json JSON NULL,
            raw_json JSON NULL,
            source_key VARCHAR(255) NULL,
            source_updated_at DATETIME(6) NULL,
            loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (accession_no),
            KEY idx_cover_pages_cik (cik),
            KEY idx_cover_pages_period (period_of_report),
            KEY idx_cover_pages_filed_at (filed_at),
            KEY idx_cover_pages_cik_period_filed (cik, period_of_report, filed_at)
        ) ENGINE = InnoDB
        """

    def _holdings_ddl(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS holdings (
            accession_no VARCHAR(25) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            line_number INT UNSIGNED NOT NULL,
            form_type VARCHAR(20) NULL,
            period_of_report DATE NULL,
            filed_at DATETIME(6) NULL,
            cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            company_name VARCHAR(255) NULL,
            name_of_issuer VARCHAR(255) NULL,
            title_of_class VARCHAR(100) NULL,
            cusip CHAR(9) CHARACTER SET ascii COLLATE ascii_bin NULL,
            ticker VARCHAR(32) NULL,
            issuer_cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            `value` DECIMAL(24, 0) NULL,
            ssh_prnamt DECIMAL(24, 4) NULL,
            ssh_prnamt_type VARCHAR(16) NULL,
            put_call VARCHAR(16) NULL,
            investment_discretion VARCHAR(32) NULL,
            other_manager VARCHAR(255) NULL,
            voting_sole DECIMAL(24, 4) NULL,
            voting_shared DECIMAL(24, 4) NULL,
            voting_none DECIMAL(24, 4) NULL,
            shrs_or_prn_amt_json JSON NULL,
            voting_authority_json JSON NULL,
            raw_holding_json JSON NULL,
            raw_filing_json JSON NULL,
            source_key VARCHAR(255) NULL,
            source_updated_at DATETIME(6) NULL,
            loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (accession_no, line_number),
            KEY idx_holdings_cusip (cusip),
            KEY idx_holdings_ticker (ticker),
            KEY idx_holdings_issuer_cik (issuer_cik),
            KEY idx_holdings_period (period_of_report),
            KEY idx_holdings_filed_at (filed_at),
            KEY idx_holdings_cik_period (cik, period_of_report),
            CONSTRAINT fk_holdings_cover_pages
                FOREIGN KEY (accession_no)
                REFERENCES cover_pages (accession_no)
                ON DELETE CASCADE
        ) ENGINE = InnoDB
        """


class Form13FNormalizer:
    """Normalize sec-api.io Form 13F JSON records into database rows."""

    @staticmethod
    def cover_page_row(
        record: Mapping[str, Any],
        source_key: str | None = None,
        source_updated_at: Any = None,
    ) -> dict[str, Any]:
        filing_manager = first_mapping(record.get("filingManager"))
        manager_address = first_mapping(filing_manager.get("address"), filing_manager)
        signature = first_mapping(record.get("signature"), record.get("signatureBlock"))

        return {
            "accession_no": record.get("accessionNo"),
            "form_type": record.get("formType"),
            "period_of_report": parse_date(record.get("periodOfReport")),
            "filed_at": parse_datetime(record.get("filedAt")),
            "cik": record.get("cik"),
            "company_name": record.get("companyName"),
            "crd_number": record.get("crdNumber"),
            "sec_file_number": record.get("secFileNumber"),
            "form_13f_file_number": record.get("form13FFileNumber"),
            "report_type": record.get("reportType"),
            "provide_info_for_instruction5": parse_bool(record.get("provideInfoForInstruction5")),
            "table_entry_total": parse_int(record.get("tableEntryTotal")),
            "table_entry_total_as_reported": parse_int(record.get("tableEntryTotalAsReported")),
            "table_value_total": parse_decimal(record.get("tableValueTotal")),
            "table_value_total_as_reported": parse_decimal(record.get("tableValueTotalAsReported")),
            "other_included_managers_count": parse_int(record.get("otherIncludedManagersCount")),
            "filing_manager_name": filing_manager.get("name"),
            "filing_manager_street1": manager_address.get("street1"),
            "filing_manager_street2": manager_address.get("street2"),
            "filing_manager_city": manager_address.get("city"),
            "filing_manager_state_or_country": manager_address.get("stateOrCountry"),
            "filing_manager_zip_code": manager_address.get("zipCode"),
            "signer_name": signature.get("name"),
            "signer_title": signature.get("title"),
            "signer_phone": signature.get("phone"),
            "signer_signature": signature.get("signature"),
            "signer_city": signature.get("city"),
            "signer_state_or_country": signature.get("stateOrCountry"),
            "signature_date": parse_date(signature.get("signatureDate")),
            "other_managers_reporting_for_this_manager_json": json_dumps(
                record.get("otherManagersReportingForThisManager")
            ),
            "other_included_managers_json": json_dumps(record.get("otherIncludedManagers")),
            "document_format_files_json": json_dumps(record.get("documentFormatFiles")),
            "raw_json": json_dumps(record),
            "source_key": source_key,
            "source_updated_at": parse_datetime(source_updated_at),
        }

    @staticmethod
    def holding_rows(
        record: Mapping[str, Any],
        source_key: str | None = None,
        source_updated_at: Any = None,
    ) -> list[dict[str, Any]]:
        filing_values = {
            "accession_no": record.get("accessionNo"),
            "form_type": record.get("formType"),
            "period_of_report": parse_date(record.get("periodOfReport")),
            "filed_at": parse_datetime(record.get("filedAt")),
            "cik": record.get("cik"),
            "company_name": record.get("companyName"),
            "source_key": source_key,
            "source_updated_at": parse_datetime(source_updated_at),
        }
        raw_filing_json = json_dumps(without_key(record, "holdings"))
        rows: list[dict[str, Any]] = []

        for line_number, holding in enumerate(record.get("holdings") or [], start=1):
            if not isinstance(holding, Mapping):
                continue

            shares = first_mapping(holding.get("shrsOrPrnAmt"))
            voting = first_mapping(holding.get("votingAuthority"))
            rows.append(
                {
                    **filing_values,
                    "line_number": line_number,
                    "name_of_issuer": holding.get("nameOfIssuer"),
                    "title_of_class": holding.get("titleOfClass"),
                    "cusip": holding.get("cusip"),
                    "ticker": holding.get("ticker"),
                    "issuer_cik": holding.get("cik"),
                    "value": parse_decimal(holding.get("value")),
                    "ssh_prnamt": parse_decimal(
                        first_present(holding, "sshPrnamt")
                        if first_present(holding, "sshPrnamt") is not None
                        else shares.get("sshPrnamt")
                    ),
                    "ssh_prnamt_type": first_present(holding, "sshPrnamtType") or shares.get("sshPrnamtType"),
                    "put_call": holding.get("putCall"),
                    "investment_discretion": holding.get("investmentDiscretion"),
                    "other_manager": holding.get("otherManager"),
                    "voting_sole": parse_decimal(first_present(voting, "Sole", "sole")),
                    "voting_shared": parse_decimal(first_present(voting, "Shared", "shared")),
                    "voting_none": parse_decimal(first_present(voting, "None", "none")),
                    "shrs_or_prn_amt_json": json_dumps(holding.get("shrsOrPrnAmt")),
                    "voting_authority_json": json_dumps(holding.get("votingAuthority")),
                    "raw_holding_json": json_dumps(holding),
                    "raw_filing_json": raw_filing_json,
                }
            )

        return rows


class Form13FImporter:
    """
    Import filtered Form 13F cover pages and holdings through sec-api.io search endpoints.

    API requests are intentionally sequential. The default request interval is
    conservative because the holdings endpoint can be slow and rate-limited
    during historical imports.
    """

    def __init__(
        self,
        api_key: str = sec_api_key,
        database: Form13FDatabase | None = None,
        normalizer: Form13FNormalizer | None = None,
        batch_size: int = 1000,
        timeout: int = 120,
        min_request_interval: float = 30.0,
        max_retries: int = 6,
        retry_backoff: float = 5.0,
        max_retry_sleep: float = 120.0,
        show_progress: bool = True,
    ) -> None:
        self.api_key = api_key
        self.database = database or Form13FDatabase()
        self.normalizer = normalizer or Form13FNormalizer()
        self.batch_size = batch_size
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_sleep = max_retry_sleep
        self.show_progress = show_progress
        self._last_request_at = 0.0
        self._last_page_requests = 0
        self.session = requests.Session()

    def import_all(
        self,
        start_period: str | dt.date | dt.datetime = DEFAULT_13F_START_PERIOD,
        end_period: str | dt.date | dt.datetime | None = None,
        threshold: int = DEFAULT_13F_THRESHOLD,
    ) -> dict[str, Any]:
        """Import all configured reporting periods for filings at or above `threshold`."""
        self.database.setup()
        periods = self._periods(start_period, end_period)
        return self._import_periods(periods, threshold).as_dict()

    def import_since(
        self,
        start_period: str | dt.date | dt.datetime | None = None,
        end_period: str | dt.date | dt.datetime | None = None,
        threshold: int = DEFAULT_13F_THRESHOLD,
    ) -> dict[str, Any]:
        """
        Import Form 13F reporting periods from `start_period` onward.

        If `start_period` is omitted, the newest `period_of_report` reached by
        both tables is used. If either table is empty, this starts from 2013+.
        """
        self.database.setup()
        if start_period is None:
            start_period = self.database.latest_synced_period_of_report() or DEFAULT_13F_START_PERIOD
        return self.import_all(start_period=start_period, end_period=end_period, threshold=threshold)

    def import_period(
        self,
        period_of_report: str | dt.date | dt.datetime,
        threshold: int = DEFAULT_13F_THRESHOLD,
    ) -> dict[str, Any]:
        """Import one `periodOfReport` quarter at or above `threshold`."""
        self.database.setup()
        stats = Form13FImportStats()
        self._import_period(parse_date(period_of_report), threshold, stats)
        return stats.as_dict()

    def import_recent_periods(
        self,
        as_of: str | dt.date | dt.datetime | None = None,
        quarters_back: int = 2,
        threshold: int = DEFAULT_13F_THRESHOLD,
    ) -> dict[str, Any]:
        """
        Import the most recent completed reporting periods.

        This is intended for scheduled deadline-window runs around February,
        May, August, and November 15-18.
        """
        self.database.setup()
        periods = self._recent_reporting_periods(as_of=as_of, quarters_back=quarters_back)
        return self._import_periods(periods, threshold).as_dict()

    def import_filed_since(
        self,
        filed_since: str | dt.date | dt.datetime,
        filed_until: str | dt.date | dt.datetime | None = None,
        threshold: int = DEFAULT_13F_THRESHOLD,
    ) -> dict[str, Any]:
        """
        Import filings submitted in a filedAt window.

        This catches late filings and amendments for older reporting periods.
        """
        self.database.setup()
        start = parse_date(filed_since)
        end = parse_date(filed_until) if filed_until is not None else dt.date.today()
        stats = Form13FImportStats()
        query = self._filed_at_query(start, end, threshold)
        self._import_query(
            stats=stats,
            query=query,
            cover_source_key=f"cover-pages:filedAt={start.isoformat()}..{end.isoformat()}",
            holdings_source_key=f"holdings:filedAt={start.isoformat()}..{end.isoformat()}",
            progress_desc=f"13F filedAt {start.isoformat()}..{end.isoformat()}",
        )
        return stats.as_dict()

    def import_holdings_for_accessions(
        self,
        accession_nos: Iterable[str],
        sleep_seconds: float = 30.0,
    ) -> dict[str, Any]:
        """Fetch and replace holdings for explicit accession numbers."""
        self.database.setup()
        previous_interval = self.min_request_interval
        self.min_request_interval = sleep_seconds
        stats = Form13FImportStats()
        accessions = [accession_no for accession_no in accession_nos if accession_no]
        progress = self._progress(accessions, desc="13F accession holdings", unit="filing")
        try:
            for accession_no in progress:
                inserted, deleted, skipped = self._import_holdings_by_accession(accession_no)
                stats.holdings_requests += 1
                stats.holding_rows += inserted
                stats.deleted_holding_rows += deleted
                stats.skipped_holding_rows += skipped
                self._set_progress_postfix(progress, holdings=stats.holding_rows)
        finally:
            self.min_request_interval = previous_interval
        return stats.as_dict()

    def _import_periods(
        self,
        periods: list[dt.date],
        threshold: int,
    ) -> Form13FImportStats:
        stats = Form13FImportStats()
        progress = self._progress(periods, desc="13F periods", unit="period")
        for period in progress:
            self._set_progress_postfix(progress, period=period.isoformat())
            self._import_period(period, threshold, stats)
            stats.periods += 1
            self._set_progress_postfix(
                progress,
                period=period.isoformat(),
                cover_rows=stats.cover_rows,
                holding_rows=stats.holding_rows,
            )
        return stats

    def _import_period(
        self,
        period: dt.date,
        threshold: int,
        stats: Form13FImportStats,
    ) -> None:
        try:
            self._import_period_query(period, stats, self._cover_query(period, threshold))
        except SearchResultLimitError:
            for query in self._filed_month_queries(period, threshold):
                self._import_period_query(period, stats, query)

    def _import_period_query(
        self,
        period: dt.date,
        stats: Form13FImportStats,
        query: str,
    ) -> None:
        self._import_query(
            stats=stats,
            query=query,
            cover_source_key=f"cover-pages:periodOfReport={period.isoformat()}",
            holdings_source_key=f"holdings:periodOfReport={period.isoformat()}",
            progress_desc=f"holdings {period.isoformat()}",
        )

    def _import_query(
        self,
        stats: Form13FImportStats,
        query: str,
        cover_source_key: str,
        holdings_source_key: str,
        progress_desc: str,
    ) -> None:
        cover_records = self._paginate_search(
            FORM13F_COVER_PAGES_ENDPOINT,
            query=query,
            sort=[{"filedAt": {"order": "asc"}}],
        )
        stats.cover_requests += self._last_page_requests
        stats.cover_files += 1

        cover_rows = [
            self.normalizer.cover_page_row(record, source_key=cover_source_key)
            for record in cover_records
        ]
        stats.cover_rows += self.database.upsert_cover_pages(cover_rows, batch_size=self.batch_size)

        cover_accessions = {
            str(record["accessionNo"])
            for record in cover_records
            if record.get("accessionNo")
        }
        if not cover_accessions:
            return

        holdings_records = self._paginate_search(
            FORM13F_HOLDINGS_ENDPOINT,
            query=query,
            sort=[{"filedAt": {"order": "asc"}}],
        )
        stats.holdings_requests += self._last_page_requests
        stats.holdings_files += 1

        imported_accessions: set[str] = set()
        holdings_progress = self._progress(holdings_records, desc=progress_desc, unit="filing", leave=False)
        for record in holdings_progress:
            accession_no = record.get("accessionNo")
            if accession_no not in cover_accessions:
                continue
            inserted, deleted, skipped = self._replace_holding_record(
                record,
                source_key=holdings_source_key,
            )
            imported_accessions.add(str(accession_no))
            stats.holding_rows += inserted
            stats.deleted_holding_rows += deleted
            stats.skipped_holding_rows += skipped
            self._set_progress_postfix(holdings_progress, holdings=stats.holding_rows)

        missing_accessions = sorted(cover_accessions - imported_accessions)
        fallback_progress = self._progress(
            missing_accessions,
            desc=f"fallback {progress_desc}",
            unit="filing",
            leave=False,
        )
        for accession_no in fallback_progress:
            inserted, deleted, skipped = self._import_holdings_by_accession(accession_no)
            stats.holdings_requests += 1
            stats.holding_rows += inserted
            stats.deleted_holding_rows += deleted
            stats.skipped_holding_rows += skipped
            self._set_progress_postfix(fallback_progress, holdings=stats.holding_rows)

    def _replace_holding_record(
        self,
        record: Mapping[str, Any],
        source_key: str,
    ) -> tuple[int, int, int]:
        rows = self.normalizer.holding_rows(record, source_key=source_key)
        return self.database.replace_holdings_for_filings(rows, batch_size=self.batch_size)

    def _import_holdings_by_accession(self, accession_no: str) -> tuple[int, int, int]:
        query = f'accessionNo:"{accession_no}"'
        records = self._paginate_search(
            FORM13F_HOLDINGS_ENDPOINT,
            query=query,
            sort=[{"filedAt": {"order": "asc"}}],
            page_size=1,
        )
        if not records:
            return 0, 0, 1
        return self._replace_holding_record(records[0], source_key=f"holdings:accessionNo={accession_no}")

    def _paginate_search(
        self,
        endpoint: str,
        query: str,
        sort: list[dict[str, Any]] | None = None,
        page_size: int = SEC_API_13F_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset = 0
        self._last_page_requests = 0

        while True:
            payload = {
                "query": query,
                "from": str(offset),
                "size": str(page_size),
            }
            if sort is not None:
                payload["sort"] = sort

            response = self._post_search(endpoint, payload)
            self._last_page_requests += 1
            data = response.get("data", [])
            total_payload = first_mapping(response.get("total"))
            total = int(total_payload.get("value") or len(data))
            relation = total_payload.get("relation")
            if total > SEC_API_13F_FROM_LIMIT or (total >= SEC_API_13F_FROM_LIMIT and relation != "eq"):
                raise SearchResultLimitError(query, total)
            if not data:
                break

            records.extend(data)
            offset += page_size
            if len(data) < page_size or offset >= total:
                break

        return records

    def _post_search(self, endpoint: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._post(endpoint, payload) as response:
            return response.json()

    def _post(self, endpoint: str, payload: Mapping[str, Any]) -> requests.Response:
        retry_statuses = {429, 500, 502, 503, 504}
        last_response: requests.Response | None = None

        for attempt in range(self.max_retries + 1):
            self._wait_for_request_slot()
            response = self.session.post(
                endpoint,
                headers=self._headers(),
                json=dict(payload),
                timeout=self.timeout,
            )
            self._last_request_at = time.monotonic()
            last_response = response

            if response.status_code not in retry_statuses:
                response.raise_for_status()
                return response

            if attempt >= self.max_retries:
                response.raise_for_status()

            response.close()
            time.sleep(self._retry_sleep_seconds(response, attempt))

        if last_response is not None:
            last_response.raise_for_status()
        raise RuntimeError(f"Unable to retrieve {endpoint}")

    def _cover_query(self, period: str | dt.date | dt.datetime, threshold: int) -> str:
        period_date = parse_date(period)
        return f"tableValueTotal:[{int(threshold)} TO *] AND periodOfReport:{period_date.isoformat()}"

    def _filed_month_queries(self, period: dt.date, threshold: int) -> list[str]:
        queries = []
        for start, end in self._month_ranges(period, dt.date.today()):
            queries.append(
                f"{self._cover_query(period, threshold)} "
                f"AND filedAt:[{start.isoformat()} TO {end.isoformat()}]"
            )
        return queries

    def _filed_at_query(
        self,
        filed_since: str | dt.date | dt.datetime,
        filed_until: str | dt.date | dt.datetime,
        threshold: int,
    ) -> str:
        start = parse_date(filed_since)
        end = parse_date(filed_until)
        return f"tableValueTotal:[{int(threshold)} TO *] AND filedAt:[{start.isoformat()} TO {end.isoformat()}]"

    def _recent_reporting_periods(
        self,
        as_of: str | dt.date | dt.datetime | None = None,
        quarters_back: int = 2,
    ) -> list[dt.date]:
        if quarters_back < 1:
            raise ValueError("quarters_back must be >= 1")

        as_of_date = parse_date(as_of) if as_of is not None else dt.date.today()
        quarter_ends = []
        year = as_of_date.year
        for candidate_year in range(year - 2, year + 1):
            for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
                quarter_end = dt.date(candidate_year, month, day)
                if quarter_end < as_of_date:
                    quarter_ends.append(quarter_end)

        return sorted(quarter_ends)[-quarters_back:]

    def _periods(
        self,
        start_period: str | dt.date | dt.datetime = DEFAULT_13F_START_PERIOD,
        end_period: str | dt.date | dt.datetime | None = None,
    ) -> list[dt.date]:
        start = parse_date(start_period)
        end = parse_date(end_period) if end_period is not None else dt.date.today()
        quarter_ends = []
        for year in range(start.year, end.year + 1):
            for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
                quarter_end = dt.date(year, month, day)
                if start <= quarter_end <= end:
                    quarter_ends.append(quarter_end)
        return quarter_ends

    def _month_ranges(self, start: dt.date, end: dt.date) -> Iterator[tuple[dt.date, dt.date]]:
        current = start.replace(day=1)
        while current <= end:
            if current.month == 12:
                next_month = dt.date(current.year + 1, 1, 1)
            else:
                next_month = dt.date(current.year, current.month + 1, 1)
            month_end = min(next_month - dt.timedelta(days=1), end)
            yield current, month_end
            current = next_month

    def _wait_for_request_slot(self) -> None:
        if self.min_request_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.min_request_interval - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _retry_sleep_seconds(self, response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), self.max_retry_sleep)
            except ValueError:
                pass

        exponential_sleep = self.retry_backoff * (2 ** attempt)
        return min(exponential_sleep, self.max_retry_sleep)

    def _progress(
        self,
        iterable: Iterable[Any],
        desc: str,
        unit: str,
        leave: bool = True,
    ) -> Iterable[Any]:
        if not self.show_progress or tqdm is None:
            return iterable
        return tqdm(iterable, desc=desc, unit=unit, leave=leave)

    def _set_progress_postfix(self, progress: Iterable[Any], **values: Any) -> None:
        if hasattr(progress, "set_postfix"):
            progress.set_postfix(**values)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self.api_key}


class Form13FAnalytics:
    """
    Analytics layer for normalized Form 13F cover pages and holdings.

    Methods return pandas DataFrames. Plotting is optional and always a side
    effect; callers should use the returned DataFrame as the primary output.
    """

    def __init__(self, database: Form13FDatabase | None = None) -> None:
        self.database = database or Form13FDatabase()
        self.engine = self.database.engine
        self._shares_outstanding_cache: dict[str, int | float | None] = {}

    def manager_performance(
        self,
        manager_cik: str | Iterable[str] | None = None,
        start_period: str | dt.date | dt.datetime | None = None,
        end_period: str | dt.date | dt.datetime | None = None,
        plot: bool = False,
    ) -> pd.DataFrame:
        """
        Return 13F portfolio-value change by manager and reporting period.

        This is not true investment performance. It measures disclosed 13F
        portfolio value changes using the latest filing for each manager/period.
        """
        df = self._latest_cover_pages(manager_cik, start_period, end_period)
        if df.empty:
            return df

        df = df.sort_values(["cik", "period_of_report"]).reset_index(drop=True)
        df["portfolio_value"] = pd.to_numeric(df["portfolio_value"], errors="coerce")
        df["prior_portfolio_value"] = df.groupby("cik")["portfolio_value"].shift(1)
        df["portfolio_value_change"] = df["portfolio_value"] - df["prior_portfolio_value"]
        df["portfolio_value_pct_change"] = (
            df["portfolio_value_change"] / df["prior_portfolio_value"] * 100
        )

        if plot:
            self._plot_manager_performance(df)

        return df

    def stocks_bought_sold_qoq(
        self,
        manager_cik: str | Iterable[str] | None = None,
        start_period: str | dt.date | dt.datetime | None = None,
        end_period: str | dt.date | dt.datetime | None = None,
        top_n: int | None = None,
        min_abs_change: int | float | None = None,
        plot: bool = False,
    ) -> pd.DataFrame:
        """
        Return quarter-over-quarter position value changes.

        The result is a net disclosed position change, not confirmed trading
        flow. Default scope aggregates all imported managers; passing
        `manager_cik` returns manager-level rows.
        """
        holdings = self._latest_holdings(
            manager_cik=manager_cik,
            start_period=start_period,
            end_period=end_period,
        )
        filings = self._latest_cover_pages(manager_cik, start_period, end_period)
        aggregate_managers = manager_cik is None
        df = self._position_value_changes(holdings, filings, aggregate_managers)
        if df.empty:
            return df

        if min_abs_change is not None:
            df = df[df["value_change"].abs() >= float(min_abs_change)]

        df = df.sort_values(["period_of_report", "abs_value_change"], ascending=[True, False])
        if top_n is not None:
            df = df.head(top_n)
        df = df.reset_index(drop=True)

        if plot:
            self._plot_bought_sold(df)

        return df

    def manager_ownership_change_qoq(
        self,
        manager_cik: str | Iterable[str] | None = None,
        tickers: str | Iterable[str] | None = None,
        start_period: str | dt.date | dt.datetime | None = None,
        end_period: str | dt.date | dt.datetime | None = None,
        top_n: int | None = None,
        plot: bool = False,
    ) -> pd.DataFrame:
        """
        Return QoQ ownership changes as a percent of current shares outstanding.

        Uses `fundamentals.current_float()` on demand and caches results for the
        lifetime of the analytics object.
        """
        holdings = self._latest_holdings(
            manager_cik=manager_cik,
            start_period=start_period,
            end_period=end_period,
            tickers=tickers,
            common_shares_only=True,
        )
        filings = self._latest_cover_pages(manager_cik, start_period, end_period)
        aggregate_managers = manager_cik is None
        df = self._position_share_changes(holdings, filings, aggregate_managers)
        if df.empty:
            return df

        unique_tickers = sorted(ticker for ticker in df["ticker"].dropna().unique() if ticker)
        shares_outstanding = {
            ticker: self._shares_outstanding(ticker)
            for ticker in unique_tickers
        }
        df["shares_outstanding"] = df["ticker"].map(shares_outstanding)
        df = df[pd.to_numeric(df["shares_outstanding"], errors="coerce") > 0].copy()
        if df.empty:
            return df

        df["shares_outstanding"] = pd.to_numeric(df["shares_outstanding"], errors="coerce")
        df["ownership_pct"] = df["shares_held"] / df["shares_outstanding"] * 100
        df["prior_ownership_pct"] = df["prior_shares_held"] / df["shares_outstanding"] * 100
        df["ownership_pct_change"] = df["ownership_pct"] - df["prior_ownership_pct"]
        df["abs_ownership_pct_change"] = df["ownership_pct_change"].abs()

        df = df.sort_values(
            ["period_of_report", "abs_ownership_pct_change"],
            ascending=[True, False],
        )
        if top_n is not None:
            df = df.head(top_n)
        df = df.reset_index(drop=True)

        if plot:
            self._plot_ownership_change(df)

        return df

    def _latest_cover_pages(
        self,
        manager_cik: str | Iterable[str] | None,
        start_period: str | dt.date | dt.datetime | None,
        end_period: str | dt.date | dt.datetime | None,
    ) -> pd.DataFrame:
        where_sql, params, expanding = self._where_clause(
            table_alias="cp",
            manager_cik=manager_cik,
            start_period=start_period,
            end_period=end_period,
        )
        query = f"""
        WITH ranked AS (
            SELECT
                cp.accession_no,
                cp.cik,
                cp.company_name,
                cp.period_of_report,
                cp.filed_at,
                cp.form_type,
                cp.report_type,
                cp.table_value_total AS portfolio_value,
                cp.table_entry_total,
                COUNT(*) OVER (
                    PARTITION BY cp.cik, cp.period_of_report
                ) AS filing_versions,
                ROW_NUMBER() OVER (
                    PARTITION BY cp.cik, cp.period_of_report
                    ORDER BY cp.filed_at DESC, cp.accession_no DESC
                ) AS rn
            FROM cover_pages cp
            WHERE {where_sql}
        )
        SELECT
            accession_no,
            cik,
            company_name,
            period_of_report,
            filed_at,
            form_type,
            report_type,
            portfolio_value,
            table_entry_total,
            filing_versions
        FROM ranked
        WHERE rn = 1
        ORDER BY cik, period_of_report
        """
        return self._read_sql(query, params, expanding)

    def _latest_holdings(
        self,
        manager_cik: str | Iterable[str] | None,
        start_period: str | dt.date | dt.datetime | None,
        end_period: str | dt.date | dt.datetime | None,
        tickers: str | Iterable[str] | None = None,
        common_shares_only: bool = False,
    ) -> pd.DataFrame:
        where_sql, params, expanding = self._where_clause(
            table_alias="cp",
            manager_cik=manager_cik,
            start_period=start_period,
            end_period=end_period,
        )
        holding_filters = []
        normalized_tickers = self._normalize_string_filter(tickers)
        if normalized_tickers:
            holding_filters.append("h.ticker IN :tickers")
            params["tickers"] = normalized_tickers
            expanding.append("tickers")
        if common_shares_only:
            holding_filters.extend([
                "h.ticker IS NOT NULL",
                "h.ticker <> ''",
                "h.ssh_prnamt_type = 'SH'",
                "(h.put_call IS NULL OR h.put_call = '')",
            ])
        holding_where_sql = ""
        if holding_filters:
            holding_where_sql = "WHERE " + " AND ".join(holding_filters)

        query = f"""
        WITH latest_filings AS (
            SELECT *
            FROM (
                SELECT
                    cp.accession_no,
                    cp.cik,
                    cp.company_name,
                    cp.period_of_report,
                    cp.filed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY cp.cik, cp.period_of_report
                        ORDER BY cp.filed_at DESC, cp.accession_no DESC
                    ) AS rn
                FROM cover_pages cp
                WHERE {where_sql}
            ) ranked
            WHERE rn = 1
        )
        SELECT
            lf.accession_no,
            lf.cik AS manager_cik,
            lf.company_name AS manager_name,
            lf.period_of_report,
            lf.filed_at,
            h.ticker,
            h.cusip,
            h.name_of_issuer,
            h.title_of_class,
            h.value,
            h.ssh_prnamt,
            h.ssh_prnamt_type,
            h.put_call
        FROM latest_filings lf
        JOIN holdings h
          ON h.accession_no = lf.accession_no
        {holding_where_sql}
        """
        return self._read_sql(query, params, expanding)

    def _position_value_changes(
        self,
        holdings: pd.DataFrame,
        filings: pd.DataFrame,
        aggregate_managers: bool,
    ) -> pd.DataFrame:
        if holdings.empty or filings.empty:
            return pd.DataFrame()

        manager_cols = [] if aggregate_managers else ["manager_cik", "manager_name"]
        entity_cols = manager_cols + ["ticker", "cusip", "name_of_issuer", "title_of_class"]
        grouped = (
            holdings.assign(value=pd.to_numeric(holdings["value"], errors="coerce").fillna(0))
            .groupby(entity_cols + ["period_of_report"], dropna=False, as_index=False)
            .agg(value=("value", "sum"))
        )
        grid = self._period_entity_grid(grouped, filings, entity_cols, manager_cols)
        df = grid.merge(grouped, on=entity_cols + ["period_of_report"], how="left")
        df["value"] = df["value"].fillna(0)
        df = df.sort_values(entity_cols + ["period_of_report"]).reset_index(drop=True)
        df["prior_value"] = df.groupby(entity_cols, dropna=False)["value"].shift(1).fillna(0)
        df["value_change"] = df["value"] - df["prior_value"]
        df["abs_value_change"] = df["value_change"].abs()
        df["action"] = self._position_action(df["value"], df["prior_value"], df["value_change"])
        return df[df["value_change"] != 0].copy()

    def _position_share_changes(
        self,
        holdings: pd.DataFrame,
        filings: pd.DataFrame,
        aggregate_managers: bool,
    ) -> pd.DataFrame:
        if holdings.empty or filings.empty:
            return pd.DataFrame()

        manager_cols = [] if aggregate_managers else ["manager_cik", "manager_name"]
        entity_cols = manager_cols + ["ticker", "cusip", "name_of_issuer", "title_of_class"]
        grouped = (
            holdings.assign(shares_held=pd.to_numeric(holdings["ssh_prnamt"], errors="coerce").fillna(0))
            .groupby(entity_cols + ["period_of_report"], dropna=False, as_index=False)
            .agg(shares_held=("shares_held", "sum"))
        )
        grid = self._period_entity_grid(grouped, filings, entity_cols, manager_cols)
        df = grid.merge(grouped, on=entity_cols + ["period_of_report"], how="left")
        df["shares_held"] = df["shares_held"].fillna(0)
        df = df.sort_values(entity_cols + ["period_of_report"]).reset_index(drop=True)
        df["prior_shares_held"] = (
            df.groupby(entity_cols, dropna=False)["shares_held"].shift(1).fillna(0)
        )
        df["shares_change"] = df["shares_held"] - df["prior_shares_held"]
        return df[df["shares_change"] != 0].copy()

    def _period_entity_grid(
        self,
        grouped: pd.DataFrame,
        filings: pd.DataFrame,
        entity_cols: list[str],
        manager_cols: list[str],
    ) -> pd.DataFrame:
        if manager_cols:
            periods = filings.rename(
                columns={"cik": "manager_cik", "company_name": "manager_name"}
            )[manager_cols + ["period_of_report"]].drop_duplicates()
            entities = grouped[entity_cols].drop_duplicates()
            return entities.merge(periods, on=manager_cols, how="inner")

        periods = filings[["period_of_report"]].drop_duplicates()
        entities = grouped[entity_cols].drop_duplicates()
        return entities.merge(periods, how="cross")

    def _position_action(
        self,
        value: pd.Series,
        prior_value: pd.Series,
        value_change: pd.Series,
    ) -> pd.Series:
        action = pd.Series("unchanged", index=value.index)
        action[(prior_value <= 0) & (value > 0)] = "new"
        action[(value <= 0) & (prior_value > 0)] = "exited"
        action[(prior_value > 0) & (value > 0) & (value_change > 0)] = "bought"
        action[(prior_value > 0) & (value > 0) & (value_change < 0)] = "sold"
        return action

    def _where_clause(
        self,
        table_alias: str,
        manager_cik: str | Iterable[str] | None,
        start_period: str | dt.date | dt.datetime | None,
        end_period: str | dt.date | dt.datetime | None,
    ) -> tuple[str, dict[str, Any], list[str]]:
        clauses = ["1 = 1"]
        params: dict[str, Any] = {}
        expanding: list[str] = []

        if start_period is not None:
            clauses.append(f"{table_alias}.period_of_report >= :start_period")
            params["start_period"] = parse_date(start_period)
        if end_period is not None:
            clauses.append(f"{table_alias}.period_of_report <= :end_period")
            params["end_period"] = parse_date(end_period)

        manager_ciks = self._normalize_string_filter(manager_cik)
        if manager_ciks:
            clauses.append(f"{table_alias}.cik IN :manager_ciks")
            params["manager_ciks"] = manager_ciks
            expanding.append("manager_ciks")

        return " AND ".join(clauses), params, expanding

    def _normalize_string_filter(self, value: str | Iterable[str] | None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return [str(item) for item in value if item is not None]

    def _read_sql(
        self,
        query: str,
        params: dict[str, Any],
        expanding: list[str],
    ) -> pd.DataFrame:
        statement = text(query)
        for key in expanding:
            statement = statement.bindparams(bindparam(key, expanding=True))
        df = pd.read_sql(statement, con=self.engine, params=params)
        for column in ("period_of_report", "filed_at"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column])
        return df

    def _shares_outstanding(self, ticker: str) -> int | float | None:
        normalized = ticker.upper()
        if normalized not in self._shares_outstanding_cache:
            try:
                from .fundamentals import current_float
            except ImportError:
                from market_data.fundamentals import current_float

            try:
                _, shares = current_float(normalized)
            except Exception:
                shares = None
            self._shares_outstanding_cache[normalized] = shares
        return self._shares_outstanding_cache[normalized]

    def _plot_manager_performance(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        plot_df = df.pivot_table(
            index="period_of_report",
            columns="company_name",
            values="portfolio_value",
            aggfunc="sum",
        )
        plot_df.plot(title="13F Portfolio Value")

    def _plot_bought_sold(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        plot_df = df.groupby(["period_of_report", "action"])["value_change"].sum().unstack(fill_value=0)
        plot_df.plot(kind="bar", title="13F QoQ Value Bought/Sold")

    def _plot_ownership_change(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        plot_df = df.pivot_table(
            index="period_of_report",
            columns="ticker",
            values="ownership_pct",
            aggfunc="sum",
        )
        plot_df.plot(title="13F Ownership % of Shares Outstanding")

@dataclass
class Form4ImportStats:
    """Summary returned by Form 4 import methods."""

    symbols: int = 0
    symbol_batches: int = 0
    date_windows: int = 0
    requests: int = 0
    filings: int = 0
    filing_rows: int = 0
    transaction_rows: int = 0
    deleted_transaction_rows: int = 0
    skipped_transaction_rows: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbols": self.symbols,
            "symbol_batches": self.symbol_batches,
            "date_windows": self.date_windows,
            "requests": self.requests,
            "filings": self.filings,
            "filing_rows": self.filing_rows,
            "transaction_rows": self.transaction_rows,
            "deleted_transaction_rows": self.deleted_transaction_rows,
            "skipped_transaction_rows": self.skipped_transaction_rows,
            "errors": self.errors,
        }


class Form4Database:
    """
    MySQL storage layer for sec-api.io Form 4 insider trading filings.

    The normalized model keeps one row per filing in `filings` and one row per
    analyzable open-market non-derivative purchase/sale in
    `non_derivative_transactions`.
    """

    FILING_COLUMNS = (
        "accession_no",
        "document_type",
        "period_of_report",
        "filed_at",
        "issuer_cik",
        "issuer_name",
        "issuer_trading_symbol",
        "reporting_owner_cik",
        "reporting_owner_name",
        "owner_is_director",
        "owner_is_officer",
        "owner_is_ten_percent_owner",
        "owner_is_other",
        "owner_officer_title",
        "owner_other_text",
        "not_subject_to_section16",
        "aff10b5_one",
        "owner_signature_name",
        "owner_signature_date",
        "remarks",
        "footnotes_json",
        "raw_json",
        "source_key",
        "source_updated_at",
    )
    TRANSACTION_COLUMNS = (
        "accession_no",
        "line_number",
        "document_type",
        "period_of_report",
        "filed_at",
        "issuer_cik",
        "issuer_name",
        "issuer_trading_symbol",
        "reporting_owner_cik",
        "reporting_owner_name",
        "owner_is_director",
        "owner_is_officer",
        "owner_is_ten_percent_owner",
        "owner_is_other",
        "owner_officer_title",
        "security_title",
        "transaction_date",
        "transaction_code",
        "transaction_form_type",
        "equity_swap_involved",
        "timeliness",
        "acquired_disposed_code",
        "shares",
        "price_per_share",
        "transaction_value",
        "shares_owned_following_transaction",
        "direct_or_indirect_ownership",
        "nature_of_ownership",
        "aff10b5_one",
        "raw_transaction_json",
        "raw_filing_json",
        "source_key",
        "source_updated_at",
    )

    def __init__(
        self,
        database: str = FORM4_DB,
        user: str = GPTDB_MYSQL_USER,
        password: str = gptdb,
        host: str = MYSQL_HOST,
        port: int = MYSQL_PORT,
        engine: Engine | None = None,
    ) -> None:
        self.database = database
        self.engine = engine or self._make_engine(database, user, password, host, port)

    def _make_engine(
        self,
        database: str,
        user: str,
        password: str,
        host: str,
        port: int,
    ) -> Engine:
        url = URL.create(
            "mysql+pymysql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})

    def setup(self) -> None:
        """Create Form 4 tables if they do not already exist."""
        with self.engine.begin() as conn:
            conn.execute(text(self._filings_ddl()))
            conn.execute(text(self._transactions_ddl()))

    def latest_filed_at(self) -> dt.datetime | None:
        query = text("SELECT MAX(filed_at) FROM filings")
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return value

    def row_counts(self) -> dict[str, int]:
        with self.engine.connect() as conn:
            return {
                "filings": int(conn.execute(text("SELECT COUNT(*) FROM filings")).scalar() or 0),
                "non_derivative_transactions": int(
                    conn.execute(text("SELECT COUNT(*) FROM non_derivative_transactions")).scalar() or 0
                ),
            }

    def upsert_filings(self, rows: list[dict[str, Any]], batch_size: int = 1000) -> int:
        if not rows:
            return 0

        statement = text(self._upsert_sql("filings", self.FILING_COLUMNS, ("accession_no",)))
        inserted = 0
        with self.engine.begin() as conn:
            for batch in chunked(rows, batch_size):
                conn.execute(statement, batch)
                inserted += len(batch)
        return inserted

    def replace_transactions_for_filings(
        self,
        rows: list[dict[str, Any]],
        batch_size: int = 2000,
    ) -> tuple[int, int, int]:
        if not rows:
            return 0, 0, 0

        accessions = sorted({row["accession_no"] for row in rows if row.get("accession_no")})
        if not accessions:
            return 0, 0, len(rows)

        valid_accessions = self.existing_accessions(accessions)
        insertable_rows = [row for row in rows if row.get("accession_no") in valid_accessions]
        skipped_rows = len(rows) - len(insertable_rows)

        if not insertable_rows:
            return 0, 0, skipped_rows

        delete_statement = (
            text("DELETE FROM non_derivative_transactions WHERE accession_no IN :accession_nos")
            .bindparams(bindparam("accession_nos", expanding=True))
        )
        insert_statement = text(
            self._upsert_sql(
                "non_derivative_transactions",
                self.TRANSACTION_COLUMNS,
                ("accession_no", "line_number"),
            )
        )

        inserted = 0
        deleted = 0
        with self.engine.begin() as conn:
            for batch in chunked(sorted(valid_accessions), 1000):
                result = conn.execute(delete_statement, {"accession_nos": batch})
                deleted += result.rowcount or 0
            for batch in chunked(insertable_rows, batch_size):
                conn.execute(insert_statement, batch)
                inserted += len(batch)

        return inserted, deleted, skipped_rows

    def existing_accessions(self, accession_nos: Iterable[str]) -> set[str]:
        accessions = [accession for accession in accession_nos if accession]
        if not accessions:
            return set()

        query = (
            text("SELECT accession_no FROM filings WHERE accession_no IN :accession_nos")
            .bindparams(bindparam("accession_nos", expanding=True))
        )
        found: set[str] = set()
        with self.engine.connect() as conn:
            for batch in chunked([{"accession_no": value} for value in accessions], 1000):
                values = [row["accession_no"] for row in batch]
                found.update(str(row[0]) for row in conn.execute(query, {"accession_nos": values}))
        return found

    def _upsert_sql(self, table_name: str, columns: tuple[str, ...], key_columns: tuple[str, ...]) -> str:
        quoted_columns = ", ".join(mysql_identifier(column) for column in columns)
        values = ", ".join(f":{column}" for column in columns)
        update_columns = [column for column in columns if column not in key_columns]
        updates = ", ".join(
            f"{mysql_identifier(column)} = VALUES({mysql_identifier(column)})"
            for column in update_columns
        )
        updates = f"{updates}, loaded_at = CURRENT_TIMESTAMP(6)"
        return (
            f"INSERT INTO {mysql_identifier(table_name)} ({quoted_columns}) "
            f"VALUES ({values}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )

    def _filings_ddl(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS filings (
            accession_no VARCHAR(25) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            document_type VARCHAR(20) NULL,
            period_of_report DATE NULL,
            filed_at DATETIME(6) NULL,
            issuer_cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            issuer_name VARCHAR(255) NULL,
            issuer_trading_symbol VARCHAR(32) NULL,
            reporting_owner_cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            reporting_owner_name VARCHAR(255) NULL,
            owner_is_director BOOLEAN NOT NULL DEFAULT FALSE,
            owner_is_officer BOOLEAN NOT NULL DEFAULT FALSE,
            owner_is_ten_percent_owner BOOLEAN NOT NULL DEFAULT FALSE,
            owner_is_other BOOLEAN NOT NULL DEFAULT FALSE,
            owner_officer_title VARCHAR(255) NULL,
            owner_other_text VARCHAR(255) NULL,
            not_subject_to_section16 BOOLEAN NOT NULL DEFAULT FALSE,
            aff10b5_one BOOLEAN NOT NULL DEFAULT FALSE,
            owner_signature_name VARCHAR(255) NULL,
            owner_signature_date DATE NULL,
            remarks TEXT NULL,
            footnotes_json JSON NULL,
            raw_json JSON NULL,
            source_key VARCHAR(255) NULL,
            source_updated_at DATETIME(6) NULL,
            loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (accession_no),
            KEY idx_form4_filings_symbol (issuer_trading_symbol),
            KEY idx_form4_filings_filed_at (filed_at),
            KEY idx_form4_filings_period (period_of_report),
            KEY idx_form4_filings_owner_cik (reporting_owner_cik),
            KEY idx_form4_filings_symbol_filed (issuer_trading_symbol, filed_at)
        ) ENGINE = InnoDB
        """

    def _transactions_ddl(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS non_derivative_transactions (
            accession_no VARCHAR(25) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            line_number INT UNSIGNED NOT NULL,
            document_type VARCHAR(20) NULL,
            period_of_report DATE NULL,
            filed_at DATETIME(6) NULL,
            issuer_cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            issuer_name VARCHAR(255) NULL,
            issuer_trading_symbol VARCHAR(32) NULL,
            reporting_owner_cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            reporting_owner_name VARCHAR(255) NULL,
            owner_is_director BOOLEAN NOT NULL DEFAULT FALSE,
            owner_is_officer BOOLEAN NOT NULL DEFAULT FALSE,
            owner_is_ten_percent_owner BOOLEAN NOT NULL DEFAULT FALSE,
            owner_is_other BOOLEAN NOT NULL DEFAULT FALSE,
            owner_officer_title VARCHAR(255) NULL,
            security_title VARCHAR(255) NULL,
            transaction_date DATE NULL,
            transaction_code VARCHAR(8) NULL,
            transaction_form_type VARCHAR(20) NULL,
            equity_swap_involved BOOLEAN NOT NULL DEFAULT FALSE,
            timeliness VARCHAR(8) NULL,
            acquired_disposed_code CHAR(1) NULL,
            shares DECIMAL(24, 4) NULL,
            price_per_share DECIMAL(24, 6) NULL,
            transaction_value DECIMAL(28, 6) NULL,
            shares_owned_following_transaction DECIMAL(24, 4) NULL,
            direct_or_indirect_ownership CHAR(1) NULL,
            nature_of_ownership VARCHAR(255) NULL,
            aff10b5_one BOOLEAN NOT NULL DEFAULT FALSE,
            raw_transaction_json JSON NULL,
            raw_filing_json JSON NULL,
            source_key VARCHAR(255) NULL,
            source_updated_at DATETIME(6) NULL,
            loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (accession_no, line_number),
            KEY idx_form4_txn_symbol (issuer_trading_symbol),
            KEY idx_form4_txn_filed_at (filed_at),
            KEY idx_form4_txn_transaction_date (transaction_date),
            KEY idx_form4_txn_code (transaction_code),
            KEY idx_form4_txn_acquired_disposed (acquired_disposed_code),
            KEY idx_form4_txn_owner_cik (reporting_owner_cik),
            KEY idx_form4_txn_symbol_date (issuer_trading_symbol, transaction_date),
            CONSTRAINT fk_form4_transactions_filings
                FOREIGN KEY (accession_no)
                REFERENCES filings (accession_no)
                ON DELETE CASCADE
        ) ENGINE = InnoDB
        """


class Form4Normalizer:
    """Normalize sec-api.io Form 4 insider-trading JSON into database rows."""

    @staticmethod
    def filing_row(
        record: Mapping[str, Any],
        source_key: str | None = None,
        source_updated_at: Any = None,
    ) -> dict[str, Any]:
        issuer = first_mapping(record.get("issuer"))
        owner = first_mapping(record.get("reportingOwner"))
        relationship = first_mapping(owner.get("relationship"))

        return {
            "accession_no": record.get("accessionNo"),
            "document_type": record.get("documentType"),
            "period_of_report": parse_date(record.get("periodOfReport")),
            "filed_at": parse_datetime(record.get("filedAt")),
            "issuer_cik": issuer.get("cik"),
            "issuer_name": issuer.get("name"),
            "issuer_trading_symbol": Form4Normalizer._normalize_symbol(issuer.get("tradingSymbol")),
            "reporting_owner_cik": owner.get("cik"),
            "reporting_owner_name": owner.get("name"),
            "owner_is_director": parse_bool(relationship.get("isDirector")),
            "owner_is_officer": parse_bool(relationship.get("isOfficer")),
            "owner_is_ten_percent_owner": parse_bool(relationship.get("isTenPercentOwner")),
            "owner_is_other": parse_bool(relationship.get("isOther")),
            "owner_officer_title": relationship.get("officerTitle"),
            "owner_other_text": relationship.get("otherText"),
            "not_subject_to_section16": parse_bool(record.get("notSubjectToSection16")),
            "aff10b5_one": parse_bool(record.get("aff10b5One")),
            "owner_signature_name": record.get("ownerSignatureName"),
            "owner_signature_date": parse_date(record.get("ownerSignatureNameDate")),
            "remarks": record.get("remarks"),
            "footnotes_json": json_dumps(record.get("footnotes")),
            "raw_json": json_dumps(record),
            "source_key": source_key,
            "source_updated_at": parse_datetime(source_updated_at),
        }

    @staticmethod
    def non_derivative_transaction_rows(
        record: Mapping[str, Any],
        transaction_codes: Iterable[str] = ("P", "S"),
        source_key: str | None = None,
        source_updated_at: Any = None,
    ) -> list[dict[str, Any]]:
        issuer = first_mapping(record.get("issuer"))
        owner = first_mapping(record.get("reportingOwner"))
        relationship = first_mapping(owner.get("relationship"))
        allowed_codes = {str(code).upper() for code in transaction_codes}
        raw_filing_json = json_dumps(without_key(record, "nonDerivativeTable"))
        table = first_mapping(record.get("nonDerivativeTable"))
        rows: list[dict[str, Any]] = []

        filing_values = {
            "accession_no": record.get("accessionNo"),
            "document_type": record.get("documentType"),
            "period_of_report": parse_date(record.get("periodOfReport")),
            "filed_at": parse_datetime(record.get("filedAt")),
            "issuer_cik": issuer.get("cik"),
            "issuer_name": issuer.get("name"),
            "issuer_trading_symbol": Form4Normalizer._normalize_symbol(issuer.get("tradingSymbol")),
            "reporting_owner_cik": owner.get("cik"),
            "reporting_owner_name": owner.get("name"),
            "owner_is_director": parse_bool(relationship.get("isDirector")),
            "owner_is_officer": parse_bool(relationship.get("isOfficer")),
            "owner_is_ten_percent_owner": parse_bool(relationship.get("isTenPercentOwner")),
            "owner_is_other": parse_bool(relationship.get("isOther")),
            "owner_officer_title": relationship.get("officerTitle"),
            "aff10b5_one": parse_bool(record.get("aff10b5One")),
            "source_key": source_key,
            "source_updated_at": parse_datetime(source_updated_at),
        }

        for line_number, transaction in enumerate(table.get("transactions") or [], start=1):
            if not isinstance(transaction, Mapping):
                continue

            coding = first_mapping(transaction.get("coding"))
            transaction_code = str(coding.get("code") or "").upper()
            if allowed_codes and transaction_code not in allowed_codes:
                continue

            amounts = first_mapping(transaction.get("amounts"))
            post_amounts = first_mapping(transaction.get("postTransactionAmounts"))
            ownership = first_mapping(transaction.get("ownershipNature"))
            shares = parse_decimal(amounts.get("shares"))
            price = parse_decimal(amounts.get("pricePerShare"))
            transaction_value = shares * price if shares is not None and price is not None else None

            rows.append(
                {
                    **filing_values,
                    "line_number": line_number,
                    "security_title": transaction.get("securityTitle"),
                    "transaction_date": parse_date(transaction.get("transactionDate")),
                    "transaction_code": transaction_code or None,
                    "transaction_form_type": coding.get("formType"),
                    "equity_swap_involved": parse_bool(coding.get("equitySwapInvolved")),
                    "timeliness": transaction.get("timeliness"),
                    "acquired_disposed_code": amounts.get("acquiredDisposedCode"),
                    "shares": shares,
                    "price_per_share": price,
                    "transaction_value": transaction_value,
                    "shares_owned_following_transaction": parse_decimal(
                        post_amounts.get("sharesOwnedFollowingTransaction")
                    ),
                    "direct_or_indirect_ownership": ownership.get("directOrIndirectOwnership"),
                    "nature_of_ownership": ownership.get("natureOfOwnership"),
                    "raw_transaction_json": json_dumps(transaction),
                    "raw_filing_json": raw_filing_json,
                }
            )

        return rows

    @staticmethod
    def _normalize_symbol(value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip().upper()


class Form4Importer:
    """
    Import Form 4 insider purchases and sales from sec-api.io.

    Public import methods accept explicit date ranges. If no range is supplied,
    imports default to the trailing `lookback_days` ending today.
    """

    VALID_DATE_FIELDS = {
        "filedAt",
        "periodOfReport",
        "nonDerivativeTable.transactions.transactionDate",
    }

    def __init__(
        self,
        api_key: str = sec_api_key,
        database: Form4Database | None = None,
        normalizer: Form4Normalizer | None = None,
        batch_size: int = 1000,
        symbol_batch_size: int = 25,
        timeout: int = 120,
        min_request_interval: float = 2.0,
        max_retries: int = 6,
        retry_backoff: float = 5.0,
        max_retry_sleep: float = 120.0,
        show_progress: bool = True,
    ) -> None:
        self.api_key = api_key
        self.database = database or Form4Database()
        self.normalizer = normalizer or Form4Normalizer()
        self.batch_size = batch_size
        self.symbol_batch_size = symbol_batch_size
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_sleep = max_retry_sleep
        self.show_progress = show_progress
        self._last_request_at = 0.0
        self._last_page_requests = 0
        self.session = requests.Session()

    def import_watchlist(
        self,
        watchlist_location: str | None = None,
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
        lookback_days: int = DEFAULT_FORM4_LOOKBACK_DAYS,
        date_field: str = "filedAt",
        transaction_codes: Iterable[str] = ("P", "S"),
        document_types: Iterable[str] = ("4", "4/A"),
    ) -> dict[str, Any]:
        """Import Form 4 transactions for symbols from a watchlist file."""
        symbols = self._load_watchlist(watchlist_location)
        return self.import_symbols(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            date_field=date_field,
            transaction_codes=transaction_codes,
            document_types=document_types,
        )

    def import_symbols(
        self,
        symbols: Iterable[str],
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
        lookback_days: int = DEFAULT_FORM4_LOOKBACK_DAYS,
        date_field: str = "filedAt",
        transaction_codes: Iterable[str] = ("P", "S"),
        document_types: Iterable[str] = ("4", "4/A"),
    ) -> dict[str, Any]:
        """Import a symbol set over an explicit or default trailing date range."""
        start, end = self._resolve_date_range(start_date, end_date, lookback_days)
        return self.import_date_range(
            symbols=symbols,
            start_date=start,
            end_date=end,
            date_field=date_field,
            transaction_codes=transaction_codes,
            document_types=document_types,
        )

    def import_date_range(
        self,
        symbols: Iterable[str],
        start_date: str | dt.date | dt.datetime,
        end_date: str | dt.date | dt.datetime,
        date_field: str = "filedAt",
        transaction_codes: Iterable[str] = ("P", "S"),
        document_types: Iterable[str] = ("4", "4/A"),
    ) -> dict[str, Any]:
        """Import Form 4 transactions for `symbols` between `start_date` and `end_date`."""
        self.database.setup()
        self._validate_date_field(date_field)
        start = parse_date(start_date)
        end = parse_date(end_date)
        if start is None or end is None:
            raise ValueError("start_date and end_date are required")
        if end < start:
            raise ValueError("end_date must be on or after start_date")

        normalized_symbols = self._normalize_symbols(symbols)
        if not normalized_symbols:
            raise ValueError("At least one symbol is required")
        stats = Form4ImportStats(symbols=len(normalized_symbols))
        symbol_batches = list(self._symbol_batches(normalized_symbols))
        progress = self._progress(symbol_batches, desc="Form 4 symbol batches", unit="batch")

        for symbol_batch in progress:
            stats.symbol_batches += 1
            self._set_progress_postfix(progress, symbols=len(symbol_batch), filings=stats.filings)
            self._import_symbol_batch_date_window(
                symbol_batch=symbol_batch,
                start_date=start,
                end_date=end,
                date_field=date_field,
                transaction_codes=transaction_codes,
                document_types=document_types,
                stats=stats,
            )

        return stats.as_dict()

    def import_filed_since(
        self,
        symbols: Iterable[str],
        filed_since: str | dt.date | dt.datetime,
        filed_until: str | dt.date | dt.datetime | None = None,
        transaction_codes: Iterable[str] = ("P", "S"),
        document_types: Iterable[str] = ("4", "4/A"),
    ) -> dict[str, Any]:
        """Import filings submitted in a filedAt date range."""
        end = parse_date(filed_until) if filed_until is not None else dt.date.today()
        return self.import_date_range(
            symbols=symbols,
            start_date=filed_since,
            end_date=end,
            date_field="filedAt",
            transaction_codes=transaction_codes,
            document_types=document_types,
        )

    def import_since_latest(
        self,
        symbols: Iterable[str],
        fallback_lookback_days: int = DEFAULT_FORM4_LOOKBACK_DAYS,
        overlap_days: int = 3,
        transaction_codes: Iterable[str] = ("P", "S"),
        document_types: Iterable[str] = ("4", "4/A"),
    ) -> dict[str, Any]:
        """Import from the latest stored filedAt timestamp, with overlap for amended/late rows."""
        self.database.setup()
        latest = self.database.latest_filed_at()
        end = dt.date.today()
        if latest is None:
            start = end - dt.timedelta(days=fallback_lookback_days)
        else:
            start = latest.date() - dt.timedelta(days=overlap_days)
        return self.import_date_range(
            symbols=symbols,
            start_date=start,
            end_date=end,
            date_field="filedAt",
            transaction_codes=transaction_codes,
            document_types=document_types,
        )

    def _import_symbol_batch_date_window(
        self,
        symbol_batch: list[str],
        start_date: dt.date,
        end_date: dt.date,
        date_field: str,
        transaction_codes: Iterable[str],
        document_types: Iterable[str],
        stats: Form4ImportStats,
    ) -> None:
        query = self._query(
            symbols=symbol_batch,
            start_date=start_date,
            end_date=end_date,
            date_field=date_field,
            transaction_codes=transaction_codes,
            document_types=document_types,
        )
        source_key = (
            f"insider-trading:{date_field}={start_date.isoformat()}..{end_date.isoformat()}:"
            f"symbols={','.join(symbol_batch)}"
        )
        try:
            self._import_query_or_split(
                query=query,
                source_key=source_key,
                start_date=start_date,
                end_date=end_date,
                date_field=date_field,
                symbol_batch=symbol_batch,
                transaction_codes=transaction_codes,
                document_types=document_types,
                stats=stats,
            )
        except Exception as exc:
            stats.errors.append(f"{source_key}: {exc}")

    def _import_query_or_split(
        self,
        query: str,
        source_key: str,
        start_date: dt.date,
        end_date: dt.date,
        date_field: str,
        symbol_batch: list[str],
        transaction_codes: Iterable[str],
        document_types: Iterable[str],
        stats: Form4ImportStats,
    ) -> None:
        try:
            records = self._paginate_search(query=query, sort=[{"filedAt": {"order": "asc"}}])
        except SearchResultLimitError:
            if start_date >= end_date:
                if len(symbol_batch) <= 1:
                    raise
                midpoint = max(1, len(symbol_batch) // 2)
                for smaller_batch in (symbol_batch[:midpoint], symbol_batch[midpoint:]):
                    self._import_symbol_batch_date_window(
                        symbol_batch=smaller_batch,
                        start_date=start_date,
                        end_date=end_date,
                        date_field=date_field,
                        transaction_codes=transaction_codes,
                        document_types=document_types,
                        stats=stats,
                    )
                return

            midpoint_date = start_date + ((end_date - start_date) // 2)
            self._import_symbol_batch_date_window(
                symbol_batch=symbol_batch,
                start_date=start_date,
                end_date=midpoint_date,
                date_field=date_field,
                transaction_codes=transaction_codes,
                document_types=document_types,
                stats=stats,
            )
            self._import_symbol_batch_date_window(
                symbol_batch=symbol_batch,
                start_date=midpoint_date + dt.timedelta(days=1),
                end_date=end_date,
                date_field=date_field,
                transaction_codes=transaction_codes,
                document_types=document_types,
                stats=stats,
            )
            return

        stats.requests += self._last_page_requests
        stats.date_windows += 1
        stats.filings += len(records)
        filing_rows = [self.normalizer.filing_row(record, source_key=source_key) for record in records]
        stats.filing_rows += self.database.upsert_filings(filing_rows, batch_size=self.batch_size)

        transaction_rows: list[dict[str, Any]] = []
        for record in records:
            transaction_rows.extend(
                self.normalizer.non_derivative_transaction_rows(
                    record,
                    transaction_codes=transaction_codes,
                    source_key=source_key,
                )
            )
        inserted, deleted, skipped = self.database.replace_transactions_for_filings(
            transaction_rows,
            batch_size=self.batch_size,
        )
        stats.transaction_rows += inserted
        stats.deleted_transaction_rows += deleted
        stats.skipped_transaction_rows += skipped

    def _paginate_search(
        self,
        query: str,
        sort: list[dict[str, Any]] | None = None,
        page_size: int = SEC_API_FORM4_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset = 0
        self._last_page_requests = 0

        while True:
            payload = {
                "query": query,
                "from": str(offset),
                "size": str(page_size),
            }
            if sort is not None:
                payload["sort"] = sort

            response = self._post_search(FORM4_INSIDER_TRADING_ENDPOINT, payload)
            self._last_page_requests += 1
            data = response.get("data", response.get("transactions", []))
            total_value = response.get("total")
            if isinstance(total_value, Mapping):
                total_payload = total_value
                total = int(total_payload.get("value") or len(data))
                relation = total_payload.get("relation")
            else:
                total = int(total_value or len(data))
                relation = "eq"
            if total > SEC_API_FORM4_FROM_LIMIT or (total >= SEC_API_FORM4_FROM_LIMIT and relation != "eq"):
                raise SearchResultLimitError(query, total)
            if not data:
                break

            records.extend(data)
            offset += page_size
            if len(data) < page_size or offset >= total:
                break

        return records

    def _post_search(self, endpoint: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._post(endpoint, payload) as response:
            return response.json()

    def _post(self, endpoint: str, payload: Mapping[str, Any]) -> requests.Response:
        retry_statuses = {429, 500, 502, 503, 504}
        last_response: requests.Response | None = None

        for attempt in range(self.max_retries + 1):
            self._wait_for_request_slot()
            response = self.session.post(
                endpoint,
                headers=self._headers(),
                json=dict(payload),
                timeout=self.timeout,
            )
            self._last_request_at = time.monotonic()
            last_response = response

            if response.status_code not in retry_statuses:
                response.raise_for_status()
                return response

            if attempt >= self.max_retries:
                response.raise_for_status()

            response.close()
            time.sleep(self._retry_sleep_seconds(response, attempt))

        if last_response is not None:
            last_response.raise_for_status()
        raise RuntimeError(f"Unable to retrieve {endpoint}")

    def _query(
        self,
        symbols: Iterable[str],
        start_date: dt.date,
        end_date: dt.date,
        date_field: str,
        transaction_codes: Iterable[str],
        document_types: Iterable[str],
    ) -> str:
        clauses = [
            self._document_type_clause(document_types),
            self._symbol_clause(symbols),
            self._transaction_code_clause(transaction_codes),
            f"{date_field}:[{start_date.isoformat()} TO {end_date.isoformat()}]",
        ]
        return " AND ".join(clause for clause in clauses if clause)

    def _document_type_clause(self, document_types: Iterable[str]) -> str:
        values = self._normalize_filter_values(document_types)
        if not values:
            return ""
        return "(" + " OR ".join(f"documentType:{self._lucene_value(value)}" for value in values) + ")"

    def _symbol_clause(self, symbols: Iterable[str]) -> str:
        values = self._normalize_symbols(symbols)
        if not values:
            raise ValueError("At least one symbol is required")
        return "(" + " OR ".join(f"issuer.tradingSymbol:{self._lucene_value(value)}" for value in values) + ")"

    def _transaction_code_clause(self, transaction_codes: Iterable[str]) -> str:
        values = self._normalize_filter_values(transaction_codes, uppercase=True)
        if not values:
            return ""
        return (
            "("
            + " OR ".join(f"nonDerivativeTable.transactions.coding.code:{self._lucene_value(value)}" for value in values)
            + ")"
        )

    def _resolve_date_range(
        self,
        start_date: str | dt.date | dt.datetime | None,
        end_date: str | dt.date | dt.datetime | None,
        lookback_days: int,
    ) -> tuple[dt.date, dt.date]:
        end = parse_date(end_date) if end_date is not None else dt.date.today()
        if end is None:
            raise ValueError("end_date could not be parsed")
        start = parse_date(start_date) if start_date is not None else end - dt.timedelta(days=lookback_days)
        if start is None:
            raise ValueError("start_date could not be parsed")
        if end < start:
            raise ValueError("end_date must be on or after start_date")
        return start, end

    def _validate_date_field(self, date_field: str) -> None:
        if date_field not in self.VALID_DATE_FIELDS:
            allowed = ", ".join(sorted(self.VALID_DATE_FIELDS))
            raise ValueError(f"date_field must be one of: {allowed}")

    def _load_watchlist(self, watchlist_location: str | None) -> list[str]:
        if watchlist_location is None:
            try:
                from .watchlists_locations import hadv, make_watchlist
            except ImportError:
                from market_data.watchlists_locations import hadv, make_watchlist

            watchlist_location = hadv
        else:
            try:
                from .watchlists_locations import make_watchlist
            except ImportError:
                from market_data.watchlists_locations import make_watchlist

        return make_watchlist(watchlist_location)

    def _normalize_symbols(self, symbols: Iterable[str]) -> list[str]:
        if isinstance(symbols, str):
            symbols = [symbols]
        normalized = []
        seen = set()
        for symbol in symbols:
            if symbol in (None, ""):
                continue
            value = str(symbol).strip().upper()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    def _normalize_filter_values(self, values: Iterable[str], uppercase: bool = False) -> list[str]:
        if isinstance(values, str):
            values = [values]
        normalized = []
        for value in values:
            text_value = str(value).strip()
            if not text_value:
                continue
            normalized.append(text_value.upper() if uppercase else text_value)
        return normalized

    def _symbol_batches(self, symbols: list[str]) -> Iterator[list[str]]:
        batch_size = max(1, self.symbol_batch_size)
        for i in range(0, len(symbols), batch_size):
            yield symbols[i:i + batch_size]

    def _lucene_value(self, value: str) -> str:
        if value.replace(".", "").replace("-", "").replace("_", "").isalnum():
            return value
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def _wait_for_request_slot(self) -> None:
        if self.min_request_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.min_request_interval - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _retry_sleep_seconds(self, response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), self.max_retry_sleep)
            except ValueError:
                pass

        exponential_sleep = self.retry_backoff * (2 ** attempt)
        return min(exponential_sleep, self.max_retry_sleep)

    def _progress(
        self,
        iterable: Iterable[Any],
        desc: str,
        unit: str,
        leave: bool = True,
    ) -> Iterable[Any]:
        if not self.show_progress or tqdm is None:
            return iterable
        return tqdm(iterable, desc=desc, unit=unit, leave=leave)

    def _set_progress_postfix(self, progress: Iterable[Any], **values: Any) -> None:
        if hasattr(progress, "set_postfix"):
            progress.set_postfix(**values)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self.api_key}


class Form4Analytics:
    """Analytics layer for normalized Form 4 insider transactions."""

    def __init__(self, database: Form4Database | None = None) -> None:
        self.database = database or Form4Database()
        self.engine = self.database.engine

    def buys_sells(
        self,
        symbols: str | Iterable[str] | None = None,
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
        owners: str | Iterable[str] | None = None,
        transaction_codes: Iterable[str] = ("P", "S"),
        min_value: int | float | Decimal | None = None,
        summary: bool = False,
    ) -> pd.DataFrame:
        """Return insider buys/sells for symbols over a transaction-date range."""
        where_sql, params, expanding = self._where_clause(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            owners=owners,
            transaction_codes=transaction_codes,
            min_value=min_value,
        )
        query = f"""
        SELECT
            issuer_trading_symbol AS symbol,
            issuer_name,
            reporting_owner_cik,
            reporting_owner_name,
            owner_is_director,
            owner_is_officer,
            owner_is_ten_percent_owner,
            owner_officer_title,
            filed_at,
            transaction_date,
            transaction_code,
            CASE
                WHEN transaction_code = 'P' THEN 'buy'
                WHEN transaction_code = 'S' THEN 'sell'
                ELSE 'other'
            END AS action,
            acquired_disposed_code,
            security_title,
            shares,
            price_per_share,
            transaction_value,
            shares_owned_following_transaction,
            direct_or_indirect_ownership,
            nature_of_ownership,
            aff10b5_one,
            accession_no,
            line_number
        FROM non_derivative_transactions
        WHERE {where_sql}
        ORDER BY transaction_date DESC, filed_at DESC, symbol, reporting_owner_name, line_number
        """
        df = self._read_sql(query, params, expanding)
        if summary and not df.empty:
            return self._summarize_buys_sells(df)
        return df

    def _where_clause(
        self,
        symbols: str | Iterable[str] | None,
        start_date: str | dt.date | dt.datetime | None,
        end_date: str | dt.date | dt.datetime | None,
        owners: str | Iterable[str] | None,
        transaction_codes: Iterable[str],
        min_value: int | float | Decimal | None,
    ) -> tuple[str, dict[str, Any], list[str]]:
        clauses = ["1 = 1"]
        params: dict[str, Any] = {}
        expanding: list[str] = []

        normalized_symbols = self._normalize_string_filter(symbols, uppercase=True)
        if normalized_symbols:
            clauses.append("issuer_trading_symbol IN :symbols")
            params["symbols"] = normalized_symbols
            expanding.append("symbols")

        normalized_owners = self._normalize_string_filter(owners)
        if normalized_owners:
            clauses.append("(reporting_owner_cik IN :owners OR reporting_owner_name IN :owners)")
            params["owners"] = normalized_owners
            expanding.append("owners")

        normalized_codes = self._normalize_string_filter(transaction_codes, uppercase=True)
        if normalized_codes:
            clauses.append("transaction_code IN :transaction_codes")
            params["transaction_codes"] = normalized_codes
            expanding.append("transaction_codes")

        if start_date is not None:
            clauses.append("transaction_date >= :start_date")
            params["start_date"] = parse_date(start_date)
        if end_date is not None:
            clauses.append("transaction_date <= :end_date")
            params["end_date"] = parse_date(end_date)
        if min_value is not None:
            clauses.append("ABS(transaction_value) >= :min_value")
            params["min_value"] = Decimal(str(min_value))

        return " AND ".join(clauses), params, expanding

    def _normalize_string_filter(
        self,
        value: str | Iterable[str] | None,
        uppercase: bool = False,
    ) -> list[str]:
        if value is None:
            return []
        values = [value] if isinstance(value, str) else list(value)
        normalized = []
        for item in values:
            if item is None:
                continue
            text_value = str(item).strip()
            if not text_value:
                continue
            normalized.append(text_value.upper() if uppercase else text_value)
        return normalized

    def _read_sql(
        self,
        query: str,
        params: dict[str, Any],
        expanding: list[str],
    ) -> pd.DataFrame:
        statement = text(query)
        for key in expanding:
            statement = statement.bindparams(bindparam(key, expanding=True))
        df = pd.read_sql(statement, con=self.engine, params=params)
        for column in ("transaction_date", "filed_at"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column])
        return df

    def _summarize_buys_sells(self, df: pd.DataFrame) -> pd.DataFrame:
        working = df.copy()
        working["shares"] = pd.to_numeric(working["shares"], errors="coerce").fillna(0)
        working["transaction_value"] = pd.to_numeric(working["transaction_value"], errors="coerce").fillna(0)
        return (
            working.groupby(["symbol", "action"], dropna=False, as_index=False)
            .agg(
                transactions=("accession_no", "count"),
                insiders=("reporting_owner_cik", "nunique"),
                shares=("shares", "sum"),
                transaction_value=("transaction_value", "sum"),
                first_transaction_date=("transaction_date", "min"),
                last_transaction_date=("transaction_date", "max"),
            )
            .sort_values(["symbol", "action"])
            .reset_index(drop=True)
        )


@dataclass
class ExecutiveCompensationImportStats:
    """Summary returned by executive compensation import methods."""

    symbols: int = 0
    symbol_batches: int = 0
    year_windows: int = 0
    requests: int = 0
    records: int = 0
    rows: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbols": self.symbols,
            "symbol_batches": self.symbol_batches,
            "year_windows": self.year_windows,
            "requests": self.requests,
            "records": self.records,
            "rows": self.rows,
            "errors": self.errors,
        }


class ExecutiveCompensationDatabase:
    """MySQL storage layer for sec-api.io executive compensation records."""

    COMPENSATION_COLUMNS = (
        "record_key",
        "ticker",
        "cik",
        "year",
        "name",
        "position",
        "salary",
        "bonus",
        "stock_awards",
        "option_awards",
        "non_equity_incentive_compensation",
        "change_in_pension_value_and_deferred_earnings",
        "other_compensation",
        "total",
        "raw_json",
        "source_key",
        "source_updated_at",
    )

    def __init__(
        self,
        database: str = EXECUTIVE_COMPENSATION_DB,
        user: str = GPTDB_MYSQL_USER,
        password: str = gptdb,
        host: str = MYSQL_HOST,
        port: int = MYSQL_PORT,
        engine: Engine | None = None,
    ) -> None:
        self.database = database
        self.engine = engine or self._make_engine(database, user, password, host, port)

    def _make_engine(
        self,
        database: str,
        user: str,
        password: str,
        host: str,
        port: int,
    ) -> Engine:
        url = URL.create(
            "mysql+pymysql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})

    def setup(self) -> None:
        """Create executive compensation tables if they do not already exist."""
        with self.engine.begin() as conn:
            conn.execute(text(self._compensation_records_ddl()))

    def latest_year(self) -> int | None:
        query = text("SELECT MAX(year) FROM compensation_records")
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return int(value) if value is not None else None

    def row_counts(self) -> dict[str, int]:
        with self.engine.connect() as conn:
            return {
                "compensation_records": int(
                    conn.execute(text("SELECT COUNT(*) FROM compensation_records")).scalar() or 0
                )
            }

    def upsert_compensation_records(self, rows: list[dict[str, Any]], batch_size: int = 1000) -> int:
        if not rows:
            return 0

        statement = text(self._upsert_sql("compensation_records", self.COMPENSATION_COLUMNS, ("record_key",)))
        inserted = 0
        with self.engine.begin() as conn:
            for batch in chunked(rows, batch_size):
                conn.execute(statement, batch)
                inserted += len(batch)
        return inserted

    def _upsert_sql(self, table_name: str, columns: tuple[str, ...], key_columns: tuple[str, ...]) -> str:
        quoted_columns = ", ".join(mysql_identifier(column) for column in columns)
        values = ", ".join(f":{column}" for column in columns)
        update_columns = [column for column in columns if column not in key_columns]
        updates = ", ".join(
            f"{mysql_identifier(column)} = VALUES({mysql_identifier(column)})"
            for column in update_columns
        )
        updates = f"{updates}, loaded_at = CURRENT_TIMESTAMP(6)"
        return (
            f"INSERT INTO {mysql_identifier(table_name)} ({quoted_columns}) "
            f"VALUES ({values}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )

    def _compensation_records_ddl(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS compensation_records (
            record_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            ticker VARCHAR(32) NULL,
            cik VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NULL,
            `year` SMALLINT UNSIGNED NOT NULL,
            name VARCHAR(255) NOT NULL,
            position VARCHAR(512) NULL,
            salary DECIMAL(28, 6) NULL,
            bonus DECIMAL(28, 6) NULL,
            stock_awards DECIMAL(28, 6) NULL,
            option_awards DECIMAL(28, 6) NULL,
            non_equity_incentive_compensation DECIMAL(28, 6) NULL,
            change_in_pension_value_and_deferred_earnings DECIMAL(28, 6) NULL,
            other_compensation DECIMAL(28, 6) NULL,
            total DECIMAL(28, 6) NULL,
            raw_json JSON NULL,
            source_key VARCHAR(1024) NULL,
            source_updated_at DATETIME(6) NULL,
            loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (record_key),
            KEY idx_exec_comp_ticker (ticker),
            KEY idx_exec_comp_cik (cik),
            KEY idx_exec_comp_year (`year`),
            KEY idx_exec_comp_ticker_year (ticker, `year`),
            KEY idx_exec_comp_cik_year (cik, `year`)
        ) ENGINE = InnoDB
        """


class ExecutiveCompensationNormalizer:
    """Normalize sec-api.io executive compensation JSON into database rows."""

    COMPENSATION_FIELD_MAP = {
        "salary": "salary",
        "bonus": "bonus",
        "stockAwards": "stock_awards",
        "optionAwards": "option_awards",
        "nonEquityIncentiveCompensation": "non_equity_incentive_compensation",
        "changeInPensionValueAndDeferredEarnings": "change_in_pension_value_and_deferred_earnings",
        "otherCompensation": "other_compensation",
        "total": "total",
    }

    @classmethod
    def compensation_row(
        cls,
        record: Mapping[str, Any],
        source_key: str | None = None,
        source_updated_at: Any = None,
    ) -> dict[str, Any]:
        ticker = cls._normalize_symbol(record.get("ticker"))
        cik = cls._normalize_cik(record.get("cik"))
        year = cls._parse_year(record.get("year"))
        name = str(record.get("name") or "").strip()
        position = str(record.get("position") or "").strip() or None
        row = {
            "record_key": cls._record_key(ticker, cik, year, name, position),
            "ticker": ticker,
            "cik": cik,
            "year": year,
            "name": name,
            "position": position,
            "raw_json": json_dumps(record),
            "source_key": source_key,
            "source_updated_at": parse_datetime(source_updated_at),
        }
        for source_field, target_field in cls.COMPENSATION_FIELD_MAP.items():
            row[target_field] = parse_decimal(record.get(source_field))
        return row

    @classmethod
    def compensation_rows(
        cls,
        records: Iterable[Mapping[str, Any]],
        source_key: str | None = None,
        source_updated_at: Any = None,
    ) -> list[dict[str, Any]]:
        return [
            cls.compensation_row(record, source_key=source_key, source_updated_at=source_updated_at)
            for record in records
            if isinstance(record, Mapping) and cls._parse_year(record.get("year")) is not None
        ]

    @staticmethod
    def _record_key(
        ticker: str | None,
        cik: str | None,
        year: int | None,
        name: str,
        position: str | None,
    ) -> str:
        natural_key = "|".join(
            (
                ticker or "",
                cik or "",
                str(year or ""),
                name.casefold(),
                (position or "").casefold(),
            )
        )
        return hashlib.sha256(natural_key.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_symbol(value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip().upper()

    @staticmethod
    def _normalize_cik(value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip()

    @staticmethod
    def _parse_year(value: Any) -> int | None:
        if value in (None, ""):
            return None
        return int(str(value).strip())


class ExecutiveCompensationImporter:
    """
    Import executive compensation records from sec-api.io.

    Requests are sequential and symbol-batched. Wide result sets are split by
    reporting year first, then by symbol batch to stay below sec-api's 10,000
    result window.
    """

    def __init__(
        self,
        api_key: str = sec_api_key,
        database: ExecutiveCompensationDatabase | None = None,
        normalizer: ExecutiveCompensationNormalizer | None = None,
        batch_size: int = 1000,
        symbol_batch_size: int = 25,
        timeout: int = 120,
        min_request_interval: float = 2.0,
        max_retries: int = 6,
        retry_backoff: float = 5.0,
        max_retry_sleep: float = 120.0,
        show_progress: bool = True,
    ) -> None:
        self.api_key = api_key
        self.database = database or ExecutiveCompensationDatabase()
        self.normalizer = normalizer or ExecutiveCompensationNormalizer()
        self.batch_size = batch_size
        self.symbol_batch_size = symbol_batch_size
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_sleep = max_retry_sleep
        self.show_progress = show_progress
        self._last_request_at = 0.0
        self._last_page_requests = 0
        self.session = requests.Session()

    def import_watchlist(
        self,
        watchlist_location: str | None = None,
        start_year: int | str | None = DEFAULT_EXEC_COMP_START_YEAR,
        end_year: int | str | None = None,
    ) -> dict[str, Any]:
        """Import executive compensation for symbols from a watchlist file."""
        return self.import_symbols(
            symbols=self._load_watchlist(watchlist_location),
            start_year=start_year,
            end_year=end_year,
        )

    def import_symbols(
        self,
        symbols: Iterable[str],
        start_year: int | str | None = DEFAULT_EXEC_COMP_START_YEAR,
        end_year: int | str | None = None,
    ) -> dict[str, Any]:
        """Import executive compensation for symbols over a reporting-year range."""
        start, end = self._resolve_year_range(start_year, end_year)
        return self.import_year_range(symbols=symbols, start_year=start, end_year=end)

    def import_recent_years(
        self,
        symbols: Iterable[str] | None = None,
        watchlist_location: str | None = None,
        years_back: int = 1,
    ) -> dict[str, Any]:
        """
        Refresh recent reporting years.

        `years_back=1` imports the current calendar year and the prior year.
        """
        if years_back < 0:
            raise ValueError("years_back must be >= 0")
        end = dt.date.today().year
        start = end - years_back
        if symbols is None:
            symbols = self._load_watchlist(watchlist_location)
        return self.import_year_range(symbols=symbols, start_year=start, end_year=end)

    def import_year_range(
        self,
        symbols: Iterable[str],
        start_year: int | str,
        end_year: int | str,
    ) -> dict[str, Any]:
        """Import executive compensation for `symbols` between reporting years."""
        self.database.setup()
        start, end = self._resolve_year_range(start_year, end_year)
        normalized_symbols = self._normalize_symbols(symbols)
        if not normalized_symbols:
            raise ValueError("At least one symbol is required")

        stats = ExecutiveCompensationImportStats(symbols=len(normalized_symbols))
        symbol_batches = list(self._symbol_batches(normalized_symbols))
        progress = self._progress(symbol_batches, desc="Exec comp symbol batches", unit="batch")
        for symbol_batch in progress:
            stats.symbol_batches += 1
            self._set_progress_postfix(progress, symbols=len(symbol_batch), records=stats.records)
            self._import_symbol_batch_year_window(
                symbol_batch=symbol_batch,
                start_year=start,
                end_year=end,
                stats=stats,
            )
        return stats.as_dict()

    def _import_symbol_batch_year_window(
        self,
        symbol_batch: list[str],
        start_year: int,
        end_year: int,
        stats: ExecutiveCompensationImportStats,
    ) -> None:
        query = self._query(symbol_batch, start_year, end_year)
        source_key = (
            f"compensation:year={start_year}..{end_year}:"
            f"symbols={','.join(symbol_batch)}"
        )
        try:
            self._import_query_or_split(
                query=query,
                source_key=source_key,
                start_year=start_year,
                end_year=end_year,
                symbol_batch=symbol_batch,
                stats=stats,
            )
        except Exception as exc:
            stats.errors.append(f"{source_key}: {exc}")

    def _import_query_or_split(
        self,
        query: str,
        source_key: str,
        start_year: int,
        end_year: int,
        symbol_batch: list[str],
        stats: ExecutiveCompensationImportStats,
    ) -> None:
        try:
            records = self._paginate_search(query=query, sort=[{"year": {"order": "asc"}}, {"ticker": {"order": "asc"}}])
        except SearchResultLimitError:
            if start_year < end_year:
                midpoint_year = start_year + ((end_year - start_year) // 2)
                self._import_symbol_batch_year_window(symbol_batch, start_year, midpoint_year, stats)
                self._import_symbol_batch_year_window(symbol_batch, midpoint_year + 1, end_year, stats)
                return

            if len(symbol_batch) > 1:
                midpoint = max(1, len(symbol_batch) // 2)
                for smaller_batch in (symbol_batch[:midpoint], symbol_batch[midpoint:]):
                    self._import_symbol_batch_year_window(smaller_batch, start_year, end_year, stats)
                return

            raise

        stats.requests += self._last_page_requests
        stats.year_windows += 1
        stats.records += len(records)
        rows = self.normalizer.compensation_rows(records, source_key=source_key)
        stats.rows += self.database.upsert_compensation_records(rows, batch_size=self.batch_size)

    def _paginate_search(
        self,
        query: str,
        sort: list[dict[str, Any]] | None = None,
        page_size: int = SEC_API_EXEC_COMP_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset = 0
        self._last_page_requests = 0

        while True:
            payload = {
                "query": query,
                "from": str(offset),
                "size": str(page_size),
            }
            if sort is not None:
                payload["sort"] = sort

            response = self._post_search(EXECUTIVE_COMPENSATION_ENDPOINT, payload)
            self._last_page_requests += 1
            if isinstance(response, list):
                data = response
                total = None
                relation = "eq"
            else:
                data = response.get("data", response.get("compensation", []))
                total_value = response.get("total")
                if isinstance(total_value, Mapping):
                    total_payload = total_value
                    total = int(total_payload.get("value") or len(data))
                    relation = total_payload.get("relation")
                else:
                    total = int(total_value) if total_value is not None else None
                    relation = "eq"

            if total is not None and (
                total > SEC_API_EXEC_COMP_FROM_LIMIT
                or (total >= SEC_API_EXEC_COMP_FROM_LIMIT and relation != "eq")
            ):
                raise SearchResultLimitError(query, total)
            if not data:
                break

            records.extend(data)
            offset += page_size
            if len(data) < page_size:
                break
            if total is not None and offset >= total:
                break
            if offset >= SEC_API_EXEC_COMP_FROM_LIMIT:
                raise SearchResultLimitError(query, offset)

        return records

    def _post_search(self, endpoint: str, payload: Mapping[str, Any]) -> dict[str, Any] | list[dict[str, Any]]:
        with self._post(endpoint, payload) as response:
            return response.json()

    def _post(self, endpoint: str, payload: Mapping[str, Any]) -> requests.Response:
        retry_statuses = {429, 500, 502, 503, 504}
        last_response: requests.Response | None = None

        for attempt in range(self.max_retries + 1):
            self._wait_for_request_slot()
            response = self.session.post(
                endpoint,
                headers=self._headers(),
                json=dict(payload),
                timeout=self.timeout,
            )
            self._last_request_at = time.monotonic()
            last_response = response

            if response.status_code not in retry_statuses:
                response.raise_for_status()
                return response

            if attempt >= self.max_retries:
                response.raise_for_status()

            response.close()
            time.sleep(self._retry_sleep_seconds(response, attempt))

        if last_response is not None:
            last_response.raise_for_status()
        raise RuntimeError(f"Unable to retrieve {endpoint}")

    def _query(self, symbols: Iterable[str], start_year: int, end_year: int) -> str:
        return f"year:[{int(start_year)} TO {int(end_year)}] AND {self._symbol_clause(symbols)}"

    def _symbol_clause(self, symbols: Iterable[str]) -> str:
        values = self._normalize_symbols(symbols)
        if not values:
            raise ValueError("At least one symbol is required")
        if len(values) == 1:
            return f"ticker:{self._lucene_value(values[0])}"
        return "ticker:(" + " OR ".join(self._lucene_value(value) for value in values) + ")"

    def _resolve_year_range(
        self,
        start_year: int | str | None,
        end_year: int | str | None,
    ) -> tuple[int, int]:
        end = int(end_year) if end_year is not None else dt.date.today().year
        start = int(start_year) if start_year is not None else DEFAULT_EXEC_COMP_START_YEAR
        if start < DEFAULT_EXEC_COMP_START_YEAR:
            start = DEFAULT_EXEC_COMP_START_YEAR
        if end < start:
            raise ValueError("end_year must be on or after start_year")
        return start, end

    def _load_watchlist(self, watchlist_location: str | None) -> list[str]:
        if watchlist_location is None:
            try:
                from .watchlists_locations import hadv, make_watchlist
            except ImportError:
                from market_data.watchlists_locations import hadv, make_watchlist

            watchlist_location = hadv
        else:
            try:
                from .watchlists_locations import make_watchlist
            except ImportError:
                from market_data.watchlists_locations import make_watchlist

        return make_watchlist(watchlist_location)

    def _normalize_symbols(self, symbols: Iterable[str]) -> list[str]:
        if isinstance(symbols, str):
            symbols = [symbols]
        normalized = []
        seen = set()
        for symbol in symbols:
            if symbol in (None, ""):
                continue
            value = str(symbol).strip().upper()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    def _symbol_batches(self, symbols: list[str]) -> Iterator[list[str]]:
        batch_size = max(1, self.symbol_batch_size)
        for i in range(0, len(symbols), batch_size):
            yield symbols[i:i + batch_size]

    def _lucene_value(self, value: str) -> str:
        if value.replace(".", "").replace("-", "").replace("_", "").isalnum():
            return value
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def _wait_for_request_slot(self) -> None:
        if self.min_request_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.min_request_interval - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _retry_sleep_seconds(self, response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), self.max_retry_sleep)
            except ValueError:
                pass

        exponential_sleep = self.retry_backoff * (2 ** attempt)
        return min(exponential_sleep, self.max_retry_sleep)

    def _progress(
        self,
        iterable: Iterable[Any],
        desc: str,
        unit: str,
        leave: bool = True,
    ) -> Iterable[Any]:
        if not self.show_progress or tqdm is None:
            return iterable
        return tqdm(iterable, desc=desc, unit=unit, leave=leave)

    def _set_progress_postfix(self, progress: Iterable[Any], **values: Any) -> None:
        if hasattr(progress, "set_postfix"):
            progress.set_postfix(**values)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self.api_key}


class ExecutiveCompensationAnalytics:
    """Analytics layer for normalized executive compensation records."""

    def __init__(self, database: ExecutiveCompensationDatabase | None = None) -> None:
        self.database = database or ExecutiveCompensationDatabase()
        self.engine = self.database.engine

    def total_compensation(
        self,
        symbols: str | Iterable[str] | None = None,
        start_year: int | str | None = None,
        end_year: int | str | None = None,
        plot: bool = False,
        detail: bool = False,
    ) -> pd.DataFrame:
        """
        Return total executive compensation for symbols over reporting years.

        By default the result is one row per symbol/year, summing compensation
        across all reported executives. Use `detail=True` for executive rows.
        """
        where_sql, params, expanding = self._where_clause(symbols, start_year, end_year)
        if detail:
            query = f"""
            SELECT
                ticker AS symbol,
                cik,
                `year`,
                name,
                position,
                salary,
                bonus,
                stock_awards,
                option_awards,
                non_equity_incentive_compensation,
                change_in_pension_value_and_deferred_earnings,
                other_compensation,
                total
            FROM compensation_records
            WHERE {where_sql}
            ORDER BY symbol, `year`, total DESC, name
            """
            df = self._read_sql(query, params, expanding)
        else:
            query = f"""
            SELECT
                ticker AS symbol,
                MIN(cik) AS cik,
                `year`,
                COUNT(*) AS executive_count,
                SUM(salary) AS salary,
                SUM(bonus) AS bonus,
                SUM(stock_awards) AS stock_awards,
                SUM(option_awards) AS option_awards,
                SUM(non_equity_incentive_compensation) AS non_equity_incentive_compensation,
                SUM(change_in_pension_value_and_deferred_earnings)
                    AS change_in_pension_value_and_deferred_earnings,
                SUM(other_compensation) AS other_compensation,
                SUM(total) AS total_compensation
            FROM compensation_records
            WHERE {where_sql}
            GROUP BY ticker, `year`
            ORDER BY symbol, `year`
            """
            df = self._read_sql(query, params, expanding)

        if plot:
            self._plot_total_compensation(df, detail=detail)
        return df

    def _where_clause(
        self,
        symbols: str | Iterable[str] | None,
        start_year: int | str | None,
        end_year: int | str | None,
    ) -> tuple[str, dict[str, Any], list[str]]:
        clauses = ["1 = 1"]
        params: dict[str, Any] = {}
        expanding: list[str] = []

        normalized_symbols = self._normalize_string_filter(symbols, uppercase=True)
        if normalized_symbols:
            clauses.append("ticker IN :symbols")
            params["symbols"] = normalized_symbols
            expanding.append("symbols")
        if start_year is not None:
            clauses.append("`year` >= :start_year")
            params["start_year"] = int(start_year)
        if end_year is not None:
            clauses.append("`year` <= :end_year")
            params["end_year"] = int(end_year)

        return " AND ".join(clauses), params, expanding

    def _normalize_string_filter(
        self,
        value: str | Iterable[str] | None,
        uppercase: bool = False,
    ) -> list[str]:
        if value is None:
            return []
        values = [value] if isinstance(value, str) else list(value)
        normalized = []
        for item in values:
            if item is None:
                continue
            text_value = str(item).strip()
            if not text_value:
                continue
            normalized.append(text_value.upper() if uppercase else text_value)
        return normalized

    def _read_sql(
        self,
        query: str,
        params: dict[str, Any],
        expanding: list[str],
    ) -> pd.DataFrame:
        statement = text(query)
        for key in expanding:
            statement = statement.bindparams(bindparam(key, expanding=True))
        return pd.read_sql(statement, con=self.engine, params=params)

    def _plot_total_compensation(self, df: pd.DataFrame, detail: bool) -> None:
        if df.empty:
            return
        working = df.copy()
        value_column = "total" if detail else "total_compensation"
        if detail:
            working = (
                working.assign(total=pd.to_numeric(working["total"], errors="coerce").fillna(0))
                .groupby(["symbol", "year"], dropna=False, as_index=False)
                .agg(total=(value_column, "sum"))
            )
            value_column = "total"
        plot_df = working.pivot_table(
            index="year",
            columns="symbol",
            values=value_column,
            aggfunc="sum",
        )
        plot_df.plot(title="Total Executive Compensation")


Form4 = Form4Importer
Form13F = Form13FImporter
ExecutiveCompensation = ExecutiveCompensationImporter


class DilutionSecApiClient:
    """Small sequential sec-api.io client for on-demand dilution workflows."""

    def __init__(
        self,
        api_key: str = sec_api_key,
        timeout: int = 120,
        min_request_interval: float = 1.0,
        max_retries: int = 6,
        retry_backoff: float = 5.0,
        max_retry_sleep: float = 120.0,
    ) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_sleep = max_retry_sleep
        self._last_request_at = 0.0
        self._last_page_requests = 0
        self.session = requests.Session()

    def get_float(self, ticker: str | None = None, cik: str | None = None) -> dict[str, Any]:
        params = {}
        if ticker:
            params["ticker"] = ticker.upper()
        if cik:
            params["cik"] = str(cik).lstrip("0")
        return self._get_json(SEC_API_FLOAT_ENDPOINT, params=params)

    def xbrl_to_json(
        self,
        accession_no: str | None = None,
        htm_url: str | None = None,
        xbrl_url: str | None = None,
    ) -> dict[str, Any]:
        params = {}
        if accession_no:
            params["accession-no"] = accession_no
        if htm_url:
            params["htm-url"] = htm_url
        if xbrl_url:
            params["xbrl-url"] = xbrl_url
        return self._get_json(SEC_API_XBRL_ENDPOINT, params=params)

    def extract_section(self, filing_url: str, section: str, return_type: str = "text") -> str:
        params = {"url": filing_url, "item": section, "type": return_type}
        with self._request("GET", SEC_API_EXTRACTOR_ENDPOINT, params=params) as response:
            return response.text or ""

    def query_filings(
        self,
        query: str,
        sort: list[dict[str, Any]] | None = None,
        page_size: int = SEC_API_DILUTION_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset = 0
        self._last_page_requests = 0
        sort = sort or [{"filedAt": {"order": "desc"}}]

        while True:
            payload = {
                "query": query,
                "from": str(offset),
                "size": str(page_size),
                "sort": sort,
            }
            response = self._post_json(SEC_API_BASE_URL, payload)
            self._last_page_requests += 1
            data = response.get("filings", response.get("data", []))
            total, relation = self._total_and_relation(response, len(data))
            if total > SEC_API_DILUTION_FROM_LIMIT or (
                total >= SEC_API_DILUTION_FROM_LIMIT and relation != "eq"
            ):
                raise SearchResultLimitError(query, total)
            if not data:
                break

            records.extend(data)
            offset += page_size
            if len(data) < page_size or offset >= total:
                break

        return records

    def full_text_search(
        self,
        query: str,
        form_types: Iterable[str],
        start_date: dt.date,
        end_date: dt.date,
        cik: str | None = None,
        max_pages: int = 3,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page = 1
        while page <= max_pages:
            payload: dict[str, Any] = {
                "query": query,
                "formTypes": list(form_types),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "page": str(page),
            }
            if cik:
                payload["ciks"] = [str(cik)]
            response = self._post_json(SEC_API_FULL_TEXT_SEARCH_ENDPOINT, payload)
            filings = response.get("filings", [])
            if not filings:
                break
            records.extend(filings)
            total, relation = self._total_and_relation(response, len(filings))
            if relation == "gte":
                total = min(total, SEC_API_DILUTION_FROM_LIMIT)
            if page * SEC_API_FULL_TEXT_PAGE_SIZE >= total:
                break
            page += 1
        return records

    def form_d_offerings(
        self,
        query: str,
        page_size: int = SEC_API_DILUTION_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset = 0
        self._last_page_requests = 0
        while True:
            payload = {
                "query": query,
                "from": str(offset),
                "size": str(page_size),
                "sort": [{"filedAt": {"order": "desc"}}],
            }
            response = self._post_json(SEC_API_FORM_D_ENDPOINT, payload)
            self._last_page_requests += 1
            data = response.get("offerings", response.get("data", []))
            total, relation = self._total_and_relation(response, len(data))
            if total > SEC_API_DILUTION_FROM_LIMIT or (
                total >= SEC_API_DILUTION_FROM_LIMIT and relation != "eq"
            ):
                raise SearchResultLimitError(query, total)
            if not data:
                break
            records.extend(data)
            offset += page_size
            if len(data) < page_size or offset >= total:
                break
        return records

    def _get_json(self, endpoint: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        with self._request("GET", endpoint, params=params) as response:
            return response.json()

    def _post_json(self, endpoint: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._request("POST", endpoint, json_payload=payload) as response:
            return response.json()

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        json_payload: Mapping[str, Any] | None = None,
    ) -> requests.Response:
        retry_statuses = {429, 500, 502, 503, 504}
        last_response: requests.Response | None = None

        for attempt in range(self.max_retries + 1):
            self._wait_for_request_slot()
            response = self.session.request(
                method,
                endpoint,
                headers=self._headers(),
                params=dict(params or {}),
                json=dict(json_payload or {}) if json_payload is not None else None,
                timeout=self.timeout,
            )
            self._last_request_at = time.monotonic()
            last_response = response

            if response.status_code not in retry_statuses:
                response.raise_for_status()
                return response

            if attempt >= self.max_retries:
                response.raise_for_status()

            sleep_seconds = self._retry_sleep_seconds(response, attempt)
            response.close()
            time.sleep(sleep_seconds)

        if last_response is not None:
            last_response.raise_for_status()
        raise RuntimeError(f"Unable to retrieve {endpoint}")

    def _wait_for_request_slot(self) -> None:
        if self.min_request_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.min_request_interval - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _retry_sleep_seconds(self, response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), self.max_retry_sleep)
            except ValueError:
                pass
        return min(self.retry_backoff * (2 ** attempt), self.max_retry_sleep)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self.api_key}

    def _total_and_relation(self, response: Mapping[str, Any], default_total: int) -> tuple[int, str]:
        total_value = response.get("total")
        if isinstance(total_value, Mapping):
            return int(total_value.get("value") or default_total), str(total_value.get("relation") or "eq")
        return int(total_value or default_total), "eq"


class ShareCountHistory:
    """Normalize the sec-api.io Outstanding Shares API into a historical DataFrame."""

    def __init__(self, client: DilutionSecApiClient | None = None) -> None:
        self.client = client or DilutionSecApiClient()

    def get(self, symbol: str) -> pd.DataFrame:
        normalized_symbol = self._normalize_symbol(symbol)
        payload = self.client.get_float(ticker=normalized_symbol)
        rows: list[dict[str, Any]] = []
        for record in payload.get("data", []):
            float_payload = first_mapping(record.get("float"))
            for observation in float_payload.get("outstandingShares", []) or []:
                rows.append(
                    {
                        "symbol": normalized_symbol,
                        "cik": record.get("cik"),
                        "period": parse_date(observation.get("period")),
                        "share_class": observation.get("shareClass"),
                        "shares_outstanding": parse_decimal(observation.get("value")),
                        "period_of_report": parse_date(record.get("periodOfReport")),
                        "reported_at": parse_datetime(record.get("reportedAt")),
                        "source_accession_no": record.get("sourceFilingAccessionNo"),
                        "actual_dilution_pct": None,
                        "raw_json": json_dumps(observation),
                    }
                )

        df = pd.DataFrame(rows, columns=DILUTION_SHARE_COUNT_COLUMNS)
        if df.empty:
            return df
        df = df.sort_values(["share_class", "period", "reported_at"], na_position="last").reset_index(drop=True)
        df["shares_outstanding"] = pd.to_numeric(df["shares_outstanding"], errors="coerce")
        df["actual_dilution_pct"] = df.groupby("share_class", dropna=False)["shares_outstanding"].pct_change()
        return df

    def _normalize_symbol(self, symbol: str) -> str:
        value = str(symbol or "").strip().upper()
        if not value:
            raise ValueError("symbol is required")
        return value


class DilutionTextParser:
    """Deterministic dilution-oriented text parser for common share and financing phrases."""

    KEYWORDS = {
        "warrant": ("warrant", "exercise price"),
        "convertible": ("convertible", "conversion price", "conversion rate", "convert"),
        "registered": ("register", "registration statement", "prospectus"),
        "reserved": ("reserved", "equity incentive", "stock plan", "employee stock purchase"),
        "authorized": ("authorized shares", "increase authorized", "certificate of amendment"),
        "selling_stockholder": ("selling stockholder", "selling shareholder", "resale"),
        "repurchased": ("repurchase", "repurchased", "buyback", "treasury stock"),
        "issued": ("issued", "issuance", "sold", "sale of"),
        "atm": ("at-the-market", "atm", "sales agreement", "equity distribution"),
    }
    SHARE_PATTERN = re.compile(
        r"(?P<number>\$?\d[\d,]*(?:\.\d+)?)\s*"
        r"(?P<scale>thousand|million|billion)?\s*"
        r"(?P<unit>shares?|common shares?|ordinary shares?|units?|dollars?|principal amount)",
        re.IGNORECASE,
    )

    def parse(
        self,
        text_value: str,
        metadata: Mapping[str, Any],
        source_endpoint: str,
        source_section: str | None = None,
        source_url: str | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not text_value:
            return rows
        text_value = self._clean_text(text_value)
        for sentence in self._sentences(text_value):
            for match in self.SHARE_PATTERN.finditer(sentence):
                raw_match = " ".join(match.group(0).split())
                context = self._clean_text(sentence)
                category = self._classify(context)
                if category is None:
                    continue
                quantity, amount = self._parse_quantity_or_amount(match)
                if quantity is None and amount is None:
                    continue
                security_type = self._security_type(context)
                confidence = self._confidence(context, category, quantity, amount)
                rows.append(
                    {
                        "symbol": metadata.get("symbol"),
                        "cik": metadata.get("cik"),
                        "accession_no": metadata.get("accession_no"),
                        "form_type": metadata.get("form_type"),
                        "filed_at": metadata.get("filed_at"),
                        "period_of_report": metadata.get("period_of_report"),
                        "company_name": metadata.get("company_name"),
                        "source_endpoint": source_endpoint,
                        "source_url": source_url or metadata.get("source_url"),
                        "source_section": source_section,
                        "category": category,
                        "quantity": quantity,
                        "amount": amount,
                        "security_type": security_type,
                        "confidence": confidence,
                        "snippet": self._snippet(context),
                        "raw_match": raw_match,
                    }
                )
        return rows

    def candidate_row(
        self,
        text_value: str,
        metadata: Mapping[str, Any],
        source_endpoint: str,
        source_section: str | None = None,
        source_url: str | None = None,
        parse_status: str = "not_parsed",
    ) -> dict[str, Any]:
        cleaned = self._clean_text(text_value)
        return {
            "symbol": metadata.get("symbol"),
            "cik": metadata.get("cik"),
            "accession_no": metadata.get("accession_no"),
            "form_type": metadata.get("form_type"),
            "filed_at": metadata.get("filed_at"),
            "period_of_report": metadata.get("period_of_report"),
            "company_name": metadata.get("company_name"),
            "source_endpoint": source_endpoint,
            "source_url": source_url or metadata.get("source_url"),
            "source_section": source_section,
            "matched_keywords": ", ".join(self.matched_keywords(cleaned)),
            "snippet": self._snippet(cleaned),
            "parse_status": parse_status,
        }

    def matched_keywords(self, text_value: str) -> list[str]:
        lowered = text_value.lower()
        matches = []
        for label, keywords in self.KEYWORDS.items():
            if any(keyword in lowered for keyword in keywords):
                matches.append(label)
        return matches

    def _classify(self, context: str) -> str | None:
        lowered = context.lower()
        if any(word in lowered for word in ("repurchase", "repurchased", "buyback", "treasury stock")):
            return "repurchased"
        if any(word in lowered for word in ("cancelled", "canceled", "cancel ", "retired")):
            return "cancelled"
        if "selling stockholder" in lowered or "selling shareholder" in lowered or "resale" in lowered:
            if "warrant" in lowered:
                return "underlying_warrants"
            if "convertible" in lowered or "conversion" in lowered:
                return "underlying_convertibles"
            return "selling_stockholder"
        if "warrant" in lowered:
            return "underlying_warrants"
        if "convertible" in lowered or "conversion" in lowered:
            return "underlying_convertibles"
        if "authorized" in lowered:
            return "authorized"
        if "reserved" in lowered or "equity incentive" in lowered or "employee stock purchase" in lowered:
            return "reserved"
        if "register" in lowered or "prospectus" in lowered:
            return "registered"
        if "issuable" in lowered or "underlying" in lowered or "exercise" in lowered:
            return "issuable"
        if "outstanding" in lowered:
            return "outstanding"
        if "issued" in lowered or "issuance" in lowered:
            return "newly_issued"
        if "sold" in lowered or "sale of" in lowered:
            return "sold"
        if any(keyword in lowered for keyword in ("at-the-market", "atm", "equity line", "pipe")):
            return "registered"
        return None

    def _parse_quantity_or_amount(self, match: re.Match[str]) -> tuple[Decimal | None, Decimal | None]:
        number = parse_decimal(match.group("number").replace("$", ""))
        if number is None:
            return None, None
        scale = (match.group("scale") or "").lower()
        multiplier = Decimal("1")
        if scale == "thousand":
            multiplier = Decimal("1000")
        elif scale == "million":
            multiplier = Decimal("1000000")
        elif scale == "billion":
            multiplier = Decimal("1000000000")
        value = number * multiplier
        unit = (match.group("unit") or "").lower()
        if "$" in match.group("number") or "dollar" in unit or "principal amount" in unit:
            return None, value
        return value, None

    def _security_type(self, context: str) -> str:
        lowered = context.lower()
        if "warrant" in lowered:
            return "warrant"
        if "convertible note" in lowered or "convertible debt" in lowered:
            return "convertible_debt"
        if "preferred" in lowered:
            return "preferred_stock"
        if "option" in lowered:
            return "option"
        if "rsu" in lowered or "restricted stock unit" in lowered:
            return "rsu"
        if "unit" in lowered:
            return "unit"
        if "common" in lowered or "ordinary" in lowered:
            return "common_stock"
        return "unknown"

    def _confidence(
        self,
        context: str,
        category: str,
        quantity: Decimal | None,
        amount: Decimal | None,
    ) -> str:
        if quantity is not None and category in DILUTION_QUANTITY_CATEGORIES:
            if any(marker in context.lower() for marker in ("approximately", "up to", "maximum", "may issue")):
                return "medium"
            return "high"
        if amount is not None:
            return "medium"
        return "low"

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def _sentences(self, value: str) -> list[str]:
        return [sentence.strip() for sentence in re.split(r"(?<=[.;])\s+", value) if sentence.strip()]

    def _snippet(self, value: str, length: int = 500) -> str:
        cleaned = self._clean_text(value)
        if len(cleaned) <= length:
            return cleaned
        return cleaned[:length].rsplit(" ", 1)[0] + "..."


class DilutionFilingSource:
    """Base object for Query API filing discovery and text parsing."""

    source_name = "query"
    forms: tuple[str, ...] = ()

    def __init__(
        self,
        client: DilutionSecApiClient | None = None,
        parser: DilutionTextParser | None = None,
    ) -> None:
        self.client = client or DilutionSecApiClient()
        self.parser = parser or DilutionTextParser()

    def collect(
        self,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        cik: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        filings = self._query_filings(symbol, start_date, end_date)
        candidates: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        for filing in filings:
            metadata = self._metadata(symbol, filing, cik)
            text_items = self._text_items(filing)
            if not text_items:
                candidates.append(
                    self.parser.candidate_row(
                        self._filing_text_seed(filing),
                        metadata,
                        source_endpoint=self.source_name,
                        parse_status="metadata_only",
                    )
                )
                continue
            parsed_any = False
            for section, text_value, source_url in text_items:
                parsed = self.parser.parse(
                    text_value,
                    metadata,
                    source_endpoint=self.source_name,
                    source_section=section,
                    source_url=source_url,
                )
                parsed_any = parsed_any or bool(parsed)
                events.extend(parsed)
                candidates.append(
                    self.parser.candidate_row(
                        text_value,
                        metadata,
                        source_endpoint=self.source_name,
                        source_section=section,
                        source_url=source_url,
                        parse_status="parsed" if parsed else "no_quantity",
                    )
                )
            if not parsed_any and not text_items:
                candidates.append(
                    self.parser.candidate_row(
                        self._filing_text_seed(filing),
                        metadata,
                        source_endpoint=self.source_name,
                        parse_status="no_quantity",
                    )
                )
        return events, candidates

    def _query_filings(self, symbol: str, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
        clauses = [
            f"ticker:{self._lucene_value(symbol.upper())}",
            self._form_clause(self.forms),
            f"filedAt:[{start_date.isoformat()} TO {end_date.isoformat()}]",
        ]
        query = " AND ".join(clause for clause in clauses if clause)
        return self.client.query_filings(query=query)

    def _text_items(self, filing: Mapping[str, Any]) -> list[tuple[str | None, str, str | None]]:
        return [(None, self._filing_text_seed(filing), filing.get("linkToFilingDetails") or filing.get("linkToHtml"))]

    def _metadata(self, symbol: str, filing: Mapping[str, Any], cik: str | None = None) -> dict[str, Any]:
        accession_no = first_present(filing, "accessionNo", "accession_no")
        form_type = first_present(filing, "formType", "form_type")
        filed_at = first_present(filing, "filedAt", "filed_at")
        period_of_report = first_present(filing, "periodOfReport", "period_of_report")
        company_name = first_present(filing, "companyName", "company_name", "companyNameLong")
        source_url = first_present(
            filing,
            "linkToFilingDetails",
            "linkToHtml",
            "linkToTxt",
            "filingUrl",
            "fileUrl",
            "url",
        )
        return {
            "symbol": symbol.upper(),
            "cik": cik or filing.get("cik"),
            "accession_no": accession_no,
            "form_type": form_type,
            "filed_at": parse_datetime(filed_at),
            "period_of_report": parse_date(period_of_report),
            "company_name": company_name,
            "source_url": source_url,
        }

    def _filing_text_seed(self, filing: Mapping[str, Any]) -> str:
        parts = [
            filing.get("description"),
            filing.get("formType"),
            filing.get("companyName"),
            " ".join(str(item) for item in filing.get("items", []) or []),
        ]
        for document in filing.get("documentFormatFiles", []) or []:
            parts.extend([document.get("type"), document.get("description")])
        return " ".join(str(part) for part in parts if part)

    def _form_clause(self, forms: Iterable[str]) -> str:
        values = [f"formType:{self._lucene_value(value)}" for value in forms]
        return "(" + " OR ".join(values) + ")" if values else ""

    def _lucene_value(self, value: str) -> str:
        if value.replace(".", "").replace("-", "").replace("_", "").isalnum():
            return value
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'


class PeriodicDilutionFiling(DilutionFilingSource):
    source_name = "periodic"
    forms = PERIODIC_DILUTION_FORMS

    def _text_items(self, filing: Mapping[str, Any]) -> list[tuple[str | None, str, str | None]]:
        form_type = str(filing.get("formType") or "")
        filing_url = filing.get("linkToFilingDetails") or filing.get("linkToHtml")
        items: list[tuple[str | None, str, str | None]] = []
        if filing_url:
            for section in PERIODIC_DILUTION_EXTRACTOR_ITEMS.get(form_type, ()):
                try:
                    text_value = self.client.extract_section(filing_url, section, return_type="text")
                except requests.HTTPError:
                    continue
                if text_value:
                    items.append((section, text_value, filing_url))
        return items or super()._text_items(filing)

    def collect(
        self,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        cik: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        events, candidates = super().collect(symbol, start_date, end_date, cik=cik)
        for filing in self._query_filings(symbol, start_date, end_date):
            metadata = self._metadata(symbol, filing, cik)
            try:
                xbrl_payload = self.client.xbrl_to_json(accession_no=metadata.get("accession_no"))
            except requests.HTTPError:
                continue
            events.extend(self._xbrl_share_events(xbrl_payload, metadata))
        return events, candidates

    def _xbrl_share_events(
        self,
        xbrl_payload: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for statement_name in ("CoverPage", "StatementsOfIncome", "StatementsOfShareholdersEquity"):
            statement = first_mapping(xbrl_payload.get(statement_name))
            for tag, facts in statement.items():
                if not isinstance(facts, list) or not self._is_share_tag(tag):
                    continue
                for fact in facts:
                    value = parse_decimal(first_present(first_mapping(fact), "value"))
                    if value is None:
                        continue
                    period = first_mapping(first_mapping(fact).get("period"))
                    rows.append(
                        {
                            "symbol": metadata.get("symbol"),
                            "cik": metadata.get("cik"),
                            "accession_no": metadata.get("accession_no"),
                            "form_type": metadata.get("form_type"),
                            "filed_at": metadata.get("filed_at"),
                            "period_of_report": parse_date(period.get("endDate") or period.get("instant"))
                            or metadata.get("period_of_report"),
                            "company_name": metadata.get("company_name"),
                            "source_endpoint": "xbrl",
                            "source_url": metadata.get("source_url"),
                            "source_section": f"{statement_name}.{tag}",
                            "category": self._xbrl_category(tag),
                            "quantity": value,
                            "amount": None,
                            "security_type": "common_stock",
                            "confidence": "high",
                            "snippet": tag,
                            "raw_match": json_dumps(fact),
                        }
                    )
        return rows

    def _is_share_tag(self, tag: str) -> bool:
        lowered = tag.lower()
        return "share" in lowered and any(
            marker in lowered
            for marker in ("outstanding", "issued", "repurchased", "reserved", "diluted", "warrant", "convertible")
        )

    def _xbrl_category(self, tag: str) -> str:
        lowered = tag.lower()
        if "repurchase" in lowered or "treasury" in lowered:
            return "repurchased"
        if "reserved" in lowered:
            return "reserved"
        if "warrant" in lowered:
            return "underlying_warrants"
        if "convertible" in lowered:
            return "underlying_convertibles"
        if "issued" in lowered:
            return "newly_issued"
        return "outstanding"


class CurrentDilutionEvent(DilutionFilingSource):
    source_name = "current_event"
    forms = CURRENT_DILUTION_EVENT_FORMS

    def _query_filings(self, symbol: str, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
        item_clause = "(" + " OR ".join(f"items:{self._lucene_value(item)}" for item in DILUTION_8K_ITEM_CODES) + ")"
        query = " AND ".join(
            [
                f"ticker:{self._lucene_value(symbol.upper())}",
                self._form_clause(self.forms),
                item_clause,
                f"filedAt:[{start_date.isoformat()} TO {end_date.isoformat()}]",
            ]
        )
        return self.client.query_filings(query=query)

    def _text_items(self, filing: Mapping[str, Any]) -> list[tuple[str | None, str, str | None]]:
        filing_url = filing.get("linkToFilingDetails") or filing.get("linkToHtml")
        items: list[tuple[str | None, str, str | None]] = []
        if filing_url:
            for section in DILUTION_8K_EXTRACTOR_ITEMS:
                try:
                    text_value = self.client.extract_section(filing_url, section, return_type="text")
                except requests.HTTPError:
                    continue
                if text_value:
                    items.append((section, text_value, filing_url))
        return items or super()._text_items(filing)


class FullTextDilutionFilingSource(DilutionFilingSource):
    source_name = "full_text"

    def collect(
        self,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        cik: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        query_candidates = self._query_filings(symbol, start_date, end_date)
        text_candidates = self.client.full_text_search(
            DILUTION_KEYWORD_QUERY,
            form_types=self.forms,
            start_date=start_date,
            end_date=end_date,
            cik=cik,
        )
        merged = self._merge_records(query_candidates, text_candidates)
        events: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for filing in merged:
            metadata = self._metadata(symbol, filing, cik)
            text_value = self._filing_text_seed(filing)
            for key in ("snippet", "text", "description"):
                if filing.get(key):
                    text_value = f"{text_value} {filing.get(key)}"
            parsed = self.parser.parse(
                text_value,
                metadata,
                source_endpoint=self.source_name,
                source_url=metadata.get("source_url"),
            )
            events.extend(parsed)
            candidates.append(
                self.parser.candidate_row(
                    text_value,
                    metadata,
                    source_endpoint=self.source_name,
                    source_url=metadata.get("source_url"),
                    parse_status="parsed" if parsed else "candidate",
                )
            )
        return events, candidates

    def _merge_records(
        self,
        first: Iterable[Mapping[str, Any]],
        second: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for record in list(first) + list(second):
            accession_no = record.get("accessionNo") or record.get("accessionNo".lower())
            key = str(accession_no or record.get("id") or len(merged))
            current = merged.setdefault(key, {})
            current.update(dict(record))
        return list(merged.values())


class RegisteredOffering(FullTextDilutionFilingSource):
    source_name = "registered_offering"
    forms = OFFERING_DILUTION_FORMS


class EquityPlan(FullTextDilutionFilingSource):
    source_name = "equity_plan"
    forms = COMPENSATION_DILUTION_FORMS


class MergerShareIssuance(FullTextDilutionFilingSource):
    source_name = "merger_share_issuance"
    forms = MERGER_DILUTION_FORMS


class PrivateOffering(DilutionFilingSource):
    source_name = "private_offering"
    forms = PRIVATE_OFFERING_DILUTION_FORMS

    def collect(
        self,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        cik: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        issuer_clause = f'primaryIssuer.cik:"{str(cik).zfill(10)}"' if cik else f'primaryIssuer.entityName:"{symbol.upper()}"'
        query = (
            f"{issuer_clause} AND filedAt:[{start_date.isoformat()} TO {end_date.isoformat()}] "
            "AND (offeringData.typesOfSecuritiesOffered.isEquityType:true "
            "OR offeringData.typesOfSecuritiesOffered.isDebtType:true)"
        )
        offerings = self.client.form_d_offerings(query)
        events: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for offering in offerings:
            metadata = self._form_d_metadata(symbol, offering, cik)
            text_value = self._form_d_text_seed(offering)
            parsed = self.parser.parse(text_value, metadata, self.source_name, source_url=metadata.get("source_url"))
            if not parsed:
                parsed = [self._form_d_event(metadata, offering)]
            events.extend(parsed)
            candidates.append(
                self.parser.candidate_row(
                    text_value,
                    metadata,
                    self.source_name,
                    source_url=metadata.get("source_url"),
                    parse_status="parsed" if parsed else "candidate",
                )
            )
        return events, candidates

    def _form_d_metadata(self, symbol: str, offering: Mapping[str, Any], cik: str | None = None) -> dict[str, Any]:
        issuer = first_mapping(offering.get("primaryIssuer"))
        return {
            "symbol": symbol.upper(),
            "cik": cik or issuer.get("cik"),
            "accession_no": offering.get("accessionNo"),
            "form_type": offering.get("submissionType") or "D",
            "filed_at": parse_datetime(offering.get("filedAt")),
            "period_of_report": parse_date(first_mapping(offering.get("offeringData")).get("dateOfFirstSale")),
            "company_name": issuer.get("entityName"),
            "source_url": offering.get("linkToFilingDetails") or offering.get("linkToTxt"),
        }

    def _form_d_text_seed(self, offering: Mapping[str, Any]) -> str:
        offering_data = first_mapping(offering.get("offeringData"))
        security_types = first_mapping(offering_data.get("typesOfSecuritiesOffered"))
        sales_amounts = first_mapping(offering_data.get("offeringSalesAmounts"))
        return " ".join(
            str(value)
            for value in (
                "Form D private offering",
                security_types,
                sales_amounts,
                offering_data.get("businessCombinationTransaction"),
                offering_data.get("minimumInvestmentAccepted"),
            )
            if value
        )

    def _form_d_event(self, metadata: Mapping[str, Any], offering: Mapping[str, Any]) -> dict[str, Any]:
        offering_data = first_mapping(offering.get("offeringData"))
        sales_amounts = first_mapping(offering_data.get("offeringSalesAmounts"))
        amount = parse_decimal(
            first_present(
                sales_amounts,
                "totalAmountSold",
                "totalOfferingAmount",
                "totalRemaining",
            )
        )
        return {
            "symbol": metadata.get("symbol"),
            "cik": metadata.get("cik"),
            "accession_no": metadata.get("accession_no"),
            "form_type": metadata.get("form_type"),
            "filed_at": metadata.get("filed_at"),
            "period_of_report": metadata.get("period_of_report"),
            "company_name": metadata.get("company_name"),
            "source_endpoint": self.source_name,
            "source_url": metadata.get("source_url"),
            "source_section": "offeringData.offeringSalesAmounts",
            "category": "issuable",
            "quantity": None,
            "amount": amount,
            "security_type": "private_offering",
            "confidence": "medium",
            "snippet": self.parser._snippet(self._form_d_text_seed(offering)),
            "raw_match": json_dumps(sales_amounts),
        }


class DilutionTracker:
    """Facade for on-demand historical and potential dilution DataFrames."""

    def __init__(
        self,
        api_key: str = sec_api_key,
        client: DilutionSecApiClient | None = None,
        parser: DilutionTextParser | None = None,
        default_lookback_days: int = DEFAULT_DILUTION_LOOKBACK_DAYS,
    ) -> None:
        self.client = client or DilutionSecApiClient(api_key=api_key)
        self.parser = parser or DilutionTextParser()
        self.default_lookback_days = default_lookback_days
        self.share_count_history = ShareCountHistory(self.client)
        self.sources = [
            PeriodicDilutionFiling(self.client, self.parser),
            CurrentDilutionEvent(self.client, self.parser),
            RegisteredOffering(self.client, self.parser),
            PrivateOffering(self.client, self.parser),
            EquityPlan(self.client, self.parser),
            MergerShareIssuance(self.client, self.parser),
        ]

    def get_share_count_history(self, symbol: str) -> pd.DataFrame:
        return self.share_count_history.get(symbol)

    def get_potential_dilution_events(
        self,
        symbol: str,
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
    ) -> pd.DataFrame:
        events, _ = self._collect(symbol, start_date, end_date)
        return self._event_df(events)

    def get_candidate_filings(
        self,
        symbol: str,
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
    ) -> pd.DataFrame:
        _, candidates = self._collect(symbol, start_date, end_date)
        return self._candidate_df(candidates)

    def get_dilution_summary(
        self,
        symbol: str,
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        share_history = self.get_share_count_history(symbol)
        events, candidates = self._collect(symbol, start_date, end_date, share_history=share_history)
        event_df = self._event_df(events)
        candidate_df = self._candidate_df(candidates)
        return {
            "share_count_history": share_history,
            "historical_dilution": self._historical_dilution(share_history),
            "potential_dilution_events": event_df,
            "potential_shares_by_category": self._potential_shares_by_category(event_df),
            "registered_reserved_capacity": self._registered_reserved_capacity(event_df),
            "low_confidence_review": self._low_confidence_review(event_df, candidate_df),
            "candidate_filings": candidate_df,
            "latest_shares_outstanding": self._latest_shares_outstanding(share_history),
        }

    def _collect(
        self,
        symbol: str,
        start_date: str | dt.date | dt.datetime | None = None,
        end_date: str | dt.date | dt.datetime | None = None,
        share_history: pd.DataFrame | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        normalized_symbol = self._normalize_symbol(symbol)
        start, end = self._resolve_date_range(start_date, end_date)
        cik = self._resolve_cik(normalized_symbol, share_history)
        events: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for source in self.sources:
            try:
                source_events, source_candidates = source.collect(normalized_symbol, start, end, cik=cik)
            except SearchResultLimitError:
                source_events, source_candidates = self._collect_split(source, normalized_symbol, start, end, cik)
            events.extend(source_events)
            candidates.extend(source_candidates)
        return events, candidates

    def _collect_split(
        self,
        source: DilutionFilingSource,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        cik: str | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        events: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for window_start, window_end in self._month_ranges(start_date, end_date):
            source_events, source_candidates = source.collect(symbol, window_start, window_end, cik=cik)
            events.extend(source_events)
            candidates.extend(source_candidates)
        return events, candidates

    def _event_df(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=DILUTION_EVENT_COLUMNS)
        if df.empty:
            return df
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        return df.drop_duplicates().sort_values(["filed_at", "accession_no"], na_position="last").reset_index(drop=True)

    def _candidate_df(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=DILUTION_CANDIDATE_COLUMNS)
        if df.empty:
            return df
        return df.drop_duplicates().sort_values(["filed_at", "accession_no"], na_position="last").reset_index(drop=True)

    def _historical_dilution(self, share_history: pd.DataFrame) -> pd.DataFrame:
        if share_history.empty:
            return pd.DataFrame(columns=DILUTION_SHARE_COUNT_COLUMNS)
        return share_history[share_history["actual_dilution_pct"].notna()].copy()

    def _potential_shares_by_category(self, event_df: pd.DataFrame) -> pd.DataFrame:
        if event_df.empty:
            return pd.DataFrame(columns=["category", "quantity", "amount", "events"])
        return (
            event_df.groupby("category", dropna=False, as_index=False)
            .agg(quantity=("quantity", "sum"), amount=("amount", "sum"), events=("accession_no", "count"))
            .sort_values("quantity", ascending=False, na_position="last")
        )

    def _registered_reserved_capacity(self, event_df: pd.DataFrame) -> pd.DataFrame:
        if event_df.empty:
            return pd.DataFrame(columns=DILUTION_EVENT_COLUMNS)
        return event_df[event_df["category"].isin(["registered", "reserved", "authorized"])].copy()

    def _low_confidence_review(self, event_df: pd.DataFrame, candidate_df: pd.DataFrame) -> pd.DataFrame:
        low_events = event_df[event_df["confidence"].isin(["low", "medium"])].copy() if not event_df.empty else pd.DataFrame()
        no_quantity = (
            candidate_df[candidate_df["parse_status"].isin(["candidate", "no_quantity", "metadata_only"])].copy()
            if not candidate_df.empty
            else pd.DataFrame()
        )
        if low_events.empty and no_quantity.empty:
            return pd.DataFrame()
        low_events = low_events.rename(columns={"raw_match": "parse_status"})
        return pd.concat([low_events, no_quantity], ignore_index=True, sort=False)

    def _latest_shares_outstanding(self, share_history: pd.DataFrame) -> pd.DataFrame:
        if share_history.empty:
            return pd.DataFrame(columns=DILUTION_SHARE_COUNT_COLUMNS)
        idx = share_history.sort_values(["period", "reported_at"], na_position="last").groupby(
            "share_class", dropna=False
        ).tail(1).index
        return share_history.loc[idx].reset_index(drop=True)

    def _resolve_cik(self, symbol: str, share_history: pd.DataFrame | None = None) -> str | None:
        if share_history is None:
            try:
                share_history = self.get_share_count_history(symbol)
            except Exception:
                share_history = pd.DataFrame()
        if not share_history.empty and "cik" in share_history:
            values = share_history["cik"].dropna().astype(str)
            if not values.empty:
                return values.iloc[-1].lstrip("0")
        return None

    def _resolve_date_range(
        self,
        start_date: str | dt.date | dt.datetime | None,
        end_date: str | dt.date | dt.datetime | None,
    ) -> tuple[dt.date, dt.date]:
        end = parse_date(end_date) if end_date is not None else dt.date.today()
        start = parse_date(start_date) if start_date is not None else end - dt.timedelta(days=self.default_lookback_days)
        if end < start:
            raise ValueError("end_date must be on or after start_date")
        return start, end

    def _month_ranges(self, start: dt.date, end: dt.date) -> Iterator[tuple[dt.date, dt.date]]:
        current = start.replace(day=1)
        while current <= end:
            if current.month == 12:
                next_month = dt.date(current.year + 1, 1, 1)
            else:
                next_month = dt.date(current.year, current.month + 1, 1)
            month_end = min(next_month - dt.timedelta(days=1), end)
            yield max(current, start), month_end
            current = next_month

    def _normalize_symbol(self, symbol: str) -> str:
        value = str(symbol or "").strip().upper()
        if not value:
            raise ValueError("symbol is required")
        return value


Dilution = DilutionTracker


def validate_dilution_text_parser_sample() -> dict[str, Any]:
    """Exercise DilutionTextParser with embedded dilution disclosure snippets."""
    parser = DilutionTextParser()
    metadata = {
        "symbol": "SAMP",
        "cik": "1234567",
        "accession_no": "0000000000-26-000010",
        "form_type": "8-K",
        "filed_at": dt.datetime(2026, 6, 1, 12, 0),
        "period_of_report": dt.date(2026, 5, 31),
        "company_name": "Sample Corp",
        "source_url": "https://www.sec.gov/sample.htm",
    }
    text_value = """
    We registered up to 10,000,000 shares of common stock under the shelf registration statement.
    The company issued warrants to purchase 5,000,000 shares of common stock.
    The company sold $25 million principal amount of convertible notes.
    The plan reserves 2,000,000 shares under the 2026 equity incentive plan.
    During the period, the company repurchased 1,000,000 shares of common stock.
    Selling stockholders may offer 3,000,000 shares of common stock for resale.
    """
    rows = parser.parse(text_value, metadata, "sample", "item")
    categories = {row["category"] for row in rows}

    assert "registered" in categories
    assert "underlying_warrants" in categories
    assert "underlying_convertibles" in categories
    assert "reserved" in categories
    assert "repurchased" in categories
    assert "selling_stockholder" in categories
    assert any(row["quantity"] == Decimal("10000000") for row in rows)
    assert any(row["amount"] == Decimal("25000000") for row in rows)

    return {"rows": rows, "categories": sorted(categories)}


def validate_dilution_tracker_sample() -> dict[str, Any]:
    """Exercise DilutionTracker DataFrame schemas without making API requests."""

    class FakeDilutionSecApiClient(DilutionSecApiClient):
        def __init__(self) -> None:
            pass

        def get_float(self, ticker: str | None = None, cik: str | None = None) -> dict[str, Any]:
            return {
                "data": [
                    {
                        "tickers": [ticker or "SAMP"],
                        "cik": "1234567",
                        "reportedAt": "2026-06-01T16:00:00-04:00",
                        "periodOfReport": "2026-03-31",
                        "sourceFilingAccessionNo": "0000000000-26-000001",
                        "float": {
                            "outstandingShares": [
                                {"period": "2025-03-31", "shareClass": "Common", "value": 10000000},
                                {"period": "2026-03-31", "shareClass": "Common", "value": 12500000},
                            ]
                        },
                    }
                ]
            }

        def query_filings(
            self,
            query: str,
            sort: list[dict[str, Any]] | None = None,
            page_size: int = SEC_API_DILUTION_PAGE_SIZE,
        ) -> list[dict[str, Any]]:
            return []

        def full_text_search(
            self,
            query: str,
            form_types: Iterable[str],
            start_date: dt.date,
            end_date: dt.date,
            cik: str | None = None,
            max_pages: int = 3,
        ) -> list[dict[str, Any]]:
            return []

        def form_d_offerings(
            self,
            query: str,
            page_size: int = SEC_API_DILUTION_PAGE_SIZE,
        ) -> list[dict[str, Any]]:
            return []

    tracker = DilutionTracker(client=FakeDilutionSecApiClient())
    share_history = tracker.get_share_count_history("SAMP")
    events = tracker.get_potential_dilution_events("SAMP", "2026-01-01", "2026-06-01")
    candidates = tracker.get_candidate_filings("SAMP", "2026-01-01", "2026-06-01")
    summary = tracker.get_dilution_summary("SAMP", "2026-01-01", "2026-06-01")

    assert list(share_history.columns) == list(DILUTION_SHARE_COUNT_COLUMNS)
    assert list(events.columns) == list(DILUTION_EVENT_COLUMNS)
    assert list(candidates.columns) == list(DILUTION_CANDIDATE_COLUMNS)
    assert share_history["actual_dilution_pct"].iloc[-1] == 0.25
    assert "latest_shares_outstanding" in summary
    assert summary["latest_shares_outstanding"]["shares_outstanding"].iloc[0] == 12500000

    return {
        "share_count_history": share_history,
        "events": events,
        "candidates": candidates,
        "summary_keys": sorted(summary.keys()),
    }


def validate_form4_normalizer_sample() -> dict[str, Any]:
    """Exercise Form4Normalizer with a small embedded sec-api-like payload."""
    sample = {
        "accessionNo": "0000000000-26-000004",
        "filedAt": "2026-06-15T18:30:00-04:00",
        "documentType": "4",
        "periodOfReport": "2026-06-13",
        "issuer": {
            "cik": "1318605",
            "name": "Tesla, Inc.",
            "tradingSymbol": "TSLA",
        },
        "reportingOwner": {
            "cik": "1234567",
            "name": "Sample Insider",
            "relationship": {
                "isDirector": True,
                "isOfficer": False,
                "isTenPercentOwner": False,
                "isOther": False,
            },
        },
        "aff10b5One": False,
        "nonDerivativeTable": {
            "transactions": [
                {
                    "securityTitle": "Common Stock",
                    "transactionDate": "2026-06-13",
                    "coding": {"formType": "4", "code": "P", "equitySwapInvolved": False},
                    "amounts": {"shares": 100, "pricePerShare": 10.25, "acquiredDisposedCode": "A"},
                    "postTransactionAmounts": {"sharesOwnedFollowingTransaction": 1100},
                    "ownershipNature": {"directOrIndirectOwnership": "D"},
                },
                {
                    "securityTitle": "Common Stock",
                    "transactionDate": "2026-06-14",
                    "coding": {"formType": "4", "code": "S", "equitySwapInvolved": False},
                    "amounts": {"shares": 40, "pricePerShare": 11.00, "acquiredDisposedCode": "D"},
                    "postTransactionAmounts": {"sharesOwnedFollowingTransaction": 1060},
                    "ownershipNature": {"directOrIndirectOwnership": "D"},
                },
                {
                    "securityTitle": "Common Stock",
                    "transactionDate": "2026-06-14",
                    "coding": {"formType": "4", "code": "A", "equitySwapInvolved": False},
                    "amounts": {"shares": 5, "pricePerShare": 0, "acquiredDisposedCode": "A"},
                    "postTransactionAmounts": {"sharesOwnedFollowingTransaction": 1065},
                    "ownershipNature": {"directOrIndirectOwnership": "D"},
                },
            ]
        },
        "footnotes": [],
        "ownerSignatureName": "/s/ Sample Insider",
        "ownerSignatureNameDate": "2026-06-15",
    }
    normalizer = Form4Normalizer()
    filing_row = normalizer.filing_row(sample, "insider-trading:filedAt=2026-06-01..2026-06-30")
    transaction_rows = normalizer.non_derivative_transaction_rows(sample)

    assert filing_row["accession_no"] == sample["accessionNo"]
    assert filing_row["issuer_trading_symbol"] == "TSLA"
    assert filing_row["owner_is_director"] is True
    assert filing_row["owner_signature_date"] == dt.date(2026, 6, 15)
    assert len(transaction_rows) == 2
    assert transaction_rows[0]["transaction_code"] == "P"
    assert transaction_rows[0]["transaction_value"] == Decimal("1025.00")
    assert transaction_rows[1]["transaction_code"] == "S"

    return {"filing_row": filing_row, "transaction_rows": transaction_rows}


def validate_form4_endpoint_query_sample() -> dict[str, Any]:
    """Exercise Form 4 query construction without making API requests."""
    importer = Form4Importer(show_progress=False, min_request_interval=0)
    query = importer._query(
        symbols=["TSLA", "AAPL"],
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 31),
        date_field="filedAt",
        transaction_codes=("P", "S"),
        document_types=("4", "4/A"),
    )
    start, end = importer._resolve_date_range(None, dt.date(2026, 6, 21), 365)

    assert "issuer.tradingSymbol:TSLA" in query
    assert "issuer.tradingSymbol:AAPL" in query
    assert "nonDerivativeTable.transactions.coding.code:P" in query
    assert "nonDerivativeTable.transactions.coding.code:S" in query
    assert "filedAt:[2026-01-01 TO 2026-01-31]" in query
    assert start == dt.date(2025, 6, 21)
    assert end == dt.date(2026, 6, 21)

    return {"query": query, "default_range": (start, end)}


def form4_row_counts() -> dict[str, int]:
    """Return row counts for the local `form4` filings and transactions tables."""
    return Form4Database().row_counts()


def validate_form13f_normalizer_sample() -> dict[str, Any]:
    """
    Exercise Form13FNormalizer with a small embedded sec-api-like payload.

    This is useful as a quick notebook smoke test before running a live import.
    """
    sample = {
        "accessionNo": "0000000000-26-000001",
        "formType": "13F-HR",
        "periodOfReport": "2026-03-31",
        "filedAt": "2026-05-15T16:30:00-04:00",
        "cik": "1234567",
        "companyName": "Sample Manager LLC",
        "crdNumber": "12345",
        "secFileNumber": "801-12345",
        "form13FFileNumber": "028-12345",
        "filingManager": {
            "name": "Sample Manager LLC",
            "address": {
                "street1": "1 Main St",
                "city": "Chicago",
                "stateOrCountry": "IL",
                "zipCode": "60601",
            },
        },
        "signature": {
            "name": "Jane Doe",
            "title": "CEO",
            "phone": "312-555-0100",
            "signature": "/s/ Jane Doe",
            "city": "Chicago",
            "stateOrCountry": "IL",
            "signatureDate": "05-15-2026",
        },
        "tableEntryTotal": 1,
        "tableValueTotal": 1565000,
        "otherManagersReportingForThisManager": [],
        "otherIncludedManagers": [],
        "documentFormatFiles": [],
        "holdings": [
            {
                "nameOfIssuer": "ADVANCED MICRO DEVICES INC",
                "titleOfClass": "COM",
                "cusip": "007903107",
                "ticker": "AMD",
                "cik": "2488",
                "value": 1565000,
                "shrsOrPrnAmt": {"sshPrnamt": 18527, "sshPrnamtType": "SH"},
                "investmentDiscretion": "SOLE",
                "votingAuthority": {"Sole": 0, "Shared": 0, "None": 18527},
            }
        ],
    }
    normalizer = Form13FNormalizer()
    cover_row = normalizer.cover_page_row(
        sample,
        "cover-pages:periodOfReport=2026-03-31",
        "2026-05-16T09:00:00.000Z",
    )
    holding_rows = normalizer.holding_rows(
        sample,
        "holdings:periodOfReport=2026-03-31",
        "2026-05-16T09:00:00.000Z",
    )

    assert cover_row["accession_no"] == sample["accessionNo"]
    assert cover_row["signature_date"] == dt.date(2026, 5, 15)
    assert len(holding_rows) == 1
    assert holding_rows[0]["line_number"] == 1
    assert holding_rows[0]["voting_sole"] == Decimal("0")
    assert holding_rows[0]["issuer_cik"] == "2488"
    assert "holdings" not in json.loads(holding_rows[0]["raw_filing_json"])

    return {"cover_row": cover_row, "holding_rows": holding_rows}


def validate_form13f_endpoint_query_sample() -> dict[str, Any]:
    """Exercise endpoint query construction without making API requests."""
    importer = Form13FImporter(show_progress=False, min_request_interval=0)
    query = importer._cover_query(dt.date(2024, 9, 30), DEFAULT_13F_THRESHOLD)
    periods = importer._periods("2024-01-01", "2024-12-31")

    assert query == "tableValueTotal:[1000000000 TO *] AND periodOfReport:2024-09-30"
    assert periods == [
        dt.date(2024, 3, 31),
        dt.date(2024, 6, 30),
        dt.date(2024, 9, 30),
        dt.date(2024, 12, 31),
    ]

    return {"query": query, "periods": periods}


def validate_form13f_maintenance_sample() -> dict[str, Any]:
    """Exercise periodic-update date/query helpers without making API requests."""
    importer = Form13FImporter(show_progress=False, min_request_interval=0)
    feb_periods = importer._recent_reporting_periods("2026-02-15", quarters_back=1)
    may_periods = importer._recent_reporting_periods("2026-05-15", quarters_back=1)
    filed_query = importer._filed_at_query("2026-02-01", "2026-02-28", DEFAULT_13F_THRESHOLD)

    assert feb_periods == [dt.date(2025, 12, 31)]
    assert may_periods == [dt.date(2026, 3, 31)]
    assert filed_query == "tableValueTotal:[1000000000 TO *] AND filedAt:[2026-02-01 TO 2026-02-28]"

    return {
        "feb_periods": feb_periods,
        "may_periods": may_periods,
        "filed_query": filed_query,
    }


def form13f_row_counts() -> dict[str, int]:
    """Return row counts for the local `form13f` cover_pages and holdings tables."""
    return Form13FDatabase().row_counts()


def validate_executive_compensation_normalizer_sample() -> dict[str, Any]:
    """Exercise ExecutiveCompensationNormalizer with a small sec-api-like payload."""
    sample = {
        "year": 2024,
        "ticker": "MSFT",
        "cik": "789019",
        "name": "Sample Executive",
        "position": "Chief Executive Officer",
        "salary": 1500000,
        "bonus": 0,
        "stockAwards": 25000000,
        "optionAwards": 0,
        "nonEquityIncentiveCompensation": 3600000,
        "changeInPensionValueAndDeferredEarnings": 0,
        "otherCompensation": 150000,
        "total": 30250000,
    }
    normalizer = ExecutiveCompensationNormalizer()
    row = normalizer.compensation_row(sample, "compensation:year=2024..2024:symbols=MSFT")

    assert len(row["record_key"]) == 64
    assert row["ticker"] == "MSFT"
    assert row["cik"] == "789019"
    assert row["year"] == 2024
    assert row["stock_awards"] == Decimal("25000000")
    assert row["non_equity_incentive_compensation"] == Decimal("3600000")
    assert row["total"] == Decimal("30250000")
    assert json.loads(row["raw_json"])["ticker"] == "MSFT"

    return {"compensation_row": row}


def validate_executive_compensation_endpoint_query_sample() -> dict[str, Any]:
    """Exercise executive compensation query construction without API requests."""
    importer = ExecutiveCompensationImporter(show_progress=False, min_request_interval=0)
    query = importer._query(["MSFT", "AAPL", "BRK.B"], 2023, 2024)
    recent = importer._resolve_year_range(None, 2026)
    batches = list(importer._symbol_batches(["A", "B", "C"]))

    assert query == "year:[2023 TO 2024] AND ticker:(MSFT OR AAPL OR BRK.B)"
    assert recent == (DEFAULT_EXEC_COMP_START_YEAR, 2026)
    assert batches == [["A", "B", "C"]]

    return {"query": query, "default_range": recent, "batches": batches}


def executive_compensation_row_counts() -> dict[str, int]:
    """Return row counts for the local executive compensation table."""
    return ExecutiveCompensationDatabase().row_counts()


def filing_urls(symbol: str, form_type: str=None, filing_date: str=None, no_filings: int=5, query_size: int=10) -> list:
    """
    Return SEC filing HTML URLs from sec-api.io's Query API.

    This helper is the filing-discovery layer used by
    `Xbrl_Tags_Manager.find_statements_tags_current()`. It queries sec-api.io
    for filings matching a ticker and optional form type, then returns the
    `linkToHtml` values that can be passed to `XbrlApi.xbrl_to_json()`.

    Parameters
    ----------
    symbol:
        Ticker to query. The XBRL manager passes uppercase tickers here even
        though symbol-specific database tables are stored lowercase.
    form_type:
        Optional SEC form type such as `"10-K"` or `"10-Q"`. When provided, the
        query is restricted to that form type.
    filing_date:
        Optional date filter appended to the sec-api query as `filedAt:<date>`.
        Use the date syntax expected by sec-api.io.
    no_filings:
        Maximum number of filing URLs to return. Pass `None` to return every
        filing URL included in the query response.
    query_size:
        Number of records requested from sec-api.io. Increase this if the query
        does not return enough filings for the requested form type.

    Examples
    --------
    ```python
    filing_urls("AAPL", "10-K", no_filings=1)
    filing_urls("MSFT", "10-Q", no_filings=3)
    ```

    Returns
    -------
    list
        Filing HTML URLs suitable for `XbrlApi.xbrl_to_json(htm_url=url)`.
    """
    query_start = 0
    query_size=query_size
    queryApi = QueryApi(sec_api_key)

    def internal_query(query_start=query_start, query_size=query_size):     
        if form_type == None:
            query = {
                "query": { "query_string": { 
                    "query": "ticker:"+ symbol,
                } },
                "from": str(query_start),
                "size": str(query_size),
                "sort": [{ "filedAt": { "order": "desc" } }]
                }
        else:
            query = {
                "query": { "query_string": { 
                    "query": "ticker:"+ symbol + " AND formType:\"" +  form_type + "\"",
                } },
                "from": str(query_start),
                "size": str(query_size),
                "sort": [{ "filedAt": { "order": "desc" } }]
                }
        if filing_date == None:
            pass
        else:
            query = {
                "query": {"query_string": {
                    "query": query["query"]["query_string"]["query"] + f" AND filedAt:{filing_date}",
                }},
                "from": str(query_start),
                "size": str(query_size),
                "sort": [{ "filedAt": { "order": "desc" } }]
                }
        response = queryApi.get_filings(query)
        return response
    que =internal_query()
    form_filings = [
        item['linkToHtml']
        for item in que.get('filings', [])
        if form_type == None or item.get('formType') == form_type
    ]

    if no_filings is None:
        return form_filings

    return form_filings[:no_filings]

def single_fundamental(
    symbol: str,
    form_type: str,
    fundamental: str,
    plot: bool=False,
    xbrl_tags_manager=None,
    ) -> pd.DataFrame:
    #TODO add function that will identify the path to the specified fundamental in the xbrl_tags_dict. This may need to be an
    #TODO algorithm that could be in a separate class so that it's easier to repurpose.
    """
    Return one normalized fundamental time series for a symbol.

    This function preserves the old lookup behavior while using the new storage
    pattern established in `xbrl_tags_manager.py`. Symbol-specific XBRL tags are
    read from an `Xbrl_Tags_Manager` instance, whose `xbrl_tags_saved` payload is
    loaded from the `xbrl_symbol_tags` database.

    Parameters
    ----------
    symbol:
        Stock symbol. Uppercase and lowercase are both accepted for tag lookup;
        SEC filing queries are sent uppercase.
    form_type:
        SEC form type to query, such as `"10-K"`, `"10-Q"`, or `"20-F"`.
    fundamental:
        Normalized fundamental name to retrieve, such as `"Revenue"`,
        `"EPSDiluted"`, or `"CashFromOperations"`.
    plot:
        If True, plot the returned values in chronological order.
    xbrl_tags_manager:
        Optional existing `Xbrl_Tags_Manager`. Pass this when making repeated
        calls so the database-backed tag maps are not reloaded each time.

    Examples
    --------
    ```python
    df = single_fundamental("AAPL", "10-K", "Revenue")

    manager = Xbrl_Tags_Manager(["AAPL", "MSFT"])
    df = single_fundamental("MSFT", "10-K", "EPSDiluted", xbrl_tags_manager=manager)
    ```

    Returns
    -------
    pd.DataFrame
        Normalized XBRL fact rows for the requested fundamental.
    """
    if xbrl_tags_manager is None:
        from .xbrl_tags_manager import Xbrl_Tags_Manager

        xbrl_tags_manager = Xbrl_Tags_Manager([symbol])

    xbrl_tags_saved = xbrl_tags_manager.xbrl_tags_saved
    filings_10k = filing_urls(symbol.upper(), form_type)
    xbrl_api = XbrlApi(sec_api_key)
    df = pd.DataFrame()

    def tag_finder(sym, target):
        for k,v in xbrl_tags_saved[sym].items():
            if type(v) == dict:
                for k2, v2 in v.items():
                    if type(v2) == list:
                        if k2 == target:
                            return xbrl_tags_saved[sym][k][k2]
                    elif type(v2) != list:
                        raise ValueError('Expected list in second level of nested dictionary!')
            elif type(v) != dict:
                raise TypeError('Type should be dictionary. Review xbrl_symbol_tags database payload.')
        raise KeyError(f'Symbol not in dictionary/file.-tag_finder-fundamental:{target} ')

    tag_target = tag_finder(symbol, fundamental)

    def statement_finder(sym, target):
        for k,v in xbrl_tags_saved[sym].items():
            if type(v) == dict:
                for k2, v2 in v.items():
                    if type(v2) == list:
                        if k2 == target:
                            return xbrl_tags_saved[sym][k]['statement tags'][0]#! The return value was k. I changed it resolve an error in the subsequent line containing the df variable.
                    elif type(v2) != list:
                        raise ValueError('Expected list in second level of nested dictionary!')
            elif type(v) != dict:
                raise TypeError('Type should be dictionary. Review xbrl_symbol_tags database payload.')
        raise KeyError(f'Symbol not in dictionary/file.-statement_finder-fundamental{target} ')

    target_statement = statement_finder(symbol, fundamental)

    #if/elif for handling symbols that have been found to have multiple xbrl tags for a fundamental.
    if len(tag_target) == 1:
        for sub in filings_10k:
            try:
                df = pd.concat([df, pd.json_normalize(xbrl_api.xbrl_to_json(htm_url=sub)[target_statement][''.join(tag_target)])[:3]])#!The error is in the tag_target key in this line.
            except KeyError:
                continue
    elif len(tag_target) > 1:
        for sub in filings_10k:
            for tag in tag_target:
                try:
                    df = pd.concat([df, pd.json_normalize(xbrl_api.xbrl_to_json(htm_url=sub)[target_statement][''.join(tag)])[:3]])
                except:
                    continue
    try:
        df = df.drop_duplicates()
    except TypeError:
        df = df[['decimals', 'unitRef', 'value', 'period.startDate', 'period.endDate']].drop_duplicates()
    if 'period.endDate' in df.columns:
            df.sort_values('period.endDate', ascending=False)#!Not all dataframes will contain period.endDate.
            df.index = df['period.endDate']
    elif 'period.instant' in df.columns:
            df.sort_values('period.instant', ascending=False)
            df.index = df['period.instant']
    if plot == False:
            return df
    elif plot == True:
            df['value'][::-1].astype(float).plot(rot=20, ylabel=fundamental)
