from __future__ import annotations

import datetime as dt
import json
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
SEC_API_BASE_URL = "https://api.sec-api.io"
FORM13F_COVER_PAGES_ENDPOINT = f"{SEC_API_BASE_URL}/form-13f/cover-pages"
FORM13F_HOLDINGS_ENDPOINT = f"{SEC_API_BASE_URL}/form-13f/holdings"
DEFAULT_13F_THRESHOLD = 1_000_000_000
DEFAULT_13F_START_PERIOD = "2013-01-01"
SEC_API_13F_PAGE_SIZE = 50
SEC_API_13F_FROM_LIMIT = 10_000


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


Form13F = Form13FImporter


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
