from __future__ import annotations

import csv
import datetime as dt
import math
import re
import sys
import tkinter as tk
import unicodedata
import webbrowser
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.filings import Form13FDatabase  # type: ignore[import-not-found]


ALL_MANAGERS = "All imported managers"
ALL_CLASSIFICATIONS = "All classifications"
UNCLASSIFIED = "Unclassified"
NON_OPTION_POSITIONS = "Non-option positions"
ALL_POSITIONS = "All reported positions"
OPTIONS_ONLY = "Options only"
POSITION_SCOPES = (NON_OPTION_POSITIONS, ALL_POSITIONS, OPTIONS_ONLY)
DEFAULT_LIMIT = 500
POLL_INTERVAL_MS = 100
CORPORATE_SUFFIXES = frozenset(
    {
        "co",
        "company",
        "corp",
        "corporation",
        "inc",
        "incorporated",
        "llc",
        "llp",
        "lp",
        "ltd",
        "limited",
        "plc",
    }
)


@dataclass(frozen=True)
class Column:
    key: str
    label: str
    width: int = 120
    anchor: str = tk.W
    formatter: Callable[[Any], str] | None = None


@dataclass(frozen=True)
class Catalog:
    periods: tuple[dt.date, ...]
    managers: tuple[tuple[str, str], ...]
    sectors: tuple[str, ...]
    industries: tuple[str, ...]
    cover_count: int
    holding_count: int


@dataclass(frozen=True)
class Overview:
    period: dt.date
    metrics: dict[str, Any]
    managers: list[dict[str, Any]]
    securities: list[dict[str, Any]]


def clean_record(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


def format_date(value: Any) -> str:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()[:10]
    return "" if value is None else str(value)


def format_datetime(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return format_date(value)


def format_integer(value: Any) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return str(value)


def format_money(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    sign = "-" if number < 0 else ""
    number = abs(number)
    if number >= 1_000_000_000_000:
        return f"{sign}${number / 1_000_000_000_000:.2f}T"
    if number >= 1_000_000_000:
        return f"{sign}${number / 1_000_000_000:.2f}B"
    if number >= 1_000_000:
        return f"{sign}${number / 1_000_000:.2f}M"
    if number >= 1_000:
        return f"{sign}${number / 1_000:.2f}K"
    return f"{sign}${number:,.0f}"


def format_percent(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return "" if not math.isfinite(number) else f"{number:.2f}%"


def raw_sort_key(value: Any) -> tuple[int, Any]:
    if value is None or value == "":
        return (1, "")
    if isinstance(value, (int, float, Decimal, dt.date, dt.datetime)):
        return (0, value)
    return (0, str(value).casefold())


def parse_positive_int(value: str, default: int = DEFAULT_LIMIT) -> int:
    try:
        return max(1, min(int(value), 10_000))
    except ValueError:
        return default


def normalize_manager_name(value: str) -> str:
    """Normalize a filer name for punctuation-insensitive, suffix-optional search."""
    decomposed = unicodedata.normalize("NFKD", value.casefold().replace("&", " and "))
    ascii_text = "".join(char for char in decomposed if not unicodedata.combining(char))
    words = re.findall(r"[a-z0-9]+", ascii_text)
    while words and words[-1] in CORPORATE_SUFFIXES:
        words.pop()
    return " ".join(words)


def manager_search_score(name: str, cik: str, query: str) -> tuple[int, int, str] | None:
    """Rank normalized partial-name and CIK matches; lower scores are better."""
    normalized_query = normalize_manager_name(query)
    if not normalized_query:
        return (0, len(name), name.casefold())

    normalized_name = normalize_manager_name(name)
    query_compact = normalized_query.replace(" ", "")
    name_compact = normalized_name.replace(" ", "")
    query_tokens = normalized_query.split()
    name_tokens = normalized_name.split()
    digits = re.sub(r"\D", "", query)

    if digits and digits == cik:
        rank = 0
    elif digits and cik.startswith(digits):
        rank = 1
    elif normalized_name == normalized_query:
        rank = 0
    elif normalized_name.startswith(normalized_query):
        rank = 1
    elif query_compact and query_compact in name_compact:
        rank = 2
    elif all(any(token.startswith(part) for token in name_tokens) for part in query_tokens):
        rank = 3
    elif all(part in normalized_name for part in query_tokens):
        rank = 4
    else:
        return None
    return (rank, len(normalized_name), normalized_name)


class Form13FRepository:
    """Read-only queries over the tables populated by Form13FImporter."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def load_catalog(self) -> Catalog:
        with self.engine.connect() as conn:
            periods = tuple(
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT DISTINCT period_of_report
                        FROM cover_pages
                        WHERE period_of_report IS NOT NULL
                        ORDER BY period_of_report DESC
                        """
                    )
                )
            )
            managers = tuple(
                (str(row.cik), str(row.company_name or row.cik))
                for row in conn.execute(
                    text(
                        """
                        SELECT
                            cik,
                            COALESCE(
                                MAX(NULLIF(filing_manager_name, '')),
                                MAX(NULLIF(company_name, '')),
                                cik
                            ) AS company_name
                        FROM cover_pages
                        WHERE cik IS NOT NULL AND cik <> ''
                        GROUP BY cik
                        ORDER BY company_name, cik
                        """
                    )
                )
            )
            cover_count = int(
                conn.execute(text("SELECT COUNT(*) FROM cover_pages")).scalar() or 0
            )
            holding_count = int(
                conn.execute(
                    text(
                        """
                        SELECT TABLE_ROWS
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'holdings'
                        """
                    )
                ).scalar()
                or 0
            )
            try:
                classifications = conn.execute(
                    text(
                        """
                        SELECT DISTINCT sector, industry
                        FROM stocks.symbol_sector_industry
                        """
                    )
                )
                sector_values: set[str] = set()
                industry_values: set[str] = set()
                for row in classifications:
                    if row.sector and str(row.sector).strip():
                        sector_values.add(str(row.sector).strip())
                    if row.industry and str(row.industry).strip():
                        industry_values.add(str(row.industry).strip())
                sectors = tuple(sorted(sector_values, key=str.casefold))
                industries = tuple(sorted(industry_values, key=str.casefold))
            except SQLAlchemyError:
                sectors = ()
                industries = ()
        return Catalog(periods, managers, sectors, industries, cover_count, holding_count)

    def overview(self, period: dt.date) -> Overview:
        latest_cte = self._latest_filings_cte()
        with self.engine.connect() as conn:
            metrics = clean_record(
                conn.execute(
                    text(
                        latest_cte
                        + """
                        SELECT
                            COUNT(*) AS manager_count,
                            COALESCE(SUM(table_value_total), 0) AS disclosed_value,
                            COALESCE(SUM(table_entry_total), 0) AS reported_entries,
                            MAX(filed_at) AS latest_filed_at
                        FROM latest_filings
                        """
                    ),
                    {"period": period},
                ).one()
            )
            managers = [
                clean_record(row)
                for row in conn.execute(
                    text(
                        latest_cte
                        + """
                        SELECT
                            cik,
                            COALESCE(
                                NULLIF(filing_manager_name, ''),
                                NULLIF(company_name, ''),
                                cik
                            ) AS company_name,
                            form_type,
                            report_type,
                            filed_at,
                            table_value_total,
                            table_entry_total,
                            accession_no
                        FROM latest_filings
                        ORDER BY table_value_total DESC, company_name
                        """
                    ),
                    {"period": period},
                )
            ]
        return Overview(period, metrics, managers, [])

    def top_securities(self, period: dt.date) -> list[dict[str, Any]]:
        query = text(
            self._latest_filings_cte()
            + """
            SELECT
                COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer) AS security,
                MAX(h.name_of_issuer) AS issuer,
                MAX(h.cusip) AS cusip,
                SUM(h.value) AS disclosed_value,
                COUNT(DISTINCT lf.cik) AS manager_count
            FROM latest_filings lf
            JOIN holdings h ON h.accession_no = lf.accession_no
            WHERE h.period_of_report = :period
            GROUP BY COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer)
            ORDER BY disclosed_value DESC
            LIMIT 100
            """
        )
        with self.engine.connect() as conn:
            return [
                clean_record(row)
                for row in conn.execute(query, {"period": period})
            ]

    def manager_holdings(
        self,
        manager_cik: str,
        period: dt.date,
        search: str = "",
        sector: str | None = None,
        industry: str | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> list[dict[str, Any]]:
        filters = ""
        params: dict[str, Any] = {
            "period": period,
            "manager_cik": manager_cik,
            "limit": limit,
        }
        if search:
            filters += """
              AND (
                    h.ticker LIKE :search
                 OR h.cusip LIKE :search
                 OR h.name_of_issuer LIKE :search
              )
            """
            params["search"] = f"%{search}%"
        if sector:
            filters += (
                " AND COALESCE(NULLIF(si.sector, ''), 'Unclassified') = :sector"
            )
            params["sector"] = sector
        if industry:
            filters += (
                " AND COALESCE(NULLIF(si.industry, ''), 'Unclassified') = :industry"
            )
            params["industry"] = industry
        query = text(
            """
            WITH ranked AS (
                SELECT
                    cp.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY cp.cik, cp.period_of_report
                        ORDER BY cp.filed_at DESC, cp.accession_no DESC
                    ) AS rn
                FROM cover_pages cp
                WHERE cp.period_of_report = :period
                  AND cp.cik = :manager_cik
            ),
            selected AS (
                SELECT * FROM ranked WHERE rn = 1
            ),
            positions AS (
                SELECT
                    h.ticker,
                    h.cusip,
                    MAX(h.name_of_issuer) AS name_of_issuer,
                    MAX(h.title_of_class) AS title_of_class,
                    MAX(h.put_call) AS put_call,
                    MAX(h.ssh_prnamt_type) AS share_type,
                    COALESCE(MAX(NULLIF(si.sector, '')), 'Unclassified') AS sector,
                    COALESCE(MAX(NULLIF(si.industry, '')), 'Unclassified') AS industry,
                    SUM(h.value) AS disclosed_value,
                    SUM(h.ssh_prnamt) AS shares,
                    SUM(h.voting_sole) AS voting_sole,
                    SUM(h.voting_shared) AS voting_shared,
                    SUM(h.voting_none) AS voting_none
                FROM selected s
                JOIN holdings h ON h.accession_no = s.accession_no
                LEFT JOIN stocks.symbol_sector_industry si
                  ON si.symbol = h.ticker
                WHERE 1 = 1
                """
            + filters
            + """
                GROUP BY
                    h.ticker,
                    h.cusip,
                    h.title_of_class,
                    h.put_call,
                    h.ssh_prnamt_type
            )
            SELECT
                *,
                disclosed_value
                    / NULLIF((SELECT table_value_total FROM selected), 0)
                    * 100 AS portfolio_weight
            FROM positions
            ORDER BY disclosed_value DESC
            LIMIT :limit
            """
        )
        with self.engine.connect() as conn:
            return [clean_record(row) for row in conn.execute(query, params)]

    def classification_leaders(
        self,
        period: dt.date,
        level: str,
        category: str,
        scope: str,
        ranking: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        category_expression = self._category_expression(level)
        scope_filter = self._position_scope_filter(scope)
        order_by = {
            "Disclosed value": "category_value DESC, portfolio_weight DESC",
            "Portfolio concentration": "portfolio_weight DESC, category_value DESC",
        }.get(ranking)
        if order_by is None:
            raise ValueError(f"Unsupported ranking: {ranking}")
        summary_query = text(
            self._latest_filings_cte()
            + f"""
            SELECT
                lf.cik,
                COALESCE(
                    NULLIF(lf.filing_manager_name, ''),
                    NULLIF(lf.company_name, ''),
                    lf.cik
                ) AS company_name,
                MAX(lf.table_value_total) AS portfolio_value,
                :category AS category,
                SUM(COALESCE(h.value, 0)) AS category_value,
                SUM(COALESCE(h.value, 0))
                    / NULLIF(MAX(lf.table_value_total), 0) * 100 AS portfolio_weight,
                COUNT(
                    DISTINCT CONCAT_WS(
                        '|',
                        COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer),
                        COALESCE(h.title_of_class, ''),
                        COALESCE(h.put_call, '')
                    )
                ) AS position_count
            FROM latest_filings lf
            JOIN holdings h ON h.accession_no = lf.accession_no
            LEFT JOIN stocks.symbol_sector_industry si
              ON si.symbol = h.ticker
            WHERE h.period_of_report = :period
              AND {category_expression} = :category
            {scope_filter}
            GROUP BY
                lf.cik,
                COALESCE(
                    NULLIF(lf.filing_manager_name, ''),
                    NULLIF(lf.company_name, ''),
                    lf.cik
                )
            ORDER BY {order_by}, company_name
            LIMIT :limit
            """
        )
        params = {"period": period, "category": category, "limit": limit}
        with self.engine.connect() as conn:
            rows = [clean_record(row) for row in conn.execute(summary_query, params)]
            if not rows:
                return rows
            leader_ciks = tuple(str(row["cik"]) for row in rows)
            largest_query = text(
                self._latest_filings_cte()
                + f"""
                , positions AS (
                    SELECT
                        lf.cik,
                        COALESCE(
                            NULLIF(h.ticker, ''),
                            h.cusip,
                            h.name_of_issuer
                        ) AS security,
                        MAX(h.name_of_issuer) AS name_of_issuer,
                        SUM(COALESCE(h.value, 0)) AS disclosed_value
                    FROM latest_filings lf
                    JOIN holdings h ON h.accession_no = lf.accession_no
                    LEFT JOIN stocks.symbol_sector_industry si
                      ON si.symbol = h.ticker
                    WHERE h.period_of_report = :period
                      AND lf.cik IN :leader_ciks
                      AND {category_expression} = :category
                    {scope_filter}
                    GROUP BY
                        lf.cik,
                        COALESCE(
                            NULLIF(h.ticker, ''),
                            h.cusip,
                            h.name_of_issuer
                        )
                ),
                ranked_largest AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY cik
                            ORDER BY disclosed_value DESC, security
                        ) AS position_rank
                    FROM positions
                )
                SELECT
                    cik,
                    security AS largest_security,
                    name_of_issuer AS largest_issuer,
                    disclosed_value AS largest_value
                FROM ranked_largest
                WHERE position_rank = 1
                """
            ).bindparams(bindparam("leader_ciks", expanding=True))
            largest_by_cik = {
                str(row.cik): clean_record(row)
                for row in conn.execute(
                    largest_query,
                    {
                        "period": period,
                        "category": category,
                        "leader_ciks": leader_ciks,
                    },
                )
            }
        for row in rows:
            row.update(largest_by_cik.get(str(row["cik"]), {}))
        return rows

    def manager_classification(
        self,
        manager_cik: str,
        period: dt.date,
        level: str,
        scope: str,
    ) -> list[dict[str, Any]]:
        category_expression = self._category_expression(level)
        scope_filter = self._position_scope_filter(scope)
        query = text(
            self._latest_filings_cte()
            + f"""
            , classified_positions AS (
                SELECT
                    lf.cik,
                    lf.table_value_total AS portfolio_value,
                    {category_expression} AS category,
                    COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer) AS security,
                    MAX(h.name_of_issuer) AS name_of_issuer,
                    SUM(COALESCE(h.value, 0)) AS disclosed_value
                FROM latest_filings lf
                JOIN holdings h ON h.accession_no = lf.accession_no
                LEFT JOIN stocks.symbol_sector_industry si
                  ON si.symbol = h.ticker
                WHERE h.period_of_report = :period
                  AND lf.cik = :manager_cik
                {scope_filter}
                GROUP BY
                    lf.cik,
                    lf.table_value_total,
                    {category_expression},
                    COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer)
            ),
            ranked_positions AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY category
                        ORDER BY disclosed_value DESC, security
                    ) AS position_rank,
                    SUM(disclosed_value) OVER () AS scope_value
                FROM classified_positions
            )
            SELECT
                category,
                SUM(disclosed_value) AS disclosed_value,
                SUM(disclosed_value)
                    / NULLIF(MAX(portfolio_value), 0) * 100 AS portfolio_weight,
                MAX(scope_value) AS scope_value,
                MAX(portfolio_value) AS portfolio_value,
                COUNT(*) AS position_count,
                MAX(CASE WHEN position_rank = 1 THEN security END) AS largest_security,
                MAX(CASE WHEN position_rank = 1 THEN name_of_issuer END) AS largest_issuer,
                MAX(CASE WHEN position_rank = 1 THEN disclosed_value END) AS largest_value
            FROM ranked_positions
            GROUP BY category
            ORDER BY disclosed_value DESC, category
            """
        )
        params = {"period": period, "manager_cik": manager_cik}
        with self.engine.connect() as conn:
            return [clean_record(row) for row in conn.execute(query, params)]

    @staticmethod
    def _category_expression(level: str) -> str:
        column = {"Sector": "sector", "Industry": "industry"}.get(level)
        if column is None:
            raise ValueError(f"Unsupported classification level: {level}")
        return f"COALESCE(NULLIF(si.{column}, ''), 'Unclassified')"

    @staticmethod
    def _position_scope_filter(scope: str) -> str:
        if scope == ALL_POSITIONS:
            return ""
        if scope == NON_OPTION_POSITIONS:
            return "AND (h.put_call IS NULL OR h.put_call = '')"
        if scope == OPTIONS_ONLY:
            return "AND h.put_call IS NOT NULL AND h.put_call <> ''"
        raise ValueError(f"Unsupported position scope: {scope}")

    def position_changes(
        self,
        current_period: dt.date,
        prior_period: dt.date,
        manager_cik: str | None,
        action: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        manager_filter = ""
        params: dict[str, Any] = {
            "current_period": current_period,
            "prior_period": prior_period,
            "limit": limit,
        }
        if manager_cik:
            manager_filter = "AND cp.cik = :manager_cik"
            params["manager_cik"] = manager_cik
        action_filter = ""
        if action != "All":
            action_filter = "WHERE action = :action"
            params["action"] = action.lower()

        query = text(
            f"""
            WITH ranked AS (
                SELECT
                    cp.accession_no,
                    cp.cik,
                    cp.period_of_report,
                    ROW_NUMBER() OVER (
                        PARTITION BY cp.cik, cp.period_of_report
                        ORDER BY cp.filed_at DESC, cp.accession_no DESC
                    ) AS rn
                FROM cover_pages cp
                WHERE cp.period_of_report IN (:current_period, :prior_period)
                {manager_filter}
            ),
            selected AS (
                SELECT accession_no, period_of_report
                FROM ranked
                WHERE rn = 1
            ),
            positions AS (
                SELECT
                    s.period_of_report,
                    CONCAT_WS(
                        '|',
                        COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer),
                        COALESCE(h.title_of_class, ''),
                        COALESCE(h.put_call, '')
                    ) AS security_key,
                    MAX(h.ticker) AS ticker,
                    MAX(h.cusip) AS cusip,
                    MAX(h.name_of_issuer) AS name_of_issuer,
                    MAX(h.put_call) AS put_call,
                    SUM(h.value) AS disclosed_value,
                    SUM(
                        CASE
                            WHEN h.ssh_prnamt_type = 'SH'
                             AND (h.put_call IS NULL OR h.put_call = '')
                            THEN h.ssh_prnamt
                            ELSE 0
                        END
                    ) AS shares
                FROM selected s
                JOIN holdings h ON h.accession_no = s.accession_no
                GROUP BY
                    s.period_of_report,
                    CONCAT_WS(
                        '|',
                        COALESCE(NULLIF(h.ticker, ''), h.cusip, h.name_of_issuer),
                        COALESCE(h.title_of_class, ''),
                        COALESCE(h.put_call, '')
                    )
            ),
            security_keys AS (
                SELECT DISTINCT security_key FROM positions
            ),
            compared AS (
                SELECT
                    k.security_key,
                    COALESCE(MAX(c.ticker), MAX(p.ticker)) AS ticker,
                    COALESCE(MAX(c.cusip), MAX(p.cusip)) AS cusip,
                    COALESCE(MAX(c.name_of_issuer), MAX(p.name_of_issuer)) AS name_of_issuer,
                    COALESCE(MAX(c.put_call), MAX(p.put_call)) AS put_call,
                    COALESCE(MAX(c.disclosed_value), 0) AS current_value,
                    COALESCE(MAX(p.disclosed_value), 0) AS prior_value,
                    COALESCE(MAX(c.disclosed_value), 0)
                        - COALESCE(MAX(p.disclosed_value), 0) AS value_change,
                    COALESCE(MAX(c.shares), 0) AS current_shares,
                    COALESCE(MAX(p.shares), 0) AS prior_shares,
                    COALESCE(MAX(c.shares), 0)
                        - COALESCE(MAX(p.shares), 0) AS shares_change
                FROM security_keys k
                LEFT JOIN positions c
                  ON c.security_key = k.security_key
                 AND c.period_of_report = :current_period
                LEFT JOIN positions p
                  ON p.security_key = k.security_key
                 AND p.period_of_report = :prior_period
                GROUP BY k.security_key
            ),
            classified AS (
                SELECT
                    *,
                    CASE
                        WHEN prior_value = 0 AND current_value > 0 THEN 'new'
                        WHEN prior_value > 0 AND current_value = 0 THEN 'exited'
                        WHEN value_change > 0 THEN 'bought'
                        WHEN value_change < 0 THEN 'sold'
                        ELSE 'unchanged'
                    END AS action,
                    ABS(value_change) AS absolute_change
                FROM compared
            )
            SELECT *
            FROM classified
            {action_filter}
            ORDER BY absolute_change DESC, security_key
            LIMIT :limit
            """
        )
        with self.engine.connect() as conn:
            return [clean_record(row) for row in conn.execute(query, params)]

    def security_owners(
        self,
        query_text: str,
        period: dt.date,
        limit: int = DEFAULT_LIMIT,
    ) -> list[dict[str, Any]]:
        def owner_query(match_clause: str, index_hint: str = "") -> Any:
            return text(
                f"""
            SELECT
                cp.cik,
                COALESCE(
                    NULLIF(cp.filing_manager_name, ''),
                    NULLIF(cp.company_name, ''),
                    cp.cik
                ) AS company_name,
                MAX(h.ticker) AS ticker,
                MAX(h.cusip) AS cusip,
                MAX(h.name_of_issuer) AS name_of_issuer,
                SUM(h.value) AS disclosed_value,
                SUM(h.ssh_prnamt) AS shares,
                h.put_call,
                h.title_of_class,
                cp.filed_at,
                cp.accession_no
            FROM holdings h {index_hint}
            JOIN cover_pages cp ON cp.accession_no = h.accession_no
            WHERE h.period_of_report = :period
              AND ({match_clause})
              AND NOT EXISTS (
                    SELECT 1
                    FROM cover_pages newer
                    WHERE newer.cik = cp.cik
                      AND newer.period_of_report = cp.period_of_report
                      AND (
                            newer.filed_at > cp.filed_at
                         OR (
                                newer.filed_at = cp.filed_at
                            AND newer.accession_no > cp.accession_no
                         )
                      )
              )
            GROUP BY
                cp.cik,
                COALESCE(
                    NULLIF(cp.filing_manager_name, ''),
                    NULLIF(cp.company_name, ''),
                    cp.cik
                ),
                h.ticker,
                h.cusip,
                h.title_of_class,
                h.put_call,
                cp.filed_at,
                cp.accession_no
            ORDER BY disclosed_value DESC
            LIMIT :limit
            """
            )

        params = {
            "period": period,
            "exact": query_text.upper(),
            "search": f"%{query_text}%",
            "limit": limit,
        }
        with self.engine.connect() as conn:
            rows = [
                clean_record(row)
                for row in conn.execute(
                    owner_query(
                        "h.ticker = :exact",
                        "FORCE INDEX (idx_holdings_ticker)",
                    ),
                    params,
                )
            ]
            if rows:
                return rows
            rows = [
                clean_record(row)
                for row in conn.execute(
                    owner_query(
                        "h.cusip = :exact",
                        "FORCE INDEX (idx_holdings_cusip)",
                    ),
                    params,
                )
            ]
            if rows:
                return rows
            return [
                clean_record(row)
                for row in conn.execute(
                    owner_query("h.name_of_issuer LIKE :search"),
                    params,
                )
            ]

    def filings(
        self,
        period: dt.date | None,
        manager_cik: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        clauses = ["1 = 1"]
        params: dict[str, Any] = {"limit": limit}
        if period:
            clauses.append("period_of_report = :period")
            params["period"] = period
        if manager_cik:
            clauses.append("cik = :manager_cik")
            params["manager_cik"] = manager_cik
        query = text(
            f"""
            SELECT
                accession_no,
                form_type,
                period_of_report,
                filed_at,
                cik,
                COALESCE(
                    NULLIF(filing_manager_name, ''),
                    NULLIF(company_name, ''),
                    cik
                ) AS company_name,
                report_type,
                table_entry_total,
                table_value_total,
                other_included_managers_count
            FROM cover_pages
            WHERE {' AND '.join(clauses)}
            ORDER BY filed_at DESC, company_name
            LIMIT :limit
            """
        )
        with self.engine.connect() as conn:
            return [clean_record(row) for row in conn.execute(query, params)]

    @staticmethod
    def _latest_filings_cte() -> str:
        return """
        WITH ranked AS (
            SELECT
                cp.*,
                ROW_NUMBER() OVER (
                    PARTITION BY cp.cik, cp.period_of_report
                    ORDER BY cp.filed_at DESC, cp.accession_no DESC
                ) AS rn
            FROM cover_pages cp
            WHERE cp.period_of_report = :period
        ),
        latest_filings AS (
            SELECT * FROM ranked WHERE rn = 1
        )
        """


class DataTable(ttk.Frame):
    def __init__(
        self,
        parent: tk.Misc,
        columns: Iterable[Column],
        *,
        selectmode: str = "browse",
    ) -> None:
        super().__init__(parent)
        self.columns = tuple(columns)
        self.rows: list[dict[str, Any]] = []
        self.sort_column: str | None = None
        self.sort_descending = False

        self.tree = ttk.Treeview(
            self,
            columns=tuple(column.key for column in self.columns),
            show="headings",
            selectmode=selectmode,
        )
        y_scroll = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        x_scroll = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self._configure_columns()

    def _configure_columns(self) -> None:
        for column in self.columns:
            self.tree.heading(
                column.key,
                text=column.label,
                command=lambda key=column.key: self.sort_by(key),
            )
            self.tree.column(
                column.key,
                width=column.width,
                minwidth=65,
                anchor=column.anchor,
                stretch=column.anchor == tk.W,
            )

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.sort_column = None
        self.sort_descending = False
        self._render()

    def _render(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for index, row in enumerate(self.rows):
            values = []
            for column in self.columns:
                value = row.get(column.key)
                values.append(column.formatter(value) if column.formatter else format_date(value))
            self.tree.insert("", tk.END, iid=str(index), values=values)

    def sort_by(self, key: str) -> None:
        if self.sort_column == key:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = key
            self.sort_descending = False
        self.rows.sort(key=lambda row: raw_sort_key(row.get(key)), reverse=self.sort_descending)
        self._render()
        for column in self.columns:
            arrow = ""
            if column.key == key:
                arrow = " ▼" if self.sort_descending else " ▲"
            self.tree.heading(column.key, text=column.label + arrow)

    def selected_row(self) -> dict[str, Any] | None:
        selection = self.tree.selection()
        if not selection:
            return None
        index = int(selection[0])
        return self.rows[index] if index < len(self.rows) else None

    def export_csv(self, path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8-sig") as output:
            writer = csv.DictWriter(
                output,
                fieldnames=[column.key for column in self.columns],
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(self.rows)


class Form13FDashboard:
    def __init__(self, root: tk.Tk, repository: Form13FRepository) -> None:
        self.root = root
        self.repository = repository
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="form13f-gui")
        self.pending: list[
            tuple[Future[Any], Callable[[Any], None], str]
        ] = []
        self.poll_after_id: str | None = None
        self.closing = False
        self.catalog: Catalog | None = None
        self.manager_by_label: dict[str, str | None] = {ALL_MANAGERS: None}
        self.label_by_cik: dict[str, str] = {}
        self.manager_options: list[tuple[str, str, str]] = []

        self.root.title("SEC Form 13F Analytics")
        self.root.geometry("1380x860")
        self.root.minsize(980, 650)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self.status_var = tk.StringVar(value="Connecting to the form13f database…")
        self._build_widgets()
        self._submit(self.repository.load_catalog, self._catalog_loaded, "Load database")

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=8)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        title_frame = ttk.Frame(container)
        title_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(
            title_frame,
            text="SEC Form 13F Analytics",
            font=("Segoe UI", 16, "bold"),
        ).pack(side=tk.LEFT)
        ttk.Button(title_frame, text="Export Current Table…", command=self.export_current).pack(
            side=tk.RIGHT
        )

        self.notebook = ttk.Notebook(container)
        self.notebook.grid(row=1, column=0, sticky="nsew")
        self._build_overview_tab()
        self._build_holdings_tab()
        self._build_changes_tab()
        self._build_security_tab()
        self._build_classification_tab()
        self._build_filings_tab()

        footer = ttk.Frame(container)
        footer.grid(row=2, column=0, sticky="ew", pady=(6, 0))
        ttk.Label(footer, textvariable=self.status_var).pack(side=tk.LEFT)
        ttk.Label(
            footer,
            text=(
                "13F data are delayed disclosures; value changes are not investment "
                "returns or confirmed trades."
            ),
            foreground="#666666",
        ).pack(side=tk.RIGHT)

    def _build_overview_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Overview")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(3, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        self.overview_period_var = tk.StringVar()
        self.overview_period_box = self._combo(
            controls, "Reporting period", self.overview_period_var, 0, 16
        )
        ttk.Button(controls, text="Refresh", command=self.refresh_overview).grid(
            row=1, column=1, padx=(8, 0)
        )
        ttk.Button(
            controls,
            text="Load Top Securities",
            command=self.refresh_top_securities,
        ).grid(row=1, column=2, padx=(8, 0))

        metrics = ttk.Frame(tab)
        metrics.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for index in range(4):
            metrics.columnconfigure(index, weight=1)
        self.metric_vars = {
            "manager_count": tk.StringVar(value="—"),
            "disclosed_value": tk.StringVar(value="—"),
            "reported_entries": tk.StringVar(value="—"),
            "latest_filed_at": tk.StringVar(value="—"),
        }
        labels = (
            ("Managers", "manager_count"),
            ("Total disclosed value", "disclosed_value"),
            ("Reported line entries", "reported_entries"),
            ("Latest filing received", "latest_filed_at"),
        )
        for index, (label, key) in enumerate(labels):
            frame = ttk.LabelFrame(metrics, text=label, padding=8)
            frame.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 5, 0))
            ttk.Label(
                frame, textvariable=self.metric_vars[key], font=("Segoe UI", 13, "bold")
            ).pack()

        ttk.Label(
            tab,
            text=(
                "Latest stored filing per manager for the selected quarter. "
                "Amendments are represented by the most recently filed accession."
            ),
            foreground="#666666",
        ).grid(row=2, column=0, sticky="w", pady=(0, 6))

        panes = ttk.Panedwindow(tab, orient=tk.HORIZONTAL)
        panes.grid(row=3, column=0, sticky="nsew")
        managers_frame = ttk.LabelFrame(panes, text="Managers", padding=4)
        securities_frame = ttk.LabelFrame(panes, text="Top disclosed securities", padding=4)
        panes.add(managers_frame, weight=3)
        panes.add(securities_frame, weight=2)
        for frame in (managers_frame, securities_frame):
            frame.columnconfigure(0, weight=1)
            frame.rowconfigure(0, weight=1)

        self.overview_managers_table = DataTable(
            managers_frame,
            (
                Column("company_name", "Manager", 260),
                Column("cik", "CIK", 100),
                Column("table_value_total", "Portfolio value", 135, tk.E, format_money),
                Column("table_entry_total", "Entries", 80, tk.E, format_integer),
                Column("form_type", "Form", 80),
                Column("filed_at", "Filed", 130, tk.W, format_datetime),
            ),
        )
        self.overview_managers_table.grid(row=0, column=0, sticky="nsew")
        self.overview_managers_table.tree.bind(
            "<Double-1>", self._open_selected_manager
        )
        self.overview_securities_table = DataTable(
            securities_frame,
            (
                Column("security", "Ticker / CUSIP", 110),
                Column("issuer", "Issuer", 230),
                Column("disclosed_value", "Disclosed value", 135, tk.E, format_money),
                Column("manager_count", "Managers", 85, tk.E, format_integer),
            ),
        )
        self.overview_securities_table.grid(row=0, column=0, sticky="nsew")
        self.overview_securities_table.tree.bind(
            "<Double-1>", self._open_selected_security
        )

    def _build_holdings_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Manager Holdings")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.holdings_manager_var = tk.StringVar()
        self.holdings_manager_box = self._combo(
            controls, "Manager (type partial name or CIK)", self.holdings_manager_var, 0, 42
        )
        self._make_manager_combo_searchable(
            self.holdings_manager_box, self.holdings_manager_var, include_all=False
        )
        self.holdings_period_var = tk.StringVar()
        self.holdings_period_box = self._combo(
            controls, "Reporting period", self.holdings_period_var, 1, 16
        )
        self.holdings_search_var = tk.StringVar()
        ttk.Label(controls, text="Ticker, CUSIP, or issuer").grid(
            row=0, column=2, sticky="w", padx=(8, 0)
        )
        search_entry = ttk.Entry(controls, textvariable=self.holdings_search_var, width=24)
        search_entry.grid(row=1, column=2, sticky="ew", padx=(8, 0))
        search_entry.bind("<Return>", lambda _event: self.refresh_holdings())
        self.holdings_sector_var = tk.StringVar(value=ALL_CLASSIFICATIONS)
        self.holdings_sector_box = self._combo(
            controls, "Sector", self.holdings_sector_var, 3, 20
        )
        self.holdings_industry_var = tk.StringVar(value=ALL_CLASSIFICATIONS)
        self.holdings_industry_box = self._combo(
            controls, "Industry", self.holdings_industry_var, 4, 26
        )
        self.holdings_limit_var = tk.StringVar(value=str(DEFAULT_LIMIT))
        self._entry(controls, "Max rows", self.holdings_limit_var, 5, 9)
        ttk.Button(controls, text="Load holdings", command=self.refresh_holdings).grid(
            row=1, column=6, padx=(8, 0)
        )
        controls.columnconfigure(0, weight=1)

        self.holdings_note_var = tk.StringVar(
            value="Choose a manager and reporting period."
        )
        ttk.Label(tab, textvariable=self.holdings_note_var, foreground="#666666").grid(
            row=1, column=0, sticky="w", pady=(0, 6)
        )
        self.holdings_table = DataTable(
            tab,
            (
                Column("ticker", "Ticker", 80),
                Column("name_of_issuer", "Issuer", 250),
                Column("title_of_class", "Class", 100),
                Column("cusip", "CUSIP", 95),
                Column("put_call", "Put/Call", 70),
                Column("sector", "Sector", 145),
                Column("industry", "Industry", 190),
                Column("disclosed_value", "Value", 125, tk.E, format_money),
                Column("portfolio_weight", "Weight", 90, tk.E, format_percent),
                Column("shares", "Shares / principal", 125, tk.E, format_integer),
                Column("share_type", "Amount type", 90),
                Column("voting_sole", "Vote sole", 105, tk.E, format_integer),
                Column("voting_shared", "Vote shared", 105, tk.E, format_integer),
                Column("voting_none", "Vote none", 105, tk.E, format_integer),
            ),
        )
        self.holdings_table.grid(row=2, column=0, sticky="nsew")

    def _build_changes_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Quarterly Changes")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.changes_manager_var = tk.StringVar(value=ALL_MANAGERS)
        self.changes_manager_box = self._combo(
            controls,
            "Manager scope (type partial name or CIK)",
            self.changes_manager_var,
            0,
            38,
        )
        self._make_manager_combo_searchable(
            self.changes_manager_box, self.changes_manager_var, include_all=True
        )
        self.current_period_var = tk.StringVar()
        self.current_period_box = self._combo(
            controls, "Current period", self.current_period_var, 1, 15
        )
        self.prior_period_var = tk.StringVar()
        self.prior_period_box = self._combo(
            controls, "Prior period", self.prior_period_var, 2, 15
        )
        self.action_var = tk.StringVar(value="All")
        self.action_box = self._combo(
            controls, "Action", self.action_var, 3, 12
        )
        self.action_box.configure(values=("All", "New", "Bought", "Sold", "Exited"))
        self.changes_limit_var = tk.StringVar(value=str(DEFAULT_LIMIT))
        self._entry(controls, "Max rows", self.changes_limit_var, 4, 9)
        ttk.Button(controls, text="Compare", command=self.refresh_changes).grid(
            row=1, column=5, padx=(8, 0)
        )
        controls.columnconfigure(0, weight=1)

        ttk.Label(
            tab,
            text=(
                "Compares reported position values between quarter-end snapshots. "
                "Price movement, manager coverage changes, options, and amendments "
                "can affect the result; labels do not prove a trade occurred."
            ),
            foreground="#666666",
            wraplength=1200,
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.changes_table = DataTable(
            tab,
            (
                Column("action", "Classification", 95),
                Column("ticker", "Ticker", 80),
                Column("name_of_issuer", "Issuer", 250),
                Column("cusip", "CUSIP", 95),
                Column("put_call", "Put/Call", 70),
                Column("current_value", "Current value", 125, tk.E, format_money),
                Column("prior_value", "Prior value", 125, tk.E, format_money),
                Column("value_change", "Value change", 125, tk.E, format_money),
                Column("current_shares", "Current shares", 120, tk.E, format_integer),
                Column("prior_shares", "Prior shares", 120, tk.E, format_integer),
                Column("shares_change", "Share change", 120, tk.E, format_integer),
            ),
        )
        self.changes_table.grid(row=2, column=0, sticky="nsew")

    def _build_security_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Security Ownership")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.security_query_var = tk.StringVar()
        ttk.Label(controls, text="Ticker, CUSIP, or issuer").grid(row=0, column=0, sticky="w")
        security_entry = ttk.Entry(
            controls, textvariable=self.security_query_var, width=30
        )
        security_entry.grid(row=1, column=0, sticky="ew")
        security_entry.bind("<Return>", lambda _event: self.refresh_security())
        self.security_period_var = tk.StringVar()
        self.security_period_box = self._combo(
            controls, "Reporting period", self.security_period_var, 1, 16
        )
        self.security_limit_var = tk.StringVar(value=str(DEFAULT_LIMIT))
        self._entry(controls, "Max rows", self.security_limit_var, 2, 9)
        ttk.Button(controls, text="Find owners", command=self.refresh_security).grid(
            row=1, column=3, padx=(8, 0)
        )
        controls.columnconfigure(0, weight=1)

        ttk.Label(
            tab,
            text=(
                "Shows reporting managers in the imported universe, not total "
                "institutional ownership. Put/call positions are displayed separately."
            ),
            foreground="#666666",
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.security_table = DataTable(
            tab,
            (
                Column("company_name", "Manager", 260),
                Column("cik", "CIK", 95),
                Column("ticker", "Ticker", 75),
                Column("name_of_issuer", "Issuer", 220),
                Column("cusip", "CUSIP", 95),
                Column("title_of_class", "Class", 95),
                Column("put_call", "Put/Call", 70),
                Column("disclosed_value", "Value", 125, tk.E, format_money),
                Column("shares", "Shares / principal", 125, tk.E, format_integer),
                Column("filed_at", "Filed", 130, tk.W, format_datetime),
            ),
        )
        self.security_table.grid(row=2, column=0, sticky="nsew")

    def _build_classification_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Sector & Industry")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)

        views = ttk.Notebook(tab)
        views.grid(row=0, column=0, sticky="nsew")
        self.classification_views = views

        leaders = ttk.Frame(views, padding=8)
        views.add(leaders, text="Top Filers")
        leaders.columnconfigure(0, weight=1)
        leaders.rowconfigure(2, weight=1)
        controls = ttk.Frame(leaders)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        self.class_level_var = tk.StringVar(value="Sector")
        self.class_level_box = self._combo(
            controls, "Classification", self.class_level_var, 0, 12
        )
        self.class_level_box.configure(values=("Sector", "Industry"))
        self.class_level_box.bind(
            "<<ComboboxSelected>>", lambda _event: self._classification_level_changed()
        )
        self.class_category_var = tk.StringVar()
        self.class_category_box = self._combo(
            controls, "Sector or industry", self.class_category_var, 1, 30
        )
        self.class_category_box.configure(state="normal")
        self.class_category_box.bind(
            "<Return>", lambda _event: self.refresh_classification_leaders()
        )
        self.class_period_var = tk.StringVar()
        self.class_period_box = self._combo(
            controls, "Reporting period", self.class_period_var, 2, 15
        )
        self.class_scope_var = tk.StringVar(value=NON_OPTION_POSITIONS)
        self.class_scope_box = self._combo(
            controls, "Position scope", self.class_scope_var, 3, 21
        )
        self.class_scope_box.configure(values=POSITION_SCOPES)
        self.class_ranking_var = tk.StringVar(value="Disclosed value")
        self.class_ranking_box = self._combo(
            controls, "Rank by", self.class_ranking_var, 4, 22
        )
        self.class_ranking_box.configure(
            values=("Disclosed value", "Portfolio concentration")
        )
        self.class_limit_var = tk.StringVar(value="100")
        self._entry(controls, "Max rows", self.class_limit_var, 5, 8)
        ttk.Button(
            controls, text="Find top filers", command=self.refresh_classification_leaders
        ).grid(row=1, column=6, padx=(8, 0))
        controls.columnconfigure(1, weight=1)

        ttk.Label(
            leaders,
            text=(
                "Ranks the latest stored filing per manager. Portfolio concentration "
                "uses the manager's full reported portfolio as the denominator."
            ),
            foreground="#666666",
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.class_leaders_table = DataTable(
            leaders,
            (
                Column("company_name", "Manager", 260),
                Column("cik", "CIK", 95),
                Column("category_value", "Category value", 130, tk.E, format_money),
                Column("portfolio_weight", "Portfolio weight", 105, tk.E, format_percent),
                Column("portfolio_value", "Portfolio value", 130, tk.E, format_money),
                Column("position_count", "Positions", 80, tk.E, format_integer),
                Column("largest_security", "Largest ticker / CUSIP", 135),
                Column("largest_issuer", "Largest issuer", 220),
                Column("largest_value", "Largest value", 120, tk.E, format_money),
            ),
        )
        self.class_leaders_table.grid(row=2, column=0, sticky="nsew")
        self.class_leaders_table.tree.bind(
            "<Double-1>", self._open_classification_manager
        )

        allocation = ttk.Frame(views, padding=8)
        views.add(allocation, text="Manager Allocation")
        allocation.columnconfigure(0, weight=1)
        allocation.rowconfigure(2, weight=1)
        controls = ttk.Frame(allocation)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        self.allocation_manager_var = tk.StringVar()
        self.allocation_manager_box = self._combo(
            controls,
            "Manager (type partial name or CIK)",
            self.allocation_manager_var,
            0,
            42,
        )
        self._make_manager_combo_searchable(
            self.allocation_manager_box,
            self.allocation_manager_var,
            include_all=False,
        )
        self.allocation_period_var = tk.StringVar()
        self.allocation_period_box = self._combo(
            controls, "Reporting period", self.allocation_period_var, 1, 15
        )
        self.allocation_level_var = tk.StringVar(value="Sector")
        self.allocation_level_box = self._combo(
            controls, "Group by", self.allocation_level_var, 2, 12
        )
        self.allocation_level_box.configure(values=("Sector", "Industry"))
        self.allocation_scope_var = tk.StringVar(value=NON_OPTION_POSITIONS)
        self.allocation_scope_box = self._combo(
            controls, "Position scope", self.allocation_scope_var, 3, 21
        )
        self.allocation_scope_box.configure(values=POSITION_SCOPES)
        ttk.Button(
            controls, text="Load allocation", command=self.refresh_manager_allocation
        ).grid(row=1, column=4, padx=(8, 0))
        controls.columnconfigure(0, weight=1)

        self.allocation_note_var = tk.StringVar(
            value="Choose a manager and reporting period."
        )
        ttk.Label(
            allocation,
            textvariable=self.allocation_note_var,
            foreground="#666666",
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.allocation_table = DataTable(
            allocation,
            (
                Column("category", "Sector / Industry", 240),
                Column("disclosed_value", "Disclosed value", 135, tk.E, format_money),
                Column("portfolio_weight", "Portfolio weight", 105, tk.E, format_percent),
                Column("position_count", "Positions", 80, tk.E, format_integer),
                Column("largest_security", "Largest ticker / CUSIP", 135),
                Column("largest_issuer", "Largest issuer", 260),
                Column("largest_value", "Largest value", 125, tk.E, format_money),
            ),
        )
        self.allocation_table.grid(row=2, column=0, sticky="nsew")
        self.allocation_table.tree.bind(
            "<Double-1>", self._open_classification_holdings
        )

    def _build_filings_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Filing Browser")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.filings_period_var = tk.StringVar(value="All periods")
        self.filings_period_box = self._combo(
            controls, "Reporting period", self.filings_period_var, 0, 16
        )
        self.filings_manager_var = tk.StringVar(value=ALL_MANAGERS)
        self.filings_manager_box = self._combo(
            controls,
            "Manager (type partial name or CIK)",
            self.filings_manager_var,
            1,
            42,
        )
        self._make_manager_combo_searchable(
            self.filings_manager_box, self.filings_manager_var, include_all=True
        )
        self.filings_limit_var = tk.StringVar(value=str(DEFAULT_LIMIT))
        self._entry(controls, "Max rows", self.filings_limit_var, 2, 9)
        ttk.Button(controls, text="Load filings", command=self.refresh_filings).grid(
            row=1, column=3, padx=(8, 0)
        )
        controls.columnconfigure(1, weight=1)

        ttk.Label(
            tab,
            text="Double-click a filing to open its SEC EDGAR filing page.",
            foreground="#666666",
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.filings_table = DataTable(
            tab,
            (
                Column("filed_at", "Filed", 140, tk.W, format_datetime),
                Column("period_of_report", "Period", 100, tk.W, format_date),
                Column("company_name", "Manager", 260),
                Column("cik", "CIK", 95),
                Column("form_type", "Form", 85),
                Column("report_type", "Report type", 130),
                Column("table_value_total", "Portfolio value", 130, tk.E, format_money),
                Column("table_entry_total", "Entries", 80, tk.E, format_integer),
                Column("other_included_managers_count", "Other managers", 100, tk.E, format_integer),
                Column("accession_no", "Accession", 165),
            ),
        )
        self.filings_table.grid(row=2, column=0, sticky="nsew")
        self.filings_table.tree.bind("<Double-1>", self.open_selected_filing)

    @staticmethod
    def _combo(
        parent: tk.Misc,
        label: str,
        variable: tk.StringVar,
        column: int,
        width: int,
    ) -> ttk.Combobox:
        ttk.Label(parent, text=label).grid(
            row=0, column=column, sticky="w", padx=(8 if column else 0, 0)
        )
        combo = ttk.Combobox(
            parent, textvariable=variable, state="readonly", width=width
        )
        combo.grid(
            row=1, column=column, sticky="ew", padx=(8 if column else 0, 0)
        )
        return combo

    def _make_manager_combo_searchable(
        self,
        combo: ttk.Combobox,
        variable: tk.StringVar,
        *,
        include_all: bool,
    ) -> None:
        combo.configure(state="normal")
        combo.bind(
            "<FocusIn>",
            lambda _event: combo.after_idle(lambda: combo.selection_range(0, tk.END)),
        )
        combo.bind(
            "<KeyRelease>",
            lambda event: self._filter_manager_combo(
                event, combo, variable, include_all=include_all
            ),
        )
        combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: combo.configure(
                values=self._manager_labels(include_all=include_all)
            ),
        )

    def _manager_labels(self, *, include_all: bool) -> tuple[str, ...]:
        labels = tuple(option[0] for option in self.manager_options)
        return (ALL_MANAGERS, *labels) if include_all else labels

    def _filter_manager_combo(
        self,
        event: tk.Event[Any],
        combo: ttk.Combobox,
        variable: tk.StringVar,
        *,
        include_all: bool,
    ) -> None:
        if event.keysym in {
            "Down",
            "Up",
            "Left",
            "Right",
            "Home",
            "End",
            "Prior",
            "Next",
            "Tab",
            "Shift_L",
            "Shift_R",
        }:
            return

        query = variable.get().strip()
        if query in self.manager_by_label:
            matches = self._manager_labels(include_all=include_all)
        elif not query:
            matches = self._manager_labels(include_all=include_all)
        else:
            ranked = sorted(
                (
                    (score, label)
                    for label, cik, name in self.manager_options
                    if (score := manager_search_score(name, cik, query)) is not None
                ),
                key=lambda item: item[0],
            )
            matches = tuple(label for _score, label in ranked)

        combo.configure(values=matches)
        variable.set(query)
        combo.icursor(tk.END)
        if event.keysym == "Return":
            if len(matches) == 1:
                variable.set(matches[0])
                combo.configure(values=self._manager_labels(include_all=include_all))
            elif matches:
                combo.event_generate("<Down>")
        if query and query not in self.manager_by_label:
            self.status_var.set(
                f"Found {len(matches):,} manager name/CIK matches; select one from the list"
            )

    @staticmethod
    def _entry(
        parent: tk.Misc,
        label: str,
        variable: tk.StringVar,
        column: int,
        width: int,
    ) -> ttk.Entry:
        ttk.Label(parent, text=label).grid(
            row=0, column=column, sticky="w", padx=(8 if column else 0, 0)
        )
        entry = ttk.Entry(parent, textvariable=variable, width=width)
        entry.grid(
            row=1, column=column, sticky="ew", padx=(8 if column else 0, 0)
        )
        return entry

    def _submit(
        self,
        function: Callable[[], Any],
        callback: Callable[[Any], None],
        description: str,
    ) -> None:
        if self.closing:
            return
        self.status_var.set(f"{description}…")
        self.pending.append((self.executor.submit(function), callback, description))
        if self.poll_after_id is None:
            self.poll_after_id = self.root.after(POLL_INTERVAL_MS, self._poll_tasks)

    def _poll_tasks(self) -> None:
        self.poll_after_id = None
        if self.closing:
            return
        remaining: list[tuple[Future[Any], Callable[[Any], None], str]] = []
        for future, callback, description in self.pending:
            if not future.done():
                remaining.append((future, callback, description))
                continue
            try:
                result = future.result()
                callback(result)
            except Exception as exc:
                self.status_var.set(f"{description} failed")
                messagebox.showerror(f"{description} Failed", str(exc))
        self.pending = remaining
        if self.pending:
            self.poll_after_id = self.root.after(POLL_INTERVAL_MS, self._poll_tasks)

    def _catalog_loaded(self, catalog: Catalog) -> None:
        self.catalog = catalog
        period_values = tuple(format_date(period) for period in catalog.periods)
        if not period_values:
            raise RuntimeError(
                "No reporting periods were found. Run form13f_recent_import.py first."
            )
        latest = period_values[0]
        prior = period_values[1] if len(period_values) > 1 else latest
        for combo in (
            self.overview_period_box,
            self.holdings_period_box,
            self.current_period_box,
            self.prior_period_box,
            self.security_period_box,
            self.class_period_box,
            self.allocation_period_box,
        ):
            combo.configure(values=period_values)
        self.filings_period_box.configure(values=("All periods", *period_values))
        self.overview_period_var.set(latest)
        self.holdings_period_var.set(latest)
        self.current_period_var.set(latest)
        self.prior_period_var.set(prior)
        self.security_period_var.set(latest)
        self.class_period_var.set(latest)
        self.allocation_period_var.set(latest)

        sector_values = (*catalog.sectors, UNCLASSIFIED)
        industry_values = (*catalog.industries, UNCLASSIFIED)
        self.holdings_sector_box.configure(
            values=(ALL_CLASSIFICATIONS, *sector_values)
        )
        self.holdings_industry_box.configure(
            values=(ALL_CLASSIFICATIONS, *industry_values)
        )
        self.class_category_box.configure(values=sector_values)
        self.class_category_var.set(sector_values[0] if sector_values else UNCLASSIFIED)

        labels = [ALL_MANAGERS]
        for cik, name in catalog.managers:
            label = f"{name}  [CIK {cik}]"
            labels.append(label)
            self.manager_by_label[label] = cik
            self.label_by_cik[cik] = label
            self.manager_options.append((label, cik, name))
        self.holdings_manager_box.configure(
            values=self._manager_labels(include_all=False)
        )
        self.changes_manager_box.configure(values=self._manager_labels(include_all=True))
        self.allocation_manager_box.configure(
            values=self._manager_labels(include_all=False)
        )
        self.filings_manager_box.configure(values=self._manager_labels(include_all=True))
        if len(labels) > 1:
            self.holdings_manager_var.set(labels[1])
            self.changes_manager_var.set(labels[1])
            self.allocation_manager_var.set(labels[1])
        self.status_var.set(
            f"Connected | {catalog.cover_count:,} filings | "
            f"approximately {catalog.holding_count:,} holding rows"
        )
        self.refresh_overview()

    @staticmethod
    def _period(value: str) -> dt.date:
        return dt.date.fromisoformat(value)

    def refresh_overview(self) -> None:
        try:
            period = self._period(self.overview_period_var.get())
        except ValueError:
            return
        self._submit(
            lambda: self.repository.overview(period),
            self._overview_loaded,
            "Load quarter overview",
        )

    def _overview_loaded(self, overview: Overview) -> None:
        metrics = overview.metrics
        self.metric_vars["manager_count"].set(format_integer(metrics["manager_count"]))
        self.metric_vars["disclosed_value"].set(format_money(metrics["disclosed_value"]))
        self.metric_vars["reported_entries"].set(format_integer(metrics["reported_entries"]))
        self.metric_vars["latest_filed_at"].set(format_datetime(metrics["latest_filed_at"]))
        self.overview_managers_table.set_rows(overview.managers)
        self.overview_securities_table.set_rows(overview.securities)
        self.status_var.set(
            f"Overview {overview.period:%Y-%m-%d}: "
            f"{len(overview.managers):,} managers | "
            "click Load Top Securities for the universe aggregation"
        )

    def refresh_top_securities(self) -> None:
        try:
            period = self._period(self.overview_period_var.get())
        except ValueError:
            return
        self._submit(
            lambda: self.repository.top_securities(period),
            self._top_securities_loaded,
            "Aggregate top disclosed securities",
        )

    def _top_securities_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.overview_securities_table.set_rows(rows)
        self.status_var.set(f"Loaded {len(rows):,} top disclosed securities")

    def refresh_holdings(self) -> None:
        manager_cik = self.manager_by_label.get(self.holdings_manager_var.get())
        if not manager_cik:
            messagebox.showwarning(
                "Manager Required",
                "Type part of a filer name or CIK, then select a matching manager.",
            )
            return
        try:
            period = self._period(self.holdings_period_var.get())
        except ValueError:
            return
        search = self.holdings_search_var.get().strip()
        sector_text = self.holdings_sector_var.get()
        industry_text = self.holdings_industry_var.get()
        sector = None if sector_text == ALL_CLASSIFICATIONS else sector_text
        industry = None if industry_text == ALL_CLASSIFICATIONS else industry_text
        limit = parse_positive_int(self.holdings_limit_var.get())
        self._submit(
            lambda: self.repository.manager_holdings(
                manager_cik,
                period,
                search=search,
                sector=sector,
                industry=industry,
                limit=limit,
            ),
            self._holdings_loaded,
            "Load manager holdings",
        )

    def _holdings_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.holdings_table.set_rows(rows)
        total = sum(float(row.get("disclosed_value") or 0) for row in rows)
        self.holdings_note_var.set(
            f"{len(rows):,} positions shown | displayed value {format_money(total)}"
        )
        self.status_var.set(f"Loaded {len(rows):,} manager positions")

    def _classification_values(self, level: str) -> tuple[str, ...]:
        if self.catalog is None:
            return (UNCLASSIFIED,)
        values = self.catalog.sectors if level == "Sector" else self.catalog.industries
        return (*values, UNCLASSIFIED)

    def _classification_level_changed(self) -> None:
        values = self._classification_values(self.class_level_var.get())
        self.class_category_box.configure(values=values)
        self.class_category_var.set(values[0] if values else UNCLASSIFIED)

    def _resolve_classification(self, level: str, query: str) -> str | None:
        values = self._classification_values(level)
        normalized = query.strip().casefold()
        exact = next((value for value in values if value.casefold() == normalized), None)
        if exact:
            return exact
        matches = [value for value in values if normalized in value.casefold()]
        if len(matches) == 1:
            return matches[0]
        messagebox.showwarning(
            "Select a Classification",
            (
                "Enter or select a sector or industry from the list."
                if not matches
                else f"More than one classification matches '{query}'. Select one from the list."
            ),
        )
        return None

    def refresh_classification_leaders(self) -> None:
        try:
            period = self._period(self.class_period_var.get())
        except ValueError:
            return
        level = self.class_level_var.get()
        category = self._resolve_classification(level, self.class_category_var.get())
        if category is None:
            return
        self.class_category_var.set(category)
        scope = self.class_scope_var.get()
        ranking = self.class_ranking_var.get()
        limit = parse_positive_int(self.class_limit_var.get(), default=100)
        self._submit(
            lambda: self.repository.classification_leaders(
                period, level, category, scope, ranking, limit
            ),
            self._classification_leaders_loaded,
            f"Find top filers in {category}",
        )

    def _classification_leaders_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.class_leaders_table.set_rows(rows)
        self.status_var.set(
            f"Found {len(rows):,} filers for {self.class_category_var.get()}"
        )

    def refresh_manager_allocation(self) -> None:
        manager_cik = self.manager_by_label.get(self.allocation_manager_var.get())
        if not manager_cik:
            messagebox.showwarning(
                "Manager Required",
                "Type part of a filer name or CIK, then select a matching manager.",
            )
            return
        try:
            period = self._period(self.allocation_period_var.get())
        except ValueError:
            return
        level = self.allocation_level_var.get()
        scope = self.allocation_scope_var.get()
        self._submit(
            lambda: self.repository.manager_classification(
                manager_cik, period, level, scope
            ),
            self._manager_allocation_loaded,
            "Load manager sector and industry allocation",
        )

    def _manager_allocation_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.allocation_table.set_rows(rows)
        scope_value = float(rows[0].get("scope_value") or 0) if rows else 0
        portfolio_value = float(rows[0].get("portfolio_value") or 0) if rows else 0
        classified_value = sum(
            float(row.get("disclosed_value") or 0)
            for row in rows
            if row.get("category") != UNCLASSIFIED
        )
        coverage = classified_value / scope_value * 100 if scope_value else 0
        self.allocation_note_var.set(
            f"{len(rows):,} categories | classified coverage {coverage:.2f}% of "
            f"displayed scope | scope value {format_money(scope_value)} | "
            f"full portfolio {format_money(portfolio_value)}"
        )
        self.status_var.set(f"Loaded {len(rows):,} allocation categories")

    def refresh_changes(self) -> None:
        try:
            current = self._period(self.current_period_var.get())
            prior = self._period(self.prior_period_var.get())
        except ValueError:
            return
        if current == prior:
            messagebox.showwarning(
                "Choose Two Periods", "Current and prior periods must differ."
            )
            return
        manager_text = self.changes_manager_var.get()
        if manager_text not in self.manager_by_label:
            messagebox.showwarning(
                "Select a Manager",
                "Select a matching manager from the list, or choose All imported managers.",
            )
            return
        manager_cik = self.manager_by_label.get(manager_text)
        action = self.action_var.get()
        limit = parse_positive_int(self.changes_limit_var.get())
        self._submit(
            lambda: self.repository.position_changes(
                current, prior, manager_cik, action, limit
            ),
            self._changes_loaded,
            "Compare quarterly positions",
        )

    def _changes_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.changes_table.set_rows(rows)
        self.status_var.set(f"Loaded {len(rows):,} disclosed position changes")

    def refresh_security(self) -> None:
        query_text = self.security_query_var.get().strip()
        if not query_text:
            messagebox.showwarning(
                "Security Required", "Enter a ticker, CUSIP, or issuer name."
            )
            return
        try:
            period = self._period(self.security_period_var.get())
        except ValueError:
            return
        limit = parse_positive_int(self.security_limit_var.get())
        self._submit(
            lambda: self.repository.security_owners(query_text, period, limit),
            self._security_loaded,
            "Find security owners",
        )

    def _security_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.security_table.set_rows(rows)
        self.status_var.set(f"Found {len(rows):,} reporting managers")

    def refresh_filings(self) -> None:
        period_text = self.filings_period_var.get()
        period = None if period_text == "All periods" else self._period(period_text)
        manager_text = self.filings_manager_var.get()
        if manager_text not in self.manager_by_label:
            messagebox.showwarning(
                "Select a Manager",
                "Select a matching manager from the list, or choose All imported managers.",
            )
            return
        manager_cik = self.manager_by_label.get(manager_text)
        limit = parse_positive_int(self.filings_limit_var.get())
        self._submit(
            lambda: self.repository.filings(period, manager_cik, limit),
            self._filings_loaded,
            "Load filing browser",
        )

    def _filings_loaded(self, rows: list[dict[str, Any]]) -> None:
        self.filings_table.set_rows(rows)
        self.status_var.set(f"Loaded {len(rows):,} filings")

    def _open_classification_manager(self, _event: tk.Event[Any]) -> None:
        row = self.class_leaders_table.selected_row()
        if not row:
            return
        label = self.label_by_cik.get(str(row.get("cik") or ""))
        if not label:
            return
        self.allocation_manager_var.set(label)
        self.allocation_period_var.set(self.class_period_var.get())
        self.allocation_level_var.set(self.class_level_var.get())
        self.allocation_scope_var.set(self.class_scope_var.get())
        self.classification_views.select(1)
        self.refresh_manager_allocation()

    def _open_classification_holdings(self, _event: tk.Event[Any]) -> None:
        row = self.allocation_table.selected_row()
        category = str(row.get("category") or "") if row else ""
        if not category:
            return
        self.holdings_manager_var.set(self.allocation_manager_var.get())
        self.holdings_period_var.set(self.allocation_period_var.get())
        self.holdings_search_var.set("")
        if self.allocation_level_var.get() == "Sector":
            self.holdings_sector_var.set(category)
            self.holdings_industry_var.set(ALL_CLASSIFICATIONS)
        else:
            self.holdings_sector_var.set(ALL_CLASSIFICATIONS)
            self.holdings_industry_var.set(category)
        self.notebook.select(1)
        self.refresh_holdings()

    def _open_selected_manager(self, _event: tk.Event[Any]) -> None:
        row = self.overview_managers_table.selected_row()
        if not row:
            return
        label = self.label_by_cik.get(str(row["cik"]))
        if label:
            self.holdings_manager_var.set(label)
        self.holdings_period_var.set(self.overview_period_var.get())
        self.notebook.select(1)
        self.refresh_holdings()

    def _open_selected_security(self, _event: tk.Event[Any]) -> None:
        row = self.overview_securities_table.selected_row()
        if not row:
            return
        self.security_query_var.set(str(row.get("security") or row.get("cusip") or ""))
        self.security_period_var.set(self.overview_period_var.get())
        self.notebook.select(3)
        self.refresh_security()

    def open_selected_filing(self, _event: tk.Event[Any]) -> None:
        row = self.filings_table.selected_row()
        if not row:
            return
        accession = str(row.get("accession_no") or "")
        cik = str(row.get("cik") or "").lstrip("0")
        if not accession or not cik:
            return
        accession_path = accession.replace("-", "")
        url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/"
            f"{accession_path}/{accession}-index.html"
        )
        webbrowser.open(url)

    def current_table(self) -> DataTable | None:
        index = self.notebook.index(self.notebook.select())
        classification_table = (
            self.class_leaders_table
            if self.classification_views.index(self.classification_views.select()) == 0
            else self.allocation_table
        )
        return {
            0: self.overview_managers_table,
            1: self.holdings_table,
            2: self.changes_table,
            3: self.security_table,
            4: classification_table,
            5: self.filings_table,
        }.get(index)

    def export_current(self) -> None:
        table = self.current_table()
        if table is None or not table.rows:
            messagebox.showinfo("Nothing to Export", "Load a table before exporting.")
            return
        path = filedialog.asksaveasfilename(
            title="Export 13F analytics",
            defaultextension=".csv",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
        )
        if not path:
            return
        try:
            table.export_csv(path)
        except OSError as exc:
            messagebox.showerror("Export Failed", str(exc))
            return
        self.status_var.set(f"Exported {len(table.rows):,} rows to {path}")

    def close(self) -> None:
        self.closing = True
        if self.poll_after_id is not None:
            self.root.after_cancel(self.poll_after_id)
            self.poll_after_id = None
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.repository.engine.dispose()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    database = Form13FDatabase()
    app = Form13FDashboard(root, Form13FRepository(database.engine))
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
