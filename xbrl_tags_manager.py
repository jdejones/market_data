import json
import time
import warnings

from requests.exceptions import Timeout as RequestsTimeout
from sec_api import XbrlApi
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

try:
    from . import filings
    from .api_keys import gptdb, sec_api_key
except ImportError:
    import market_data.filings as filings  # type: ignore[import-not-found]
    from market_data.api_keys import gptdb, sec_api_key  # type: ignore[import-not-found]


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
GPTDB_MYSQL_USER = "gptdb"
XBRL_TAGS_DB = "xbrl_tags"
XBRL_SYMBOL_TAGS_DB = "xbrl_symbol_tags"
STATEMENT_COLUMNS = (
    "BalanceSheet",
    "StatementsOfIncome",
    "StatementsOfCashFlows",
)


def quote_identifier(identifier: str) -> str:
    """Return a MySQL-safe quoted identifier for schema, table, and column names."""
    return f"`{identifier.replace('`', '``')}`"


class SymbolTagsDict(dict):
    """
    Dictionary for symbol-specific XBRL tags with case-insensitive symbol keys.

    The `xbrl_symbol_tags` database stores one table per symbol using lowercase
    table names. This wrapper keeps the in-memory keys lowercase while allowing
    callers to use either `manager.xbrl_tags_saved["AAPL"]` or
    `manager.xbrl_tags_saved["aapl"]`.
    """

    def _normalize_key(self, key):
        """Normalize only string keys, leaving non-string dictionary keys unchanged."""
        return key.lower() if isinstance(key, str) else key

    def __contains__(self, key):
        """Return True for symbol keys regardless of uppercase/lowercase input."""
        return super().__contains__(self._normalize_key(key))

    def __getitem__(self, key):
        """Fetch a symbol payload using either uppercase or lowercase symbols."""
        return super().__getitem__(self._normalize_key(key))

    def __setitem__(self, key, value):
        """Store symbol payloads under lowercase keys to match database table names."""
        super().__setitem__(self._normalize_key(key), value)

    def get(self, key, default=None):
        """Return a symbol payload using case-insensitive lookup semantics."""
        return super().get(self._normalize_key(key), default)

    def setdefault(self, key, default=None):
        """Create or return a symbol payload using a normalized lowercase key."""
        return super().setdefault(self._normalize_key(key), default)


class Xbrl_Tags_Manager:
    """
    Manage shared and symbol-specific XBRL tag mappings for SEC filings.

    This class keeps the original working structure of the old code while moving
    storage into MySQL:

    - `xbrl_tags.statement_tags` maps normalized statement names to known XBRL
      statement tags.
    - `xbrl_tags.statement_item_links` maps normalized statement names to the
      normalized line items expected under each statement.
    - `xbrl_tags.item_tags` maps normalized line items to known XBRL line-item
      tags.
    - `xbrl_symbol_tags` stores symbol-specific tags, one lowercase symbol table
      per ticker, with statement payloads stored as JSON in statement columns.

    Basic usage:

    ```python
    manager = Xbrl_Tags_Manager(["AAPL", "MSFT"])
    manager.update_symbols(filing_type="10-K")
    ```

    For manual inspection before saving:

    ```python
    manager = Xbrl_Tags_Manager(["MRSH"])
    manager.update_all_tags("MRSH", "10-K")
    display(manager.xbrl_tags_saved["MRSH"])
    manager.save_tags()
    ```

    Symbols are stored lowercase for database compatibility, but lookups accept
    either uppercase or lowercase ticker strings.
    """

    def __init__(self, symbols: list):
        """
        Initialize SEC API clients and load XBRL tag mappings from MySQL.

        Parameters
        ----------
        symbols:
            Tickers to process. They may be supplied uppercase or lowercase.
            The manager stores them internally as lowercase database keys, while
            SEC filing queries are sent using uppercase tickers.
        """
        self.symbols = [self._normalize_symbol(symbol) for symbol in symbols]
        self.xbrlApi = XbrlApi(api_key=sec_api_key)
        self._xbrl_tags_engine = self._make_xbrl_tags_engine()
        self._xbrl_symbol_tags_engine = self._make_xbrl_symbol_tags_engine()
        self.statement_tags = self._load_xbrl_tag_table("statement_tags")
        self.statement_item_links = self._load_xbrl_tag_table("statement_item_links")
        self.item_tags = self._load_xbrl_tag_table("item_tags")
        self.xbrl_tags_saved = self._load_xbrl_symbol_tags()
        self.sym_statements_items_tags_saved_container = {}#Temporary container for locally saved tags.
        self.sym_statements_items_tags_current_container = {}#Temporary container for current SEC filed tags.
        self.symbols_tags_needed = {}

    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize a ticker for local storage and `xbrl_symbol_tags` table names."""
        return symbol.lower()

    def _filing_query_symbol(self, symbol: str) -> str:
        """Normalize a ticker for external SEC API queries."""
        return symbol.upper()

    def _make_xbrl_tags_engine(self):
        """Create a SQLAlchemy engine for the shared `xbrl_tags` schema."""
        url = URL.create(
            "mysql+pymysql",
            username=GPTDB_MYSQL_USER,
            password=gptdb,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=XBRL_TAGS_DB,
        )
        return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})

    def _make_xbrl_symbol_tags_engine(self):
        """Create a SQLAlchemy engine for the symbol-specific `xbrl_symbol_tags` schema."""
        url = URL.create(
            "mysql+pymysql",
            username=GPTDB_MYSQL_USER,
            password=gptdb,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=XBRL_SYMBOL_TAGS_DB,
        )
        return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})

    def _load_xbrl_tag_table(self, table_name: str) -> dict:
        """
        Load one shared XBRL mapping table from `xbrl_tags`.

        The current storage model is a one-row, wide table. Column names are the
        normalized terms used by the code, and each cell is a JSON-encoded list.
        The returned dictionary preserves the original in-code structure:

        ```python
        {
            "Revenue": ["Revenue", "Revenues", ...],
            "EPSBasic": ["EarningsPerShareBasic"],
        }
        ```

        Raises
        ------
        ValueError
            If the table does not contain exactly one row.
        """
        table = quote_identifier(table_name)

        with self._xbrl_tags_engine.connect() as conn:
            rows = conn.execute(text(f"SELECT * FROM {table}")).mappings().all()

        if len(rows) != 1:
            raise ValueError(
                f"Expected xbrl_tags.{table_name} to contain exactly 1 row; "
                f"found {len(rows)}."
            )

        return {
            column: self._loads_tag_list(table_name, column, value)
            for column, value in rows[0].items()
        }

    def _loads_tag_list(self, table_name: str, column: str, value) -> list:
        """
        Decode a JSON list from a shared XBRL mapping table cell.

        Null values are treated as empty lists. Non-list JSON values are rejected
        because the downstream code iterates over these values as lists of known
        tags.
        """
        if value is None:
            return []

        parsed_value = json.loads(value)

        if not isinstance(parsed_value, list):
            raise TypeError(
                f"Expected xbrl_tags.{table_name}.{column} to contain a JSON list; "
                f"found {type(parsed_value).__name__}."
            )

        return parsed_value

    def _load_xbrl_symbol_tags(self) -> dict:
        """
        Load all symbol-specific XBRL tag tables into memory.

        Each table in `xbrl_symbol_tags` is expected to be named by lowercase
        symbol and contain one row with statement columns such as
        `BalanceSheet`, `StatementsOfIncome`, and `StatementsOfCashFlows`.

        Returns
        -------
        SymbolTagsDict
            A dictionary shaped as:

            ```python
            {
                "aapl": {
                    "BalanceSheet": {"statement tags": [...], "Assets": [...]},
                    "StatementsOfIncome": {...},
                }
            }
            ```

            The wrapper allows case-insensitive symbol lookup while preserving
            lowercase storage keys.
        """
        with self._xbrl_symbol_tags_engine.connect() as conn:
            symbols = conn.execute(
                text(
                    "SELECT table_name "
                    "FROM information_schema.tables "
                    "WHERE table_schema = :schema"
                ),
                {"schema": XBRL_SYMBOL_TAGS_DB},
            ).scalars().all()

            return SymbolTagsDict({
                self._normalize_symbol(symbol): self._load_xbrl_symbol_table(conn, symbol)
                for symbol in symbols
            })

    def _load_xbrl_symbol_table(self, conn, symbol: str) -> dict:
        """
        Load one symbol table from `xbrl_symbol_tags`.

        Parameters
        ----------
        conn:
            Active SQLAlchemy connection.
        symbol:
            Table name for the symbol. Database tables are expected to be
            lowercase, but the method quotes the identifier before querying.

        Returns
        -------
        dict
            Statement-level payload for the symbol, with each non-null statement
            column JSON-decoded back into the original nested dictionary shape.
        """
        rows = conn.execute(
            text(f"SELECT * FROM {quote_identifier(symbol)}")
        ).mappings().all()

        if len(rows) != 1:
            raise ValueError(
                f"Expected xbrl_symbol_tags.{symbol} to contain exactly 1 row; "
                f"found {len(rows)}."
            )

        return {
            statement: json.loads(value)
            for statement, value in rows[0].items()
            if value is not None
        }

    def _save_xbrl_symbol_tags(self):
        """
        Persist all in-memory symbol-specific tags to `xbrl_symbol_tags`.

        This is called by `save_tags()` when no file path is provided. It creates
        missing lowercase symbol tables and updates existing one-row tables.
        """
        with self._xbrl_symbol_tags_engine.begin() as conn:
            for symbol, symbol_tags in self.xbrl_tags_saved.items():
                self._save_xbrl_symbol_table(conn, self._normalize_symbol(symbol), symbol_tags)

    def _save_xbrl_symbol_table(self, conn, symbol: str, symbol_tags: dict):
        """
        Insert or update one symbol's XBRL tag payload in `xbrl_symbol_tags`.

        The table name is the lowercase ticker. The table has one row and the
        statement columns defined by `STATEMENT_COLUMNS`. Each statement payload
        is JSON-encoded so the rest of the code can continue to work with the
        original nested dictionary structure after loading.
        """
        symbol = self._normalize_symbol(symbol)
        table = quote_identifier(symbol)
        columns_sql = ", ".join(
            f"{quote_identifier(column)} MEDIUMTEXT NULL"
            for column in STATEMENT_COLUMNS
        )

        conn.execute(
            text(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql})")
        )

        payload = {
            statement: (
                json.dumps(symbol_tags[statement], ensure_ascii=False)
                if statement in symbol_tags
                else None
            )
            for statement in STATEMENT_COLUMNS
        }

        row_count = conn.execute(
            text(f"SELECT COUNT(*) FROM {table}")
        ).scalar_one()

        if row_count == 0:
            columns = ", ".join(quote_identifier(column) for column in STATEMENT_COLUMNS)
            params = ", ".join(f":{column}" for column in STATEMENT_COLUMNS)
            conn.execute(
                text(f"INSERT INTO {table} ({columns}) VALUES ({params})"),
                payload,
            )
            return

        if row_count == 1:
            set_sql = ", ".join(
                f"{quote_identifier(column)} = :{column}"
                for column in STATEMENT_COLUMNS
            )
            conn.execute(text(f"UPDATE {table} SET {set_sql}"), payload)
            return

        raise ValueError(
            f"Expected xbrl_symbol_tags.{symbol} to contain 0 or 1 rows; "
            f"found {row_count}."
        )

    #Function for identifying if statements are saved and if tags are saved within the statement identifiers.
    def find_statements_tags_saved(self, symbol):
        """
        Load a symbol's previously saved XBRL tags into the saved-tag container.

        This does not call the SEC API. It only checks the in-memory
        `self.xbrl_tags_saved` dictionary that was loaded from
        `xbrl_symbol_tags` during initialization.

        Use this when you want to know whether a symbol already has curated or
        previously discovered tags:

        ```python
        manager.find_statements_tags_saved("AAPL")
        manager.sym_statements_items_tags_saved_container
        ```

        If the symbol is not present, `sym_statements_items_tags_saved_container`
        is set to `None`.
        """
        symbol = self._normalize_symbol(symbol)
        if symbol in self.xbrl_tags_saved.keys():#Checks for symbol in saved xbrl tags json file.
            self.sym_statements_items_tags_saved_container = self.xbrl_tags_saved[symbol]#Adds the symbols saved tags to a temporary container.
        else:
            self.sym_statements_items_tags_saved_container = None#If no tags are saved the temporary container gets assigned a value of None.

    def _symbols_for_tag_report(self, symbols=None) -> list:
        """
        Return normalized symbols included in a tag coverage report.

        By default, reports include both the symbols requested when the manager
        was created and every symbol loaded from `xbrl_symbol_tags`. This makes
        requested symbols with no database table visible as zero-tag symbols.
        """
        if symbols is None:
            symbols = set(self.symbols) | set(self.xbrl_tags_saved)
        return sorted({self._normalize_symbol(symbol) for symbol in symbols})

    def _flatten_symbol_tags(
        self,
        symbol: str,
        include_statement_tags: bool = True,
    ) -> list:
        """Return all stored XBRL tag strings for one symbol."""
        symbol = self._normalize_symbol(symbol)
        symbol_tags = self.xbrl_tags_saved.get(symbol, {})

        if not isinstance(symbol_tags, dict):
            raise TypeError(f"Expected saved tags for {symbol} to be a dictionary.")

        tags = []
        for statement, statement_payload in symbol_tags.items():
            if not isinstance(statement_payload, dict):
                raise TypeError(
                    f"Expected saved tags for {symbol}.{statement} to be a dictionary."
                )

            for item, item_tags in statement_payload.items():
                if item == "statement tags" and not include_statement_tags:
                    continue
                if not isinstance(item_tags, list):
                    raise TypeError(
                        f"Expected saved tags for {symbol}.{statement}.{item} "
                        "to be a list."
                    )
                tags.extend(tag for tag in item_tags if isinstance(tag, str) and tag)

        return tags

    def get_tags_by_symbol(
        self,
        symbols=None,
        include_statement_tags: bool = True,
        unique: bool = True,
    ) -> dict:
        """
        Return the XBRL tags available for each symbol.

        Symbols without any saved tags are included with an empty list. By
        default, duplicate strings are removed because repeated calls to
        `append_tags()` can save the same tag more than once.

        Parameters
        ----------
        symbols:
            Optional iterable of symbols to report. If omitted, include all
            loaded database symbols and all symbols supplied at initialization.
        include_statement_tags:
            Include statement-level tags as well as line-item tags.
        unique:
            Remove duplicate tag strings while preserving their stored order.
        """
        tags_by_symbol = {}
        for symbol in self._symbols_for_tag_report(symbols):
            tags = self._flatten_symbol_tags(symbol, include_statement_tags)
            if unique:
                tags = list(dict.fromkeys(tags))
            tags_by_symbol[symbol] = tags
        return tags_by_symbol

    def get_tag_counts_by_symbol(
        self,
        symbols=None,
        include_statement_tags: bool = True,
        unique: bool = True,
    ) -> dict:
        """Return each symbol's tag count, including zero-tag symbols."""
        return {
            symbol: len(tags)
            for symbol, tags in self.get_tags_by_symbol(
                symbols=symbols,
                include_statement_tags=include_statement_tags,
                unique=unique,
            ).items()
        }

    def get_symbols_with_tags(
        self,
        symbols=None,
        include_statement_tags: bool = True,
    ) -> list:
        """Return symbols that have at least one stored XBRL tag."""
        counts = self.get_tag_counts_by_symbol(
            symbols=symbols,
            include_statement_tags=include_statement_tags,
        )
        return [symbol for symbol, count in counts.items() if count > 0]

    def get_tag_coverage(
        self,
        symbols=None,
        include_statement_tags: bool = True,
    ) -> dict:
        """
        Return symbol-level counts, membership lists, and tag inventories.

        This reports whether symbols have any tags; it does not calculate a
        percentage against every possible XBRL concept.
        """
        tags_by_symbol = self.get_tags_by_symbol(
            symbols=symbols,
            include_statement_tags=include_statement_tags,
        )
        tag_counts_by_symbol = {
            symbol: len(tags) for symbol, tags in tags_by_symbol.items()
        }
        symbols_with_tags = [
            symbol for symbol, count in tag_counts_by_symbol.items() if count > 0
        ]
        symbols_without_tags = [
            symbol for symbol, count in tag_counts_by_symbol.items() if count == 0
        ]

        return {
            "symbol_count": len(tags_by_symbol),
            "symbols_with_tags_count": len(symbols_with_tags),
            "symbols_without_tags_count": len(symbols_without_tags),
            "symbols_with_tags": symbols_with_tags,
            "symbols_without_tags": symbols_without_tags,
            "tag_counts_by_symbol": tag_counts_by_symbol,
            "tags_by_symbol": tags_by_symbol,
        }

    def _item_tags_for_symbol(self, symbol: str) -> dict:
        """
        Return saved line-item tags keyed by normalized `item_tags` column name.

        If the same normalized item appears under more than one statement, its
        XBRL tags are combined and deduplicated.
        """
        symbol = self._normalize_symbol(symbol)
        symbol_tags = self.xbrl_tags_saved.get(symbol, {})

        if not isinstance(symbol_tags, dict):
            raise TypeError(f"Expected saved tags for {symbol} to be a dictionary.")

        tags_by_item = {}
        for statement, statement_payload in symbol_tags.items():
            if not isinstance(statement_payload, dict):
                raise TypeError(
                    f"Expected saved tags for {symbol}.{statement} to be a dictionary."
                )

            for item, saved_tags in statement_payload.items():
                if item == "statement tags":
                    continue
                if not isinstance(saved_tags, list):
                    raise TypeError(
                        f"Expected saved tags for {symbol}.{statement}.{item} "
                        "to be a list."
                    )

                valid_tags = [
                    tag for tag in saved_tags if isinstance(tag, str) and tag
                ]
                if valid_tags:
                    tags_by_item.setdefault(item, []).extend(valid_tags)

        return {
            item: list(dict.fromkeys(tags))
            for item, tags in tags_by_item.items()
        }

    def get_item_tag_coverage(self, symbols=None) -> dict:
        """
        Measure coverage for every normalized item in `xbrl_tags.item_tags`.

        The columns loaded into `self.item_tags` define the expected item-tag
        universe. A symbol covers an item when its saved payload contains at
        least one nonempty XBRL tag string for that normalized item.

        Returns
        -------
        dict
            `dataset` contains aggregate populated/missing counts.
            `by_symbol` shows each symbol's coverage, missing items, and actual
            XBRL tags.
            `by_item_tag` shows how many and which symbols cover each normalized
            item.
        """
        report_symbols = self._symbols_for_tag_report(symbols)
        expected_items = sorted(self.item_tags)
        expected_item_set = set(expected_items)
        expected_item_count = len(expected_items)

        saved_items_by_symbol = {
            symbol: self._item_tags_for_symbol(symbol)
            for symbol in report_symbols
        }

        by_symbol = {}
        for symbol, saved_items in saved_items_by_symbol.items():
            covered_items = [
                item for item in expected_items if saved_items.get(item)
            ]
            missing_items = [
                item for item in expected_items if not saved_items.get(item)
            ]
            unrecognized_items = sorted(set(saved_items) - expected_item_set)
            covered_count = len(covered_items)

            by_symbol[symbol] = {
                "expected_item_count": expected_item_count,
                "covered_item_count": covered_count,
                "missing_item_count": len(missing_items),
                "coverage_percent": (
                    round(covered_count / expected_item_count * 100, 2)
                    if expected_item_count
                    else 0.0
                ),
                "covered_items": covered_items,
                "missing_items": missing_items,
                "tags_by_item": {
                    item: saved_items[item] for item in covered_items
                },
                "unrecognized_saved_items": unrecognized_items,
            }

        by_item_tag = {}
        for item in expected_items:
            symbols_with_tag = [
                symbol
                for symbol in report_symbols
                if saved_items_by_symbol[symbol].get(item)
            ]
            symbols_without_tag = [
                symbol
                for symbol in report_symbols
                if not saved_items_by_symbol[symbol].get(item)
            ]
            by_item_tag[item] = {
                "symbols_with_tag_count": len(symbols_with_tag),
                "symbols_without_tag_count": len(symbols_without_tag),
                "coverage_percent": (
                    round(len(symbols_with_tag) / len(report_symbols) * 100, 2)
                    if report_symbols
                    else 0.0
                ),
                "symbols_with_tag": symbols_with_tag,
                "symbols_without_tag": symbols_without_tag,
                "tags_by_symbol": {
                    symbol: saved_items_by_symbol[symbol][item]
                    for symbol in symbols_with_tag
                },
            }

        possible_symbol_items = len(report_symbols) * expected_item_count
        populated_symbol_items = sum(
            symbol_report["covered_item_count"]
            for symbol_report in by_symbol.values()
        )

        return {
            "dataset": {
                "symbol_count": len(report_symbols),
                "expected_item_tag_count": expected_item_count,
                "possible_symbol_item_count": possible_symbol_items,
                "populated_symbol_item_count": populated_symbol_items,
                "missing_symbol_item_count": (
                    possible_symbol_items - populated_symbol_items
                ),
                "coverage_percent": (
                    round(
                        populated_symbol_items / possible_symbol_items * 100,
                        2,
                    )
                    if possible_symbol_items
                    else 0.0
                ),
            },
            "by_symbol": by_symbol,
            "by_item_tag": by_item_tag,
        }
    
    #Function for identifying current SEC filed tags for a symbol.
    def find_statements_tags_current(self, symbol, filing_type):
        """
        Discover XBRL tags from the latest SEC filing for one symbol.

        This method performs the live SEC work:

        1. Uses `filings.filing_urls()` to find the latest filing URL for the
           requested symbol/form type.
        2. Converts that filing to JSON through `sec-api.io`'s `XbrlApi`.
        3. Locates known statement tags using `self.statement_tags`.
        4. Locates known line-item tags using `self.statement_item_links` and
           `self.item_tags`.

        The returned value has the same nested structure used by
        `xbrl_symbol_tags`, for example:

        ```python
        {
            "BalanceSheet": {
                "statement tags": ["BalanceSheets"],
                "CurrentAssets": ["AssetsCurrent"],
            }
        }
        ```

        This method updates `sym_statements_items_tags_current_container` for
        compatibility with the original code, but it also returns the current
        result directly. It does not save to the database by itself.

        Empty filing results or filings without attached XBRL data return an
        empty dictionary and print a short diagnostic message.
        """
        current_container = {}
        for attempt in range(2):
            try:
                filing_urls = filings.filing_urls(self._filing_query_symbol(symbol), filing_type, no_filings=1)
                if not filing_urls:
                    print(f"filings.filing_urls returned 0 filings of type: {filing_type} for symbol: {symbol}.")
                    self.sym_statements_items_tags_current_container = current_container
                    return current_container
                filing = self.xbrlApi.xbrl_to_json(htm_url=filing_urls[0])#Brings the filing into the namespace.
                break
            except (RequestsTimeout, TimeoutError):
                if attempt == 1:
                    raise
                time.sleep(5)
            except Exception as e:
                if "The filer did not attach XBRL data" in str(e):
                    print(f"{symbol} {filing_type} filing has no attached XBRL data. A conversion is not possible.")
                    self.sym_statements_items_tags_current_container = current_container
                    return current_container
                raise
        for statement in self.statement_tags:#Iterates through statement identifiers.
            for tag in self.statement_tags[statement]:#Iterates through a list of known statment tags for the current statement identifier.
                if tag in filing.keys():#Checks for the presence of the statement tag in SEC filing.
                    current_container[statement] = {'statement tags': [tag]}
                    break
            if statement not in current_container:#Handles symbols whose statement tags I don't have an identifier for.
                self.symbols_tags_needed[statement] = {}
        for statement in current_container:
            statement_tag = current_container[statement]['statement tags'][0]
            filing_statement = filing.get(statement_tag, {})
            for item in self.statement_item_links.get(statement, []):
                for tag in self.item_tags.get(item, []):
                    if tag in filing_statement:
                        current_container[statement][item] = [tag]
        self.sym_statements_items_tags_current_container = current_container
        return current_container
    
    def update_all_tags(self, symbol, filing_type):
        """
        Replace a symbol's saved tags with tags found in the latest filing.

        This is intended for symbols that are new to `xbrl_symbol_tags` or when
        you intentionally want to rebuild the symbol's saved tag payload from a
        current filing.

        Example:

        ```python
        manager.update_all_tags("MRSH", "10-K")
        display(manager.xbrl_tags_saved["MRSH"])
        manager.save_tags()
        ```

        The update is in memory until `save_tags()` is called.
        """
        symbol = self._normalize_symbol(symbol)
        current_container = self.find_statements_tags_current(symbol, filing_type)
        self.xbrl_tags_saved[symbol] = current_container
        self.sym_statements_items_tags_current_container = {}
    
    def append_tags(self, symbol, filing_type):
        """
        Append newly discovered filing tags to an existing symbol payload.

        This preserves the original append behavior: if a statement or line item
        already exists for the symbol, the current tag is appended to that list.
        If a statement or item is missing, it is created.

        Use this when a symbol already exists in `xbrl_symbol_tags` and you want
        to add tags discovered from a newer filing:

        ```python
        manager.append_tags("AAPL", "10-K")
        manager.save_tags()
        ```

        Missing line items in the current filing are skipped. This prevents a
        filing that lacks one expected tag from breaking the entire update.
        """
        symbol = self._normalize_symbol(symbol)
        current_container = self.find_statements_tags_current(symbol, filing_type)
        if len(current_container) > 0:#Checks that tags were found.
            self.xbrl_tags_saved.setdefault(symbol, {})
            for statement in current_container:#Iterates statement current.
                if statement not in self.xbrl_tags_saved[symbol]:
                    self.xbrl_tags_saved[symbol][statement] = {'statement tags': []}
                if len(current_container[statement]['statement tags']) > 1:#Raise warning if more than one tag is found.
                    warnings.warn(f'{symbol} has more than 1 statement tag. Verify correct tag and manually add.')
                else:
                    if 'statement tags' in self.xbrl_tags_saved[symbol][statement]:#Check for list of statement tags.
                        self.xbrl_tags_saved[symbol][statement]['statement tags'].append(current_container[statement]['statement tags'][0])#Append first current statement tag to symbol tag structure.
                    else:
                        self.xbrl_tags_saved[symbol][statement] = {'statement tags': [current_container[statement]['statement tags'][0]]}#If no statement tags key is found, the key:value pair is created and the current tag is contained in a list.
                for item in self.statement_item_links.get(statement, []):#Iterate over statement items.
                    current_item_tags = current_container[statement].get(item)
                    if not current_item_tags:
                        continue
                    if len(current_item_tags) > 1:#Check for other items.
                        warnings.warn(f'{symbol}:{statement}-{item} has more than 1 tag. Verify correct tag and manually add.')
                    else:
                        if item in self.xbrl_tags_saved[symbol][statement]:#statement_item_links
                            self.xbrl_tags_saved[symbol][statement][item].append(current_item_tags[0])
                        else:
                            self.xbrl_tags_saved[symbol][statement][item] = [current_item_tags[0]]
        else:
            warnings.warn(f'{symbol} did not have current tags saved to access in {self.append_tags.__name__}')
        self.sym_statements_items_tags_current_container = {}

    def save_tags(self, file=None):
        """
        Save the current `xbrl_tags_saved` state.

        By default, this writes to the `xbrl_symbol_tags` database:

        ```python
        manager.save_tags()
        ```

        Each symbol is saved to a lowercase table name, and statement payloads
        are JSON-encoded into the statement columns.

        If `file` is provided, the method exports the in-memory dictionary to a
        JSON file instead of writing to the database. This keeps the old optional
        file-export behavior available for inspection or backup:

        ```python
        manager.save_tags(r"C:\\temp\\xbrl_tags_backup.json")
        ```
        """
        if file == None:
            self._save_xbrl_symbol_tags()
        else:
            with open(file, "w") as f:
                f.write(json.dumps(self.xbrl_tags_saved, indent=4, sort_keys=True))
        
    def update_symbols(self, symbols=None, filing_type=None, file=None):
        """
        Update multiple symbols and save the result at the end.

        If `symbols` is omitted, the method uses the symbols passed to
        `Xbrl_Tags_Manager(...)` during initialization. Symbols may be uppercase
        or lowercase; storage keys are normalized to lowercase.

        Workflow:

        - Existing symbols call `append_tags()` to add any newly discovered tags.
        - New symbols call `update_all_tags()` to create the initial payload.
        - If a new symbol has no usable `10-K`, the method tries `10-Q` as a
          fallback.
        - After all symbols are processed, `save_tags(file)` persists the result.

        Example:

        ```python
        manager = Xbrl_Tags_Manager(["AAPL", "MRSH"])
        manager.update_symbols(filing_type="10-K")
        ```

        To process a different list without recreating the manager:

        ```python
        manager.update_symbols(["FRMI", "RA"], filing_type="10-K")
        ```
        """
        symbols = self.symbols if symbols is None else [self._normalize_symbol(symbol) for symbol in symbols]
        for sym in symbols:
            try:                
                if sym in self.xbrl_tags_saved:
                    self.find_statements_tags_saved(sym)
                else:
                    try:
                        self.update_all_tags(sym, filing_type)
                        continue
                    except:
                        if filing_type == '10-K':
                            self.update_all_tags(sym, '10-Q')
                            continue
                        raise
                self.append_tags(sym, filing_type)
            except IndexError as ie:
                print(f'{sym} returned {ie} check results of filings.filing_urls() first. A different filing type my be needed.')
            except Exception as e:
                print(f'{sym}: {e}')
        self.save_tags(file)
        
