import json
import warnings

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
    return f"`{identifier.replace('`', '``')}`"


class Xbrl_Tags_Manager:
    def __init__(self, symbols: list):
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

    def _make_xbrl_tags_engine(self):
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
        with self._xbrl_symbol_tags_engine.connect() as conn:
            symbols = conn.execute(
                text(
                    "SELECT table_name "
                    "FROM information_schema.tables "
                    "WHERE table_schema = :schema"
                ),
                {"schema": XBRL_SYMBOL_TAGS_DB},
            ).scalars().all()

            return {
                symbol: self._load_xbrl_symbol_table(conn, symbol)
                for symbol in symbols
            }

    def _load_xbrl_symbol_table(self, conn, symbol: str) -> dict:
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
        with self._xbrl_symbol_tags_engine.begin() as conn:
            for symbol, symbol_tags in self.xbrl_tags_saved.items():
                self._save_xbrl_symbol_table(conn, symbol, symbol_tags)

    def _save_xbrl_symbol_table(self, conn, symbol: str, symbol_tags: dict):
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
        if symbol in self.xbrl_tags_saved.keys():#Checks for symbol in saved xbrl tags json file.
            self.sym_statements_items_tags_saved_container = self.xbrl_tags_saved[symbol]#Adds the symbols saved tags to a temporary container.
        else:
            self.sym_statements_items_tags_saved_container = None#If no tags are saved the temporary container gets assigned a value of None.
    
    #Function for identifying current SEC filed tags for a symbol.
    def find_statements_tags_current(self, symbol, filing_type):
        filing = self.xbrlApi.xbrl_to_json(htm_url=filings.filing_urls(symbol, filing_type, no_filings=1)[0])#Brings the filing into the namespace.
        for statement in self.statement_tags:#Iterates through statement identifiers.
            for tag in self.statement_tags[statement]:#Iterates through a list of known statment tags for the current statement identifier.
                if tag in filing.keys():#Checks for the presence of the statement tag in SEC filing.
                    self.sym_statements_items_tags_current_container[statement] = {'statement tags': [tag]}
                    break
            try:
                if len(self.sym_statements_items_tags_current_container[statement]) == 0:#Handles symbols whose statement tags I don't have an identifier for.
                    self.symbols_tags_needed[statement] = {}
            except KeyError as ke:
                if ke == f'{statement}':
                    continue
        for statement in self.sym_statements_items_tags_current_container:
            for item in self.statement_item_links[statement]:
                for tag in self.item_tags[item]:
                    if tag in filing[self.sym_statements_items_tags_current_container[statement]['statement tags'][0]]:
                        self.sym_statements_items_tags_current_container[statement][item] = [tag]
    
    def update_all_tags(self, symbol, filing_type):
       self.find_statements_tags_current(symbol, filing_type)
       self.xbrl_tags_saved[symbol] = self.sym_statements_items_tags_current_container
       self.sym_statements_items_tags_current_container = {}
    
    def append_tags(self, symbol, filing_type):
        self.find_statements_tags_current(symbol, filing_type)
        if len(self.sym_statements_items_tags_current_container) > 0:#Checks that tags were found.
            for statement in self.sym_statements_items_tags_current_container:#Iterates statement current.
                if statement in self.xbrl_tags_saved[symbol]:
                    if len(self.sym_statements_items_tags_current_container[statement]['statement tags']) > 1:#Raise warning if more than one tag is found.
                        warnings.warn(f'{symbol} has more than 1 statement tag. Verify correct tag and manually add.')
                    else:
                        if 'statement tags' in self.xbrl_tags_saved[symbol][statement]:#Check for list of statement tags.
                            self.xbrl_tags_saved[symbol][statement]['statement tags'].append(self.sym_statements_items_tags_current_container[statement]['statement tags'][0])#Append first current statement tag to symbol tag structure.
                        else:
                            self.xbrl_tags_saved[symbol][statement] = {'statement tags': [self.sym_statements_items_tags_current_container[statement]['statement tags'][0]]}#If no statement tags key is found, the key:value pair is created and the current tag is contained in a list.
                    for item in self.statement_item_links[statement]:#Iterate over statement items.
                        if len(self.sym_statements_items_tags_current_container[statement][item]) > 1:#Check for other items.
                            warnings.warn(f'{symbol}:{statement}-{item} has more than 1 tag. Verify correct tag and manually add.')
                        else:
                            if item in self.xbrl_tags_saved[symbol]:
                                self.xbrl_tags_saved[symbol][statement][item].append(self.sym_statements_items_tags_current_container[statement][item][0])
                            else:
                                self.xbrl_tags_saved[symbol][statement][item] = [self.sym_statements_items_tags_current_container[statement][item][0]]
                else:
                    self.xbrl_tags_saved[symbol][statement] = {'statement tags': []}
                    if len(self.sym_statements_items_tags_current_container[statement]['statement tags']) > 1:#Raise warning if more than one tag is found.
                        warnings.warn(f'{symbol} has more than 1 statement tag. Verify correct tag and manually add.')
                    else:
                        if 'statement tags' in self.xbrl_tags_saved[symbol][statement]:#Check for list of statement tags.
                            self.xbrl_tags_saved[symbol][statement]['statement tags'].append(self.sym_statements_items_tags_current_container[statement]['statement tags'][0])#Append first current statement tag to symbol tag structure.
                        else:
                            self.xbrl_tags_saved[symbol][statement] = {'statement tags': [self.sym_statements_items_tags_current_container[statement]['statement tags'][0]]}#If no statement tags key is found, the key:value pair is created and the current tag is contained in a list.
                    for item in self.statement_item_links[statement]:#Iterate over statement items.
                        if len(self.sym_statements_items_tags_current_container[statement][item]) > 1:#Check for other items.
                            warnings.warn(f'{symbol}:{statement}-{item} has more than 1 tag. Verify correct tag and manually add.')
                        else:
                            if item in self.xbrl_tags_saved[symbol]:#statement_item_links
                                self.xbrl_tags_saved[symbol][statement][item].append(self.sym_statements_items_tags_current_container[statement][item][0])
                            else:
                                self.xbrl_tags_saved[symbol][statement][item] = [self.sym_statements_items_tags_current_container[statement][item][0]]
                    
        else:
            warnings.warn(f'{symbol} did not have current tags saved to access in {self.append_tags.__name__}')
        self.sym_statements_items_tags_current_container = {}

    def save_tags(self, file=None):
        if file == None:
            self._save_xbrl_symbol_tags()
        else:
            with open(file, "w") as f:
                f.write(json.dumps(self.xbrl_tags_saved, indent=4, sort_keys=True))
        
    def update_symbols(self, symbols, filing_type, file=None):
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
                self.append_tags(sym, filing_type)
            except IndexError as ie:
                print(f'{sym} returned {ie} check results of filings.filing_urls() first. A different filing type my be needed.')
            except Exception as e:
                print(f'{sym}: {e}')
        self.save_tags(file)
        
