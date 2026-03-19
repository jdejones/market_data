from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path, PurePosixPath
from shlex import quote as shell_quote

try:
    from market_data.api_keys import database_password
except ModuleNotFoundError:
    sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
    from market_data.api_keys import database_password


DEFAULT_MYSQLDUMP_PATH = Path(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe")
DEFAULT_MYSQL_CLIENT_PATH = Path(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe")
DEFAULT_REMOTE_DIR = PurePosixPath("/home/openclaw/.openclaw/workspace/data_transfer/incoming")
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a MySQL database to SQLite and upload it to OpenClaw over Tailscale SSH."
    )
    parser.add_argument("--database", default="stocks", help="MySQL database to export.")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host.")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port.")
    parser.add_argument("--user", default="root", help="MySQL user.")
    parser.add_argument(
        "--password",
        default=database_password,
        help="MySQL password. Defaults to market_data.api_keys.database_password.",
    )
    parser.add_argument(
        "--mysqldump-path",
        type=Path,
        default=DEFAULT_MYSQLDUMP_PATH,
        help="Path to mysqldump.exe for producing a raw SQL backup.",
    )
    parser.add_argument(
        "--mysql-client-path",
        type=Path,
        default=DEFAULT_MYSQL_CLIENT_PATH,
        help="Path to mysql.exe. Stored for convenience and future extension.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd() / "db_exports",
        help="Local directory where export artifacts are written.",
    )
    parser.add_argument(
        "--sqlite-name",
        default="",
        help="Optional local SQLite filename. Defaults to '<database>_YYYYMMDD_HHMMSS.sqlite'.",
    )
    parser.add_argument(
        "--skip-mysqldump",
        action="store_true",
        help="Skip creation of the raw MySQL .sql dump backup.",
    )
    parser.add_argument(
        "--include-views",
        action="store_true",
        help="Also materialize MySQL views as SQLite tables.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5_000,
        help="Number of rows per chunk when copying MySQL tables to SQLite.",
    )
    parser.add_argument(
        "--ssh-target",
        default="openclaw@openclaw-ubuntu",
        help="Tailscale SSH target for the OpenClaw machine.",
    )
    parser.add_argument(
        "--remote-dir",
        default=str(DEFAULT_REMOTE_DIR),
        help="Remote directory where the SQLite database will be uploaded.",
    )
    parser.add_argument(
        "--remote-name",
        default="",
        help="Optional remote filename. Defaults to the local SQLite filename.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Build the local artifacts but do not upload them to OpenClaw.",
    )
    return parser.parse_args()


def ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def ensure_command_available(command_name: str) -> None:
    if shutil.which(command_name) is None:
        raise FileNotFoundError(f"Required command not found on PATH: {command_name}")


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def sqlite_identifier(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def mysql_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def local_sqlite_name(args: argparse.Namespace) -> str:
    if args.sqlite_name:
        return args.sqlite_name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{args.database}_{timestamp}.sqlite"


def create_mysql_dump(args: argparse.Namespace, sql_dump_path: Path) -> None:
    print(f"Creating MySQL dump: {sql_dump_path}")
    command = [
        str(args.mysqldump_path),
        f"--host={args.host}",
        f"--port={args.port}",
        f"--user={args.user}",
        f"--password={args.password}",
        "--single-transaction",
        "--skip-lock-tables",
        "--routines",
        "--events",
        "--triggers",
        "--column-statistics=0",
        args.database,
    ]
    with sql_dump_path.open("wb") as dump_handle:
        subprocess.run(command, check=True, stdout=dump_handle)


def mysql_base_command(args: argparse.Namespace) -> list[str]:
    return [
        str(args.mysql_client_path),
        f"--host={args.host}",
        f"--port={args.port}",
        f"--user={args.user}",
        f"--password={args.password}",
        "--batch",
        "--skip-column-names",
        "--default-character-set=utf8mb4",
        args.database,
    ]


def run_mysql_query(args: argparse.Namespace, query: str) -> subprocess.CompletedProcess[str]:
    command = mysql_base_command(args) + ["--execute", query]
    return subprocess.run(
        command,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )


def stream_mysql_query(args: argparse.Namespace, query: str) -> subprocess.Popen[str]:
    command = mysql_base_command(args) + ["--quick", "--raw", "--execute", query]
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def mysql_lines(result: subprocess.CompletedProcess[str]) -> list[str]:
    return [line for line in result.stdout.splitlines() if line.strip()]


def get_mysql_object_names(args: argparse.Namespace, table_type: str) -> list[str]:
    query = (
        "SELECT TABLE_NAME "
        "FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA = {mysql_string_literal(args.database)} "
        f"AND TABLE_TYPE = {mysql_string_literal(table_type)} "
        "ORDER BY TABLE_NAME"
    )
    return mysql_lines(run_mysql_query(args, query))


def get_mysql_columns(args: argparse.Namespace, table_name: str) -> list[dict[str, str]]:
    query = (
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
        "FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA = {mysql_string_literal(args.database)} "
        f"AND TABLE_NAME = {mysql_string_literal(table_name)} "
        "ORDER BY ORDINAL_POSITION"
    )
    columns: list[dict[str, str]] = []
    for line in mysql_lines(run_mysql_query(args, query)):
        column_name, data_type, is_nullable = line.split("\t")
        columns.append(
            {
                "name": column_name,
                "mysql_type": data_type.lower(),
                "nullable": is_nullable == "YES",
            }
        )
    return columns


def sqlite_type_for_mysql(mysql_type: str) -> str:
    integer_types = {"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "bit", "bool", "boolean"}
    real_types = {"float", "double", "decimal", "dec", "numeric", "real"}
    blob_types = {"blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary"}

    if mysql_type in integer_types:
        return "INTEGER"
    if mysql_type in real_types:
        return "REAL"
    if mysql_type in blob_types:
        return "BLOB"
    return "TEXT"


def table_row_count(args: argparse.Namespace, table_name: str) -> int:
    query = f"SELECT COUNT(*) FROM {mysql_identifier(table_name)}"
    return int(run_mysql_query(args, query).stdout.strip() or "0")


def create_sqlite_table(
    sqlite_conn: sqlite3.Connection,
    table_name: str,
    columns: list[dict[str, str]],
) -> None:
    column_defs = []
    for column in columns:
        sqlite_type = sqlite_type_for_mysql(column["mysql_type"])
        column["sqlite_type"] = sqlite_type
        null_clause = "" if column["nullable"] else " NOT NULL"
        column_defs.append(f"{sqlite_identifier(column['name'])} {sqlite_type}{null_clause}")

    create_sql = f"CREATE TABLE {sqlite_identifier(table_name)} ({', '.join(column_defs)})"
    sqlite_conn.execute(f"DROP TABLE IF EXISTS {sqlite_identifier(table_name)}")
    sqlite_conn.execute(create_sql)


def iter_mysql_json_rows(
    args: argparse.Namespace,
    table_name: str,
    columns: list[dict[str, str]],
):
    select_parts = []
    for column in columns:
        column_ref = mysql_identifier(column["name"])
        if sqlite_type_for_mysql(column["mysql_type"]) == "BLOB":
            select_parts.append(f"TO_BASE64({column_ref})")
        else:
            select_parts.append(column_ref)

    query = f"SELECT JSON_ARRAY({', '.join(select_parts)}) FROM {mysql_identifier(table_name)}"
    process = stream_mysql_query(args, query)

    assert process.stdout is not None
    assert process.stderr is not None
    for line in process.stdout:
        line = line.rstrip("\r\n")
        if line:
            yield json.loads(line)

    stderr = process.stderr.read()
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, process.args, output=None, stderr=stderr)


def normalize_mysql_row(row: list[object], columns: list[dict[str, str]]) -> tuple[object, ...]:
    converted: list[object] = []
    for value, column in zip(row, columns):
        if value is None:
            converted.append(None)
        elif column["sqlite_type"] == "BLOB":
            converted.append(base64.b64decode(value))
        elif isinstance(value, (list, dict)):
            converted.append(json.dumps(value, ensure_ascii=False))
        else:
            converted.append(value)
    return tuple(converted)


def copy_table_to_sqlite(
    args: argparse.Namespace,
    sqlite_conn: sqlite3.Connection,
    table_name: str,
    chunk_size: int,
) -> int:
    columns = get_mysql_columns(args, table_name)
    if not columns:
        raise RuntimeError(f"No columns found for MySQL object '{table_name}'.")

    create_sqlite_table(sqlite_conn, table_name, columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {sqlite_identifier(table_name)} VALUES ({placeholders})"

    batch: list[tuple[object, ...]] = []
    rows_written = 0
    for row in iter_mysql_json_rows(args, table_name, columns):
        batch.append(normalize_mysql_row(row, columns))
        if len(batch) >= chunk_size:
            sqlite_conn.executemany(insert_sql, batch)
            rows_written += len(batch)
            batch.clear()

    if batch:
        sqlite_conn.executemany(insert_sql, batch)
        rows_written += len(batch)

    return rows_written


def build_sqlite_database(args: argparse.Namespace, sqlite_path: Path) -> list[str]:
    print(f"Converting MySQL database '{args.database}' to SQLite: {sqlite_path}")
    if sqlite_path.exists():
        sqlite_path.unlink()

    object_names = get_mysql_object_names(args, "BASE TABLE")
    if args.include_views:
        object_names.extend(get_mysql_object_names(args, "VIEW"))
    object_names = sorted(dict.fromkeys(object_names))

    if not object_names:
        raise RuntimeError(f"No tables found in MySQL database '{args.database}'.")

    copied_objects: list[str] = []
    with sqlite3.connect(sqlite_path) as sqlite_conn:
        sqlite_conn.execute("PRAGMA journal_mode = DELETE;")
        sqlite_conn.execute("PRAGMA synchronous = OFF;")
        for object_name in object_names:
            expected_rows = table_row_count(args, object_name)
            print(f"Copying {object_name} ({expected_rows:,} rows expected)")
            rows_written = copy_table_to_sqlite(args, sqlite_conn, object_name, args.chunk_size)
            print(f"Finished {object_name}: {rows_written:,} rows written")
            copied_objects.append(object_name)
        sqlite_conn.commit()

    return copied_objects


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_tailscale_ssh(args: argparse.Namespace, remote_command: str, *, capture_output: bool = False) -> subprocess.CompletedProcess:
    command = ["tailscale", "ssh", args.ssh_target, remote_command]
    return subprocess.run(command, check=True, text=True, capture_output=capture_output)


def upload_file(args: argparse.Namespace, local_path: Path, remote_path: PurePosixPath) -> None:
    remote_dir = remote_path.parent
    remote_temp_path = PurePosixPath(f"{remote_path}.partial")

    print(f"Ensuring remote directory exists: {remote_dir}")
    run_tailscale_ssh(args, f"mkdir -p {shell_quote(remote_dir.as_posix())}")

    print(f"Uploading SQLite database to {args.ssh_target}:{remote_path}")
    upload_command = [
        "tailscale",
        "ssh",
        args.ssh_target,
        f"cat > {shell_quote(remote_temp_path.as_posix())}",
    ]

    bytes_sent = 0
    total_size = local_path.stat().st_size
    with local_path.open("rb") as source_handle:
        with subprocess.Popen(upload_command, stdin=subprocess.PIPE) as process:
            assert process.stdin is not None
            for chunk in iter(lambda: source_handle.read(UPLOAD_CHUNK_SIZE), b""):
                process.stdin.write(chunk)
                bytes_sent += len(chunk)
                progress = (bytes_sent / total_size) * 100 if total_size else 100.0
                print(f"Uploaded {bytes_sent:,} / {total_size:,} bytes ({progress:.1f}%)")
            process.stdin.close()
            return_code = process.wait()
            if return_code != 0:
                raise subprocess.CalledProcessError(return_code, upload_command)

    local_hash = sha256_file(local_path)
    remote_hash_result = run_tailscale_ssh(
        args,
        f"sha256sum {shell_quote(remote_temp_path.as_posix())}",
        capture_output=True,
    )
    remote_hash = remote_hash_result.stdout.strip().split()[0]
    if remote_hash != local_hash:
        raise RuntimeError(
            "SHA256 mismatch after upload "
            f"(local={local_hash}, remote={remote_hash}, remote_path={remote_temp_path})"
        )

    run_tailscale_ssh(
        args,
        f"mv {shell_quote(remote_temp_path.as_posix())} {shell_quote(remote_path.as_posix())}",
    )
    print(f"Upload verified and finalized at {remote_path}")


def main() -> None:
    args = parse_args()

    ensure_command_available("tailscale")
    ensure_exists(args.mysql_client_path, "mysql client")
    if not args.skip_mysqldump:
        ensure_exists(args.mysqldump_path, "mysqldump")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    sqlite_filename = local_sqlite_name(args)
    sqlite_path = args.output_dir / sqlite_filename
    sql_dump_path = args.output_dir / f"{Path(sqlite_filename).stem}.sql"
    remote_name = args.remote_name or sqlite_filename
    remote_path = PurePosixPath(args.remote_dir) / remote_name

    print(f"MySQL source: {args.user}@{args.host}:{args.port}/{args.database}")
    print(f"Local SQLite output: {sqlite_path}")
    print(f"Remote target: {args.ssh_target}:{remote_path}")

    if not args.skip_mysqldump:
        create_mysql_dump(args, sql_dump_path)
        print(f"Raw SQL backup saved to: {sql_dump_path}")

    copied_objects = build_sqlite_database(args, sqlite_path)
    print(f"SQLite export complete. Objects copied: {len(copied_objects)}")

    if args.skip_upload:
        print("Upload skipped by request.")
        return

    upload_file(args, sqlite_path, remote_path)
    print("Database transfer complete.")


if __name__ == "__main__":
    main()
