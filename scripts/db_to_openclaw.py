from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from shlex import quote as shell_quote
import time

try:
    from market_data import tqdm
    from market_data.api_keys import database_password
except ModuleNotFoundError:
    sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
    from market_data import tqdm
    from market_data.api_keys import database_password


DEFAULT_MYSQLDUMP_PATH = Path(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe")
DEFAULT_MYSQL_CLIENT_PATH = Path(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe")
DEFAULT_REMOTE_DIR = PurePosixPath("/home/openclaw/.openclaw/workspace/data_transfer/incoming")
DEFAULT_DATABASES = ("stocks", "results_finvizsearch", "news")
DEFAULT_WORKERS = max(1, min(4, os.cpu_count() or 1))
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024
SSH_COMMON_OPTIONS = (
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "ConnectTimeout=15",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert one or more MySQL databases to SQLite and upload them to OpenClaw over Tailscale SSH."
    )
    parser.add_argument(
        "--database",
        default="",
        help="Single MySQL database to export. Overrides --databases when provided.",
    )
    parser.add_argument(
        "--databases",
        nargs="+",
        default=list(DEFAULT_DATABASES),
        help="MySQL databases to export into the SQLite file.",
    )
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
        help="Optional local SQLite filename. Defaults to '<database>_YYYYMMDD_HHMMSS.sqlite' for a single database or '<db1>_<db2>_..._YYYYMMDD_HHMMSS.sqlite' for multiple databases.",
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
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Worker processes used while building the SQLite database.",
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
        "--upload-transport",
        default="auto",
        choices=("auto", "ssh-scp", "tailscale-ssh"),
        help=(
            "Upload method to use. 'auto' tries standard ssh/scp first and "
            "falls back to tailscale ssh."
        ),
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Build the local artifacts but do not upload them to OpenClaw.",
    )
    return parser.parse_args()


def selected_databases(args: argparse.Namespace) -> list[str]:
    requested = [args.database] if args.database else args.databases
    databases = [database.strip() for database in requested if database and database.strip()]
    databases = list(dict.fromkeys(databases))
    if not databases:
        raise ValueError("At least one MySQL database must be provided.")
    return databases


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
    name_prefix = "_".join(args.databases)
    return f"{name_prefix}_{timestamp}.sqlite"


def sqlite_table_name(database_name: str, object_name: str, *, prefix_database: bool) -> str:
    if prefix_database:
        return f"{database_name}__{object_name}"
    return object_name


def open_sqlite_connection(sqlite_path: Path) -> sqlite3.Connection:
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.execute("PRAGMA journal_mode = DELETE;")
    sqlite_conn.execute("PRAGMA synchronous = OFF;")
    return sqlite_conn


@contextmanager
def managed_sqlite_connection(sqlite_path: Path):
    sqlite_conn = open_sqlite_connection(sqlite_path)
    try:
        yield sqlite_conn
    finally:
        sqlite_conn.close()


def create_mysql_dump(args: argparse.Namespace, sql_dump_path: Path) -> None:
    print(f"Creating MySQL dump for {', '.join(args.databases)}: {sql_dump_path}")
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
        "--databases",
        *args.databases,
    ]
    with sql_dump_path.open("wb") as dump_handle:
        subprocess.run(command, check=True, stdout=dump_handle)


def mysql_base_command(args: argparse.Namespace, database_name: str) -> list[str]:
    return [
        str(args.mysql_client_path),
        f"--host={args.host}",
        f"--port={args.port}",
        f"--user={args.user}",
        f"--password={args.password}",
        "--batch",
        "--skip-column-names",
        "--default-character-set=utf8mb4",
        database_name,
    ]


def run_mysql_query(args: argparse.Namespace, database_name: str, query: str) -> subprocess.CompletedProcess[str]:
    command = mysql_base_command(args, database_name) + ["--execute", query]
    return subprocess.run(
        command,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )


def stream_mysql_query(args: argparse.Namespace, database_name: str, query: str) -> subprocess.Popen[str]:
    command = mysql_base_command(args, database_name) + ["--quick", "--raw", "--execute", query]
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


def get_mysql_object_names(args: argparse.Namespace, database_name: str, table_type: str) -> list[str]:
    query = (
        "SELECT TABLE_NAME "
        "FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA = {mysql_string_literal(database_name)} "
        f"AND TABLE_TYPE = {mysql_string_literal(table_type)} "
        "ORDER BY TABLE_NAME"
    )
    return mysql_lines(run_mysql_query(args, database_name, query))


def get_mysql_columns(args: argparse.Namespace, database_name: str, table_name: str) -> list[dict[str, str]]:
    query = (
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
        "FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA = {mysql_string_literal(database_name)} "
        f"AND TABLE_NAME = {mysql_string_literal(table_name)} "
        "ORDER BY ORDINAL_POSITION"
    )
    columns: list[dict[str, str]] = []
    for line in mysql_lines(run_mysql_query(args, database_name, query)):
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
    database_name: str,
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
    process = stream_mysql_query(args, database_name, query)

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
    database_name: str,
    source_table_name: str,
    destination_table_name: str,
    chunk_size: int,
) -> int:
    columns = get_mysql_columns(args, database_name, source_table_name)
    if not columns:
        raise RuntimeError(f"No columns found for MySQL object '{database_name}.{source_table_name}'.")

    create_sqlite_table(sqlite_conn, destination_table_name, columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {sqlite_identifier(destination_table_name)} VALUES ({placeholders})"

    batch: list[tuple[object, ...]] = []
    rows_written = 0
    for row in iter_mysql_json_rows(args, database_name, source_table_name, columns):
        batch.append(normalize_mysql_row(row, columns))
        if len(batch) >= chunk_size:
            sqlite_conn.executemany(insert_sql, batch)
            rows_written += len(batch)
            batch.clear()

    if batch:
        sqlite_conn.executemany(insert_sql, batch)
        rows_written += len(batch)

    return rows_written


def collect_table_tasks(args: argparse.Namespace) -> list[tuple[str, str, str]]:
    prefix_database = len(args.databases) > 1
    table_tasks: list[tuple[str, str, str]] = []
    for database_name in args.databases:
        object_names = get_mysql_object_names(args, database_name, "BASE TABLE")
        if args.include_views:
            object_names.extend(get_mysql_object_names(args, database_name, "VIEW"))
        object_names = sorted(dict.fromkeys(object_names))

        if not object_names:
            raise RuntimeError(f"No tables found in MySQL database '{database_name}'.")

        for object_name in object_names:
            table_tasks.append(
                (
                    database_name,
                    object_name,
                    sqlite_table_name(
                        database_name,
                        object_name,
                        prefix_database=prefix_database,
                    ),
                )
            )
    return table_tasks


def table_task_batches(
    table_tasks: list[tuple[str, str, str]],
    workers: int,
) -> list[list[tuple[str, str, str]]]:
    target_batch_count = max(1, workers * 4)
    batch_size = max(1, min(25, (len(table_tasks) + target_batch_count - 1) // target_batch_count))
    return [table_tasks[index:index + batch_size] for index in range(0, len(table_tasks), batch_size)]


def copy_table_batch_to_shard(
    args: argparse.Namespace,
    shard_path: str,
    table_tasks: list[tuple[str, str, str]],
) -> list[tuple[str, str, str, int]]:
    copied_tables: list[tuple[str, str, str, int]] = []
    with managed_sqlite_connection(Path(shard_path)) as sqlite_conn:
        for database_name, source_table_name, destination_table_name in table_tasks:
            rows_written = copy_table_to_sqlite(
                args,
                sqlite_conn,
                database_name,
                source_table_name,
                destination_table_name,
                args.chunk_size,
            )
            copied_tables.append(
                (
                    database_name,
                    source_table_name,
                    destination_table_name,
                    rows_written,
                )
            )
        sqlite_conn.commit()
    return copied_tables


def merge_sqlite_shard(sqlite_conn: sqlite3.Connection, shard_path: Path) -> None:
    sqlite_conn.execute("ATTACH DATABASE ? AS shard_db", (str(shard_path),))
    shard_tables = sqlite_conn.execute(
        "SELECT name, sql FROM shard_db.sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    for table_name, create_sql in shard_tables:
        sqlite_conn.execute(create_sql)
        sqlite_conn.execute(
            f"INSERT INTO {sqlite_identifier(table_name)} "
            f"SELECT * FROM shard_db.{sqlite_identifier(table_name)}"
        )
    sqlite_conn.commit()
    sqlite_conn.execute("DETACH DATABASE shard_db")


def build_sqlite_database_serial(
    args: argparse.Namespace,
    sqlite_path: Path,
    table_tasks: list[tuple[str, str, str]],
) -> list[str]:
    copied_objects: list[str] = []
    with managed_sqlite_connection(sqlite_path) as sqlite_conn:
        with tqdm(
            total=len(table_tasks),
            desc="Building SQLite",
            unit="table",
            dynamic_ncols=True,
        ) as progress_bar:
            for database_name, source_table_name, destination_table_name in table_tasks:
                copy_table_to_sqlite(
                    args,
                    sqlite_conn,
                    database_name,
                    source_table_name,
                    destination_table_name,
                    args.chunk_size,
                )
                copied_objects.append(destination_table_name)
                progress_bar.update(1)
                progress_bar.set_postfix_str(
                    f"{database_name}.{source_table_name}",
                    refresh=False,
                )
        sqlite_conn.commit()
    return copied_objects


def build_sqlite_database_parallel(
    args: argparse.Namespace,
    sqlite_path: Path,
    table_tasks: list[tuple[str, str, str]],
) -> list[str]:
    copied_objects: list[str] = []
    worker_count = min(args.workers, len(table_tasks))
    batches = table_task_batches(table_tasks, worker_count)
    print(
        f"Copying {len(table_tasks):,} tables with {worker_count} worker "
        f"processes across {len(batches):,} shard batches"
    )
    with tempfile.TemporaryDirectory(prefix=f"{sqlite_path.stem}_parts_", dir=str(args.output_dir)) as temp_dir:
        # SQLite only supports one writer at a time, so workers write to shard
        # databases in parallel and the parent merges those shards serially.
        with managed_sqlite_connection(sqlite_path) as sqlite_conn:
            with tqdm(
                total=len(table_tasks),
                desc="Building SQLite",
                unit="table",
                dynamic_ncols=True,
            ) as progress_bar:
                with ProcessPoolExecutor(max_workers=worker_count) as executor:
                    futures = {}
                    for batch_index, batch in enumerate(batches):
                        shard_path = Path(temp_dir) / f"shard_{batch_index:05d}.sqlite"
                        future = executor.submit(copy_table_batch_to_shard, args, str(shard_path), batch)
                        futures[future] = shard_path

                    for future in as_completed(futures):
                        shard_path = futures[future]
                        copied_batch = future.result()
                        merge_sqlite_shard(sqlite_conn, shard_path)
                        shard_path.unlink(missing_ok=True)
                        copied_objects.extend(
                            destination_table_name
                            for _, _, destination_table_name, _ in copied_batch
                        )
                        progress_bar.update(len(copied_batch))
                        if copied_batch:
                            last_database_name, last_source_table_name, _, _ = copied_batch[-1]
                            progress_bar.set_postfix_str(
                                f"{last_database_name}.{last_source_table_name}",
                                refresh=False,
                            )
            sqlite_conn.commit()
    return copied_objects


def build_sqlite_database(args: argparse.Namespace, sqlite_path: Path) -> list[str]:
    print(f"Converting MySQL databases {', '.join(args.databases)} to SQLite: {sqlite_path}")
    if sqlite_path.exists():
        sqlite_path.unlink()

    table_tasks = collect_table_tasks(args)
    print(f"Discovered {len(table_tasks):,} tables/views to copy")

    if args.workers <= 1 or len(table_tasks) <= 1:
        return build_sqlite_database_serial(args, sqlite_path, table_tasks)

    return build_sqlite_database_parallel(args, sqlite_path, table_tasks)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_tailscale_ssh(args: argparse.Namespace, remote_command: str, *, capture_output: bool = False) -> subprocess.CompletedProcess:
    command = ["tailscale", "ssh", args.ssh_target, remote_command]
    return subprocess.run(command, check=True, text=True, capture_output=capture_output)


def run_ssh_command(args: argparse.Namespace, remote_command: str, *, capture_output: bool = False) -> subprocess.CompletedProcess:
    command = ["ssh", *SSH_COMMON_OPTIONS, args.ssh_target, remote_command]
    return subprocess.run(command, check=True, text=True, capture_output=capture_output)


def run_remote_command(
    args: argparse.Namespace,
    remote_command: str,
    *,
    transport: str,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    if transport == "ssh-scp":
        return run_ssh_command(args, remote_command, capture_output=capture_output)
    if transport == "tailscale-ssh":
        return run_tailscale_ssh(args, remote_command, capture_output=capture_output)
    raise ValueError(f"Unsupported upload transport: {transport}")


def available_upload_transports(args: argparse.Namespace) -> list[str]:
    if args.upload_transport == "auto":
        return ["ssh-scp", "tailscale-ssh"]
    return [args.upload_transport]


def summarize_called_process_error(error: subprocess.CalledProcessError) -> str:
    output_chunks = []
    if error.stdout:
        output_chunks.append(error.stdout.strip())
    if error.stderr:
        output_chunks.append(error.stderr.strip())
    output_text = "\n".join(chunk for chunk in output_chunks if chunk)
    if output_text:
        return output_text
    return f"exit code {error.returncode}"


def is_tailscale_additional_check_error(error: subprocess.CalledProcessError) -> bool:
    details = summarize_called_process_error(error).lower()
    return (
        "requires an additional check" in details
        or "failed to fetch next ssh action" in details
    )


def ensure_upload_tools_available(args: argparse.Namespace) -> None:
    transports = available_upload_transports(args)
    if "ssh-scp" in transports:
        ensure_command_available("ssh")
        ensure_command_available("scp")
    if "tailscale-ssh" in transports:
        ensure_command_available("tailscale")


def select_upload_transport(args: argparse.Namespace, remote_dir: PurePosixPath) -> str:
    preflight_command = (
        f"mkdir -p {shell_quote(remote_dir.as_posix())} "
        f"&& test -d {shell_quote(remote_dir.as_posix())}"
    )
    failures: list[tuple[str, str]] = []

    for transport in available_upload_transports(args):
        try:
            print(f"Checking remote access via {transport}")
            run_remote_command(args, preflight_command, transport=transport, capture_output=True)
            return transport
        except subprocess.CalledProcessError as error:
            detail = summarize_called_process_error(error)
            failures.append((transport, detail))
            if transport == "tailscale-ssh" and is_tailscale_additional_check_error(error):
                print("Tailscale SSH requires an interactive approval check; trying the next upload transport.")
            else:
                print(f"{transport} preflight failed: {detail}")

    failure_text = "\n".join(f"- {transport}: {detail}" for transport, detail in failures)
    raise RuntimeError(
        "Unable to establish remote upload access before starting the export.\n"
        f"{failure_text}"
    )


def verify_and_finalize_remote_upload(
    args: argparse.Namespace,
    local_path: Path,
    remote_path: PurePosixPath,
    remote_temp_path: PurePosixPath,
    *,
    transport: str,
) -> None:
    local_hash = sha256_file(local_path)
    remote_hash_result = run_remote_command(
        args,
        f"sha256sum {shell_quote(remote_temp_path.as_posix())}",
        transport=transport,
        capture_output=True,
    )
    remote_hash = remote_hash_result.stdout.strip().split()[0]
    if remote_hash != local_hash:
        raise RuntimeError(
            "SHA256 mismatch after upload "
            f"(local={local_hash}, remote={remote_hash}, remote_path={remote_temp_path})"
        )

    run_remote_command(
        args,
        f"mv {shell_quote(remote_temp_path.as_posix())} {shell_quote(remote_path.as_posix())}",
        transport=transport,
    )
    print(f"Upload verified and finalized at {remote_path}")


def upload_file_via_ssh_scp(
    args: argparse.Namespace,
    local_path: Path,
    remote_path: PurePosixPath,
) -> None:
    remote_dir = remote_path.parent
    remote_temp_path = PurePosixPath(f"{remote_path}.partial")

    print(f"Ensuring remote directory exists: {remote_dir}")
    run_ssh_command(args, f"mkdir -p {shell_quote(remote_dir.as_posix())}")

    print(f"Uploading SQLite database to {args.ssh_target}:{remote_path} via scp")
    remote_spec = f"{args.ssh_target}:{remote_temp_path.as_posix()}"
    upload_command = ["scp", *SSH_COMMON_OPTIONS, str(local_path), remote_spec]
    upload_result = subprocess.run(upload_command, check=False, text=True, capture_output=True)
    if upload_result.returncode != 0:
        raise subprocess.CalledProcessError(
            upload_result.returncode,
            upload_command,
            output=upload_result.stdout,
            stderr=upload_result.stderr,
        )

    verify_and_finalize_remote_upload(
        args,
        local_path,
        remote_path,
        remote_temp_path,
        transport="ssh-scp",
    )


def upload_file_via_tailscale_ssh(
    args: argparse.Namespace,
    local_path: Path,
    remote_path: PurePosixPath,
) -> None:
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

    verify_and_finalize_remote_upload(
        args,
        local_path,
        remote_path,
        remote_temp_path,
        transport="tailscale-ssh",
    )


def upload_file(args: argparse.Namespace, local_path: Path, remote_path: PurePosixPath, *, transport: str) -> None:
    if transport == "ssh-scp":
        upload_file_via_ssh_scp(args, local_path, remote_path)
        return
    if transport == "tailscale-ssh":
        upload_file_via_tailscale_ssh(args, local_path, remote_path)
        return
    raise ValueError(f"Unsupported upload transport: {transport}")

def disconnect_vpn():
    subprocess.run(r'cd /d "C:\Program Files\NordVPN" && nordvpn -d', 
                shell=True, 
                check=True)
    print("VPN disconnected")

def connect_vpn():
    subprocess.run(r'cd /d "C:\Program Files\NordVPN" && nordvpn -c', 
                shell=True, 
                check=True)
    print("VPN reconnected")

def main() -> None:
    disconnect_vpn()
    time.sleep(5)
    
    args = parse_args()
    args.databases = selected_databases(args)
    if args.workers < 1:
        raise ValueError("--workers must be at least 1.")

    ensure_exists(args.mysql_client_path, "mysql client")
    if not args.skip_mysqldump:
        ensure_exists(args.mysqldump_path, "mysqldump")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    sqlite_filename = local_sqlite_name(args)
    sqlite_path = args.output_dir / sqlite_filename
    sql_dump_path = args.output_dir / f"{Path(sqlite_filename).stem}.sql"
    remote_name = args.remote_name or sqlite_filename
    remote_path = PurePosixPath(args.remote_dir) / remote_name

    print(f"MySQL sources: {args.user}@{args.host}:{args.port} ({', '.join(args.databases)})")
    print(f"Local SQLite output: {sqlite_path}")
    print(f"Remote target: {args.ssh_target}:{remote_path}")

    upload_transport = ""
    if not args.skip_upload:
        ensure_upload_tools_available(args)
        upload_transport = select_upload_transport(args, remote_path.parent)
        print(f"Using upload transport: {upload_transport}")

    if not args.skip_mysqldump:
        create_mysql_dump(args, sql_dump_path)
        print(f"Raw SQL backup saved to: {sql_dump_path}")

    copied_objects = build_sqlite_database(args, sqlite_path)
    print(f"SQLite export complete. Objects copied: {len(copied_objects)}")

    if args.skip_upload:
        print("Upload skipped by request.")
        return

    upload_file(args, sqlite_path, remote_path, transport=upload_transport)
    print("Database transfer complete.")
    connect_vpn()

if __name__ == "__main__":
    main()
