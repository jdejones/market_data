from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from shlex import quote as shell_quote
import re
import socket


DEFAULT_SOURCE_DIR = Path(r"E:\Market Research\Dataset\daily_after_close_study")
DEFAULT_REMOTE_DIR = PurePosixPath("/home/openclaw/.openclaw/workspace/data_transfer/incoming")
DEFAULT_SSH_TARGET = "openclaw@openclaw-ubuntu"
DEFAULT_PRODUCER = "daily_after_close_study.py"
DEFAULT_MANIFEST_NAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = "1.0"
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024
BATCH_ID_PREFIX = "daily_pkl_"
BATCH_ID_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}_\d{6}$")
SSH_COMMON_OPTIONS = (
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "ConnectTimeout=15",
)


@dataclass(frozen=True)
class PayloadSpec:
    filename: str
    object_kind: str
    description: str
    dependency_notes: str | None = None


PAYLOAD_SPECS: tuple[PayloadSpec, ...] = (
    PayloadSpec(
        "symbols.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "High-dollar-volume SymbolData map with refreshed technicals",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "daily_quant_rating_df.pkl.gz",
        "pandas.DataFrame",
        "Daily Seeking Alpha quant rating history",
    ),
    PayloadSpec(
        "sec.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "Sector index SymbolData aggregates",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "ind.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "Industry index SymbolData aggregates",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "sp500.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "S&P 500 member SymbolData map",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "mdy.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "MDY member SymbolData map",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "iwm.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "IWM member SymbolData map",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "etfs.pkl.gz",
        "dict[str, market_data.Symbol_Data.SymbolData]",
        "ETF SymbolData map",
        "Dictionary values require market_data.Symbol_Data.SymbolData when unpickling.",
    ),
    PayloadSpec(
        "stock_stats.pkl.gz",
        "dict[str, dict]",
        "Per-symbol condition statistics and return summaries",
    ),
    PayloadSpec(
        "ev.pkl.gz",
        "dict[str, dict[str, float]]",
        "Expected value by condition and return horizon",
    ),
    PayloadSpec(
        "all_returns.pkl.gz",
        "dict[str, dict[str, list[float]]]",
        "Raw condition returns grouped by horizon",
    ),
    PayloadSpec(
        "sector_close_vwap_ratio.pkl.gz",
        "dict[str, float]",
        "Sector close-over-VWAP percentages",
    ),
    PayloadSpec(
        "industry_close_vwap_ratio.pkl.gz",
        "dict[str, float]",
        "Industry close-over-VWAP percentages",
    ),
    PayloadSpec(
        "ep_curdur.pkl.gz",
        "dict[str, list[float | int]]",
        "Episodic pivot duration plus quant rating",
    ),
    PayloadSpec(
        "ep_rr.pkl.gz",
        "dict[str, list[float]]",
        "Episodic pivot reward-risk plus quant rating",
    ),
    PayloadSpec(
        "rel_stren.pkl.gz",
        "list[tuple[str, float]]",
        "Relative strength rankings versus SPY",
    ),
    PayloadSpec(
        "prev_perf_since_earnings.pkl.gz",
        "list[tuple[str, float]]",
        "Performance since prior earnings season",
    ),
    PayloadSpec(
        "perf_since_earnings.pkl.gz",
        "list[tuple[str, float]]",
        "Performance since current earnings season",
    ),
    PayloadSpec(
        "days_elevated_rvol.pkl.gz",
        "dict[str, int]",
        "Consecutive days with relative volume above one",
    ),
    PayloadSpec(
        "days_range_expansion.pkl.gz",
        "dict[str, int]",
        "Consecutive days with ATRs traded above one",
    ),
    PayloadSpec(
        "results_finvizsearch.pkl.gz",
        "pandas.DataFrame",
        "Finviz screener snapshot with derived factors",
    ),
    PayloadSpec(
        "tsc.pkl.gz",
        "market_data.watchlist_filters.Technical_Score_Calculator",
        "Technical score calculator state for symbols",
        "Requires market_data.watchlist_filters.Technical_Score_Calculator when unpickling.",
    ),
    PayloadSpec(
        "tsc_sec.pkl.gz",
        "market_data.watchlist_filters.Technical_Score_Calculator",
        "Technical score calculator state for sectors",
        "Requires market_data.watchlist_filters.Technical_Score_Calculator when unpickling.",
    ),
    PayloadSpec(
        "tsc_ind.pkl.gz",
        "market_data.watchlist_filters.Technical_Score_Calculator",
        "Technical score calculator state for industries",
        "Requires market_data.watchlist_filters.Technical_Score_Calculator when unpickling.",
    ),
    PayloadSpec(
        "qplus1.pkl.gz",
        "dict[str, float]",
        "Expected revenue growth for next quarter",
    ),
    PayloadSpec(
        "qplus4.pkl.gz",
        "dict[str, float]",
        "Expected revenue growth four quarters ahead",
    ),
    PayloadSpec(
        "interest_list_long.pkl.gz",
        "list[str]",
        "Long interest-list symbols",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Transfer the daily_after_close_study pickle snapshots to OpenClaw "
            "over the Tailscale network with a batch manifest."
        )
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help="Directory containing the daily_after_close_study .pkl.gz payloads.",
    )
    parser.add_argument(
        "--ssh-target",
        default=DEFAULT_SSH_TARGET,
        help="Tailscale SSH target for the OpenClaw machine.",
    )
    parser.add_argument(
        "--remote-dir",
        default=str(DEFAULT_REMOTE_DIR),
        help="Remote directory where the batch folder will be uploaded.",
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
        "--batch-id",
        default="",
        help="Optional batch identifier in YYYY_MM_DD_HHMMSS format.",
    )
    parser.add_argument(
        "--producer",
        default=DEFAULT_PRODUCER,
        help="Producer name written to the manifest.",
    )
    parser.add_argument(
        "--source-machine",
        default=socket.gethostname(),
        help="Source machine name written to the manifest.",
    )
    return parser.parse_args()


def ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def ensure_command_available(command_name: str) -> None:
    if shutil.which(command_name) is None:
        raise FileNotFoundError(f"Required command not found on PATH: {command_name}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_batch_id(raw_batch_id: str) -> str:
    batch_id = raw_batch_id.strip() if raw_batch_id else datetime.now().strftime("%Y_%m_%d_%H%M%S")
    if batch_id.startswith(BATCH_ID_PREFIX):
        batch_id = batch_id[len(BATCH_ID_PREFIX):]
    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise ValueError("Batch ID must match YYYY_MM_DD_HHMMSS, optionally prefixed with daily_pkl_.")
    return f"{BATCH_ID_PREFIX}{batch_id}"


def resolve_payload_paths(source_dir: Path) -> list[tuple[PayloadSpec, Path]]:
    ensure_exists(source_dir, "Source directory")
    payloads: list[tuple[PayloadSpec, Path]] = []
    missing_payloads: list[str] = []

    for spec in PAYLOAD_SPECS:
        payload_path = source_dir / spec.filename
        if payload_path.exists():
            payloads.append((spec, payload_path))
        else:
            missing_payloads.append(spec.filename)

    if missing_payloads:
        missing_text = "\n".join(f"- {name}" for name in missing_payloads)
        raise FileNotFoundError(
            "One or more expected daily_after_close_study payloads are missing.\n"
            f"{missing_text}"
        )

    return payloads


def build_manifest(
    batch_id: str,
    producer: str,
    source_machine: str,
    payloads: list[tuple[PayloadSpec, Path]],
) -> dict[str, object]:
    files: list[dict[str, str]] = []

    for spec, payload_path in payloads:
        entry: dict[str, str] = {
            "name": payload_path.name,
            "format": "pickle",
            "object_kind": spec.object_kind,
            "description": spec.description,
            "sha256": sha256_file(payload_path),
        }
        if payload_path.suffix == ".gz":
            entry["compression"] = "gzip"
        if spec.dependency_notes:
            entry["dependency_notes"] = spec.dependency_notes
        files.append(entry)

    return {
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_machine": source_machine,
        "producer": producer,
        "python_version": platform.python_version(),
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "files": files,
    }


def write_manifest(manifest: dict[str, object], manifest_path: Path) -> None:
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def run_tailscale_ssh(
    args: argparse.Namespace,
    remote_command: str,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    command = ["tailscale", "ssh", args.ssh_target, remote_command]
    return subprocess.run(command, check=True, text=True, capture_output=capture_output)


def run_ssh_command(
    args: argparse.Namespace,
    remote_command: str,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
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
        "Unable to establish remote upload access before starting the transfer.\n"
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


def cleanup_remote_partial_upload(
    args: argparse.Namespace,
    remote_temp_path: PurePosixPath,
    *,
    transport: str,
) -> None:
    try:
        run_remote_command(
            args,
            f"rm -f {shell_quote(remote_temp_path.as_posix())}",
            transport=transport,
            capture_output=True,
        )
    except subprocess.CalledProcessError as error:
        detail = summarize_called_process_error(error)
        print(f"Warning: unable to delete remote partial upload at {remote_temp_path}: {detail}")
    else:
        print(f"Deleted remote partial upload: {remote_temp_path}")


def upload_file_via_ssh_scp(
    args: argparse.Namespace,
    local_path: Path,
    remote_path: PurePosixPath,
) -> None:
    remote_dir = remote_path.parent
    remote_temp_path = PurePosixPath(f"{remote_path}.partial")

    print(f"Ensuring remote directory exists: {remote_dir}")
    run_ssh_command(args, f"mkdir -p {shell_quote(remote_dir.as_posix())}")

    print(f"Uploading file to {args.ssh_target}:{remote_path} via scp")
    remote_spec = f"{args.ssh_target}:{remote_temp_path.as_posix()}"
    upload_command = ["scp", *SSH_COMMON_OPTIONS, str(local_path), remote_spec]
    try:
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
    except Exception:
        cleanup_remote_partial_upload(args, remote_temp_path, transport="ssh-scp")
        raise


def upload_file_via_tailscale_ssh(
    args: argparse.Namespace,
    local_path: Path,
    remote_path: PurePosixPath,
) -> None:
    remote_dir = remote_path.parent
    remote_temp_path = PurePosixPath(f"{remote_path}.partial")

    print(f"Ensuring remote directory exists: {remote_dir}")
    run_tailscale_ssh(args, f"mkdir -p {shell_quote(remote_dir.as_posix())}")

    print(f"Uploading file to {args.ssh_target}:{remote_path}")
    upload_command = [
        "tailscale",
        "ssh",
        args.ssh_target,
        f"cat > {shell_quote(remote_temp_path.as_posix())}",
    ]

    bytes_sent = 0
    total_size = local_path.stat().st_size
    try:
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
    except Exception:
        cleanup_remote_partial_upload(args, remote_temp_path, transport="tailscale-ssh")
        raise


def upload_file(
    args: argparse.Namespace,
    local_path: Path,
    remote_path: PurePosixPath,
    *,
    transport: str,
) -> None:
    if transport == "ssh-scp":
        upload_file_via_ssh_scp(args, local_path, remote_path)
        return
    if transport == "tailscale-ssh":
        upload_file_via_tailscale_ssh(args, local_path, remote_path)
        return
    raise ValueError(f"Unsupported upload transport: {transport}")


def prepare_remote_batch_dir(
    args: argparse.Namespace,
    remote_batch_dir: PurePosixPath,
    remote_temp_batch_dir: PurePosixPath,
    *,
    transport: str,
) -> None:
    remote_parent = remote_batch_dir.parent
    command = (
        f"mkdir -p {shell_quote(remote_parent.as_posix())} "
        f"&& test ! -e {shell_quote(remote_batch_dir.as_posix())} "
        f"&& rm -rf {shell_quote(remote_temp_batch_dir.as_posix())} "
        f"&& mkdir -p {shell_quote(remote_temp_batch_dir.as_posix())}"
    )
    run_remote_command(args, command, transport=transport)
    print(f"Prepared remote batch directory: {remote_temp_batch_dir}")


def finalize_remote_batch_dir(
    args: argparse.Namespace,
    remote_batch_dir: PurePosixPath,
    remote_temp_batch_dir: PurePosixPath,
    *,
    transport: str,
) -> None:
    command = (
        f"test -d {shell_quote(remote_temp_batch_dir.as_posix())} "
        f"&& mv {shell_quote(remote_temp_batch_dir.as_posix())} {shell_quote(remote_batch_dir.as_posix())}"
    )
    run_remote_command(args, command, transport=transport)
    print(f"Remote batch finalized: {remote_batch_dir}")


def cleanup_remote_batch_dir(
    args: argparse.Namespace,
    remote_temp_batch_dir: PurePosixPath,
    *,
    transport: str,
) -> None:
    try:
        run_remote_command(
            args,
            f"rm -rf {shell_quote(remote_temp_batch_dir.as_posix())}",
            transport=transport,
            capture_output=True,
        )
    except subprocess.CalledProcessError as error:
        detail = summarize_called_process_error(error)
        print(f"Warning: unable to delete remote temp batch directory {remote_temp_batch_dir}: {detail}")
    else:
        print(f"Deleted remote temp batch directory: {remote_temp_batch_dir}")


def disconnect_vpn() -> None:
    subprocess.run(
        r'cd /d "C:\Program Files\NordVPN" && nordvpn -d',
        shell=True,
        check=True,
    )
    print("VPN disconnected")


def connect_vpn() -> None:
    subprocess.run(
        r'cd /d "C:\Program Files\NordVPN" && nordvpn -c',
        shell=True,
        check=True,
    )
    print("VPN reconnected")


def transfer_batch(
    args: argparse.Namespace,
    payloads: list[tuple[PayloadSpec, Path]],
    manifest_path: Path,
    batch_id: str,
    *,
    transport: str,
) -> None:
    remote_root = PurePosixPath(args.remote_dir)
    remote_batch_dir = remote_root / batch_id
    remote_temp_batch_dir = PurePosixPath(f"{remote_batch_dir}.partial")

    prepare_remote_batch_dir(
        args,
        remote_batch_dir,
        remote_temp_batch_dir,
        transport=transport,
    )

    batch_finalized = False
    try:
        for _, payload_path in payloads:
            remote_payload_path = remote_temp_batch_dir / payload_path.name
            upload_file(args, payload_path, remote_payload_path, transport=transport)

        upload_file(
            args,
            manifest_path,
            remote_temp_batch_dir / DEFAULT_MANIFEST_NAME,
            transport=transport,
        )
        finalize_remote_batch_dir(
            args,
            remote_batch_dir,
            remote_temp_batch_dir,
            transport=transport,
        )
        batch_finalized = True
    finally:
        if not batch_finalized:
            cleanup_remote_batch_dir(args, remote_temp_batch_dir, transport=transport)


def main() -> None:
    vpn_disconnected = False

    try:
        disconnect_vpn()
        vpn_disconnected = True
        time.sleep(10)

        args = parse_args()
        batch_id = resolve_batch_id(args.batch_id)
        payloads = resolve_payload_paths(args.source_dir)

        print(f"Local source directory: {args.source_dir}")
        print(f"Payload count: {len(payloads)}")
        print(f"Remote batch target: {args.ssh_target}:{PurePosixPath(args.remote_dir) / batch_id}")

        ensure_upload_tools_available(args)
        upload_transport = select_upload_transport(args, PurePosixPath(args.remote_dir))
        print(f"Using upload transport: {upload_transport}")

        manifest = build_manifest(
            batch_id=batch_id,
            producer=args.producer,
            source_machine=args.source_machine,
            payloads=payloads,
        )

        with tempfile.TemporaryDirectory(prefix=f"pkl_to_openclaw_{batch_id}_") as temp_dir:
            manifest_path = Path(temp_dir) / DEFAULT_MANIFEST_NAME
            write_manifest(manifest, manifest_path)
            transfer_batch(
                args,
                payloads,
                manifest_path,
                batch_id,
                transport=upload_transport,
            )

        print("Pickle batch transfer complete.")
    finally:
        encountered_error = sys.exc_info()[0] is not None
        if vpn_disconnected:
            try:
                connect_vpn()
            except Exception as error:
                if encountered_error:
                    print(f"Warning: unable to reconnect VPN: {error}")
                else:
                    raise


if __name__ == "__main__":
    main()
