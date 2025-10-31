"""
manage_workflow_bundles.py

Upload workflow bundle ZIP files to S3 using explicit prefix mappings.

Each ZIP must match the pattern: nf-core-<workflow>_<version>.zip
The workflow name (<workflow>) is used to look up its S3 prefix from --map.

Example:
  python manage_workflow_bundles.py \
      --bucket my-bucket \
      --map mag=/mag \
      --map metatdenovo=/metatdenovo \
      nf-core-mag_1.0.0.zip nf-core-metatdenovo_1.2.0.zip

Result:
  s3://my-bucket/mag/nf-core-mag_1.0.0.zip
  s3://my-bucket/metatdenovo/nf-core-metatdenovo_1.2.0.zip
"""
import argparse
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.s3.transfer import TransferConfig


# ---------- Helpers ----------

def human_size(n: int) -> str:
    """Convert file size to human-readable units."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def parse_mappings(maps: List[str]) -> Dict[str, str]:
    """Parse --map entries like workflow=/prefix"""
    out: Dict[str, str] = {}
    for item in maps or []:
        if "=" not in item:
            raise SystemExit(f"Invalid --map '{item}'. Expected format: name=/prefix")
        name, prefix = item.split("=", 1)
        name = name.strip().lower()
        norm = prefix.strip().strip("/")
        out[name] = norm
    return out


def validate_inputs(files: List[str]) -> List[Path]:
    """Ensure all inputs exist and are .zip files."""
    paths: List[Path] = []
    errors: List[str] = []
    for f in files:
        p = Path(f)
        if not p.exists():
            errors.append(f"Not found: {f}")
            continue
        if not p.is_file():
            errors.append(f"Not a regular file: {f}")
            continue
        if p.suffix.lower() != ".zip":
            errors.append(f"Not a .zip file: {f}")
            continue
        paths.append(p)
    if errors:
        msg = "\n".join(errors)
        raise SystemExit(f"Input validation failed:\n{msg}")
    return paths


def extract_workflow_name(filename: str) -> str:
    """
    Extract workflow name from filenames like:
      nf-core-mag_1.0.0.zip  -> 'mag'
      nf-core-metatdenovo_1.2.0.zip -> 'metatdenovo'
    """
    match = re.match(r"^nf-core-([A-Za-z0-9_-]+)_", filename)
    if not match:
        raise ValueError(f"Filename '{filename}' does not match expected pattern 'nf-core-<workflow>_<version>.zip'")
    return match.group(1).lower()


def upload_with_retries(s3_client, file_path: Path, bucket: str, key: str,
                        max_attempts: int = 5, base_delay: float = 1.0) -> None:
    """Upload file to S3 with exponential backoff retries."""
    config = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,  # 64 MB
        max_concurrency=8,
        multipart_chunksize=64 * 1024 * 1024,
        use_threads=True,
    )
    attempt = 0
    while True:
        attempt += 1
        try:
            s3_client.upload_file(
                Filename=str(file_path),
                Bucket=bucket,
                Key=key,
                ExtraArgs={"ContentType": "application/zip"},
                Config=config,
            )
            return
        except (ClientError, BotoCoreError) as e:
            if attempt >= max_attempts:
                raise
            sleep_for = base_delay * (2 ** (attempt - 1))
            print(f"[WARN] Upload failed for {file_path.name} (attempt {attempt}/{max_attempts}): {e}")
            print(f"       Retrying in {sleep_for:.1f}s...")
            time.sleep(sleep_for)


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(
        description="Upload nf-core workflow bundle ZIPs to S3 using explicit prefix mappings."
    )
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument(
        "--map",
        action="append",
        default=[],
        help="Mapping 'workflow=/prefix'. Example: --map mag=/mag --map metatdenovo=/metatdenovo",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show uploads but don't perform them")
    parser.add_argument("zips", nargs="+", help="One or more nf-core-<workflow>_<version>.zip bundle files to upload")

    args = parser.parse_args()
    paths = validate_inputs(args.zips)
    mapping = parse_mappings(args.map)

    if not mapping:
        sys.exit("Error: At least one --map entry is required (e.g. --map mag=/mag)")

    s3 = boto3.client("s3")
    successes: List[Tuple[Path, str]] = []
    failures: List[Tuple[Path, str]] = []

    # Dry-run preview
    if args.dry_run:
        print("[DRY RUN] No uploads will be performed.")
        for p in paths:
            try:
                wf_name = extract_workflow_name(p.name)
                prefix = mapping.get(wf_name)
                if not prefix:
                    print(f"[SKIP] No mapping found for workflow '{wf_name}' — skipping {p}")
                    continue
                key = f"{prefix}/{p.name}".lstrip("/")
                print(f"Would upload: {p} -> s3://{args.bucket}/{key} ({human_size(p.stat().st_size)})")
            except ValueError as e:
                print(f"[SKIP] {e}")
        return

    # Perform uploads
    for p in paths:
        try:
            wf_name = extract_workflow_name(p.name)
        except ValueError as e:
            print(f"[ERROR] {e}")
            failures.append((p, "Invalid filename"))
            continue

        prefix = mapping.get(wf_name)
        if not prefix:
            print(f"[ERROR] No mapping found for workflow '{wf_name}' (from file {p.name}). Skipping.")
            failures.append((p, "No mapping"))
            continue

        key = f"{prefix}/{p.name}".lstrip("/")
        size = human_size(p.stat().st_size)
        print(f"Uploading: {p} -> s3://{args.bucket}/{key} ({size}) ...")
        try:
            upload_with_retries(s3, p, args.bucket, key)
            print(f"[OK] {p.name} uploaded to s3://{args.bucket}/{key}")
            successes.append((p, key))
        except Exception as e:
            print(f"[ERROR] Failed to upload {p}: {e}")
            failures.append((p, str(e)))

    # Summary
    print("\n=== Upload Summary ===")
    if successes:
        print("Successful uploads:")
        for p, key in successes:
            print(f"  - {p.name} -> s3://{args.bucket}/{key}")
    if failures:
        print("\nFailed uploads:")
        for p, err in failures:
            print(f"  - {p.name}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
