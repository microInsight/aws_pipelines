#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound

HEADER_BY_WORKFLOW = {
    "mag": ["sample", "fastq_1", "fastq_2"],
    "metatdenovo": ["sample", "fastq_1", "fastq_2"],
}

def infer_job_name(samples_dir: Path) -> str:
    return samples_dir.name

def find_pairs(samples_dir: Path):
    import re

    seen = set()
    files = [p.name for p in samples_dir.glob("*.fastq.gz") if p.is_file()]
    pairs = {}
    pat_R = re.compile(r"^(?P<sample>.+)_(?P<direction>R1)_([0-9]{3})\.fastq\.gz$")
    pat_F = re.compile(r"^(?P<sample>.+)_(?P<direction>R2)_([0-9]{3})\.fastq\.gz$")

    match_list = []
    for fname in files:
        matched_r = pat_R.match(fname)
        matched_f = pat_F.match(fname)
        for matched in [matched_r, matched_f]:
            if matched:
                sample = matched.group("sample")
                direction = matched.group("direction")
                if sample not in pairs:
                    pairs[sample] = {"sample": sample, "fastq_1": None, "fastq_2": None}
                if direction == "R1":
                    pairs[sample]["fastq_1"] = fname
                else:
                    pairs[sample]["fastq_2"] = fname
                seen.add(sample)
        match_list.append(pairs)
    return pairs


def generate_samplesheet(samples_dir: Path, workflow: str, pairs: dict, input_bucket: str, job_name: str):
    wf = workflow.lower()
    header = HEADER_BY_WORKFLOW.get(wf, ["sample", "fastq_1", "fastq_2"])
    out_path = samples_dir / f"samplesheet_{wf}.csv"
    with out_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for sample, (r1, r2) in sorted(pairs.items()):
            s3_r1 = f"s3://{input_bucket}/{job_name}/{r1}"
            s3_r2 = f"s3://{input_bucket}/{job_name}/{r2}"
            if wf == "ampliseq":
                writer.writerow([sample, s3_r1, s3_r2])
            elif wf == "rnaseq":
                writer.writerow([sample, s3_r1, s3_r2, "auto"])
            else:
                writer.writerow([sample, s3_r1, s3_r2])
    return out_path

def create_manifest(samples_dir: Path, job_name: str, workflows: list):
    manifest = {
        "job_name": job_name,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "workflows": workflows,
    }
    for wf in workflows:
        wf_key = f"samplesheet_{wf}.csv"
        manifest[wf_key] = str(Path(wf_key))
    path = samples_dir / "run_manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    return path

def upload_files(session, bucket: str, job_name: str, samples_dir: Path, pairs: dict, workflows: list):
    s3 = session.client("s3")
    # FASTQs
    for sample, (r1, r2) in pairs.items():
        for fname in (r1, r2):
            s3.upload_file(str(samples_dir / fname), bucket, f"{job_name}/{fname}")
    # CSVs + manifest
    for wf in workflows:
        csv_path = samples_dir / f"samplesheet_{wf}.csv"
        s3.upload_file(str(csv_path), bucket, f"{job_name}/{csv_path.name}")
    manifest_path = samples_dir / "run_manifest.json"
    if manifest_path.exists():
        s3.upload_file(str(manifest_path), bucket, f"{job_name}/{manifest_path.name}")

def upload_extra_files(session, bucket: str, job_name: str, files: list):
    if not files:
        return
    import json as _json
    s3 = session.client("s3")
    for f in files:
        if not f:
            continue
        p = Path(f)
        if not p.exists():
            raise SystemExit(f"ERROR: parameter does not exist: {p}")
        if p.suffix.lower() == ".json":
            try:
                _ = _json.loads(p.read_text())
            except Exception as e:
                raise SystemExit(f"ERROR: invalid JSON for parameter file: {p} -> {e}")
        s3.upload_file(str(p), bucket, f"{job_name}/{p.name}")
        print(f"Uploaded parameter file to s3://{bucket}/{job_name}/{p.name}")

def main():
    ap = argparse.ArgumentParser(description="Generate samplesheets and upload FASTQs + metadata to S3.")
    ap.add_argument("--samples-dir", required=True, help="Directory containing *.fastq.gz files")
    ap.add_argument("--input-bucket", required=True, help="S3 bucket for uploads")
    ap.add_argument("--job-name", default=None, help="Job name used as S3 prefix (defaults to name of samples dir)")
    ap.add_argument("--run-id", default=None, help="(deprecated) Alias for --job-name")
    ap.add_argument("--workflows", nargs="+", default=["mag", "metatdenovo"],
                    help="Workflows to generate samplesheets for (e.g., mag metatdenovo rnaseq ampliseq)")
    ap.add_argument("--aws-profile", default=None, help="AWS profile name")
    ap.add_argument("--region", default=None, help="AWS region")
    ap.add_argument("--mag-params", default=None, help="Path to MAG parameters JSON file to upload")
    ap.add_argument("--metatdenovo-params", default=None, help="Path to MetaTDeNovo parameters JSON file to upload")
    args = ap.parse_args()

    samples_dir = Path(args.samples_dir).resolve()
    if not samples_dir.exists():
        raise SystemExit(f"ERROR: samples-dir does not exist: {samples_dir}")

    job_name = args.job_name or args.run_id or infer_job_name(samples_dir)
    workflows = [w.lower() for w in args.workflows]

    # AWS session
    try:
        if args.aws_profile:
            session = boto3.Session(profile_name=args.aws_profile, region_name=args.region)
        else:
            session = boto3.Session(region_name=args.region)
    except ProfileNotFound as e:
        raise SystemExit(f"ERROR: {e}")

    pairs = find_pairs(samples_dir)
    if not pairs:
        raise SystemExit("ERROR: No paired FASTQ files found (*.fastq.gz with _R1/_R2 or _1/_2).")

    created = []
    for wf in workflows:
        created.append(generate_samplesheet(samples_dir, wf, pairs, args.input_bucket, job_name))

    manifest_path = create_manifest(samples_dir, job_name, workflows)

    try:
        upload_files(session, args.input_bucket, job_name, samples_dir, pairs, workflows)
        upload_extra_files(session, args.input_bucket, job_name, [args.mag_params, args.metatdenovo_params])
    except (ClientError, NoCredentialsError) as e:
        raise SystemExit(f"ERROR uploading to S3: {e}")

    print(f"\nDone. Uploaded to s3://{args.input_bucket}/{job_name}/")
    print("Created files:")
    for p in created + [manifest_path]:
        print(f"  - {p.name}")
    if args.region:
        print("\nUseful console links:")
        print(f"  S3: https://s3.console.aws.amazon.com/s3/buckets/{args.input_bucket}?prefix={job_name}/&region={args.region}")
        print(f"  Step Functions: https://console.aws.amazon.com/states/home?region={args.region}#/statemachines")
        print(f"  HealthOmics: https://console.aws.amazon.com/omics/home?region={args.region}#/runs")

if __name__ == "__main__":
    main()
