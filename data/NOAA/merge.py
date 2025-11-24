#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Auto-merge state-level weekly/monthly selected-station files into one US summary.

Input directory structure (default --indir ghcnd_out_rep):
  ghcnd_out_rep/
    FIPS_01/weekly_selected_station.csv    (optional)
    FIPS_01/monthly_selected_station.csv   (optional, preferred)
    ...
    FIPS_56/...

Behavior:
- For each FIPS, prefer monthly_selected_station.csv; else fall back to weekly_selected_station.csv.
- Normalize to a common schema:
    * period_start: datetime (week_start or month_start normalized)
    * frequency   : 'monthly' or 'weekly'
    * fips, state : added
    * variable columns are passed through unchanged (e.g., TAVG_C, PRCP_mm)
- Merge all states into a single CSV.
- If all rows end up same frequency, filename suffix is specialized accordingly; otherwise generic.

Options:
  --indir         : base directory that contains FIPS_* subfolders
  --outfile       : output filename (auto-adjusted suffix if not absolute and frequency uniform)
  --states        : comma-separated FIPS list (default: 50 states only, DC/territories excluded)
  --long          : also write long/tidy version as *_long.csv
  --require_all   : error out if any state's file is missing
  --sort          : sort rows by (period_start, fips) or (period_start, frequency, fips)
  --strict_same_freq : require that all inputs share the same frequency (weekly or monthly)

Examples:
  python merge_us_auto.py --indir ghcnd_out_rep --long --sort
  python merge_us_auto.py --indir ghcnd_out_rep --strict_same_freq --require_all
"""

import os
import sys
import argparse
import pandas as pd
from typing import List, Tuple

ALL_STATE_FIPS_50 = [
    "01","02","04","05","06","08","09","10","12","13",
    "15","16","17","18","19","20","21","22","23","24",
    "25","26","27","28","29","30","31","32","33","34",
    "35","36","37","38","39","40","41","42","44","45",
    "46","47","48","49","50","51","53","54","55","56"
]

FIPS_TO_STATE = {
    "01":"Alabama","02":"Alaska","04":"Arizona","05":"Arkansas","06":"California",
    "08":"Colorado","09":"Connecticut","10":"Delaware","12":"Florida","13":"Georgia",
    "15":"Hawaii","16":"Idaho","17":"Illinois","18":"Indiana","19":"Iowa",
    "20":"Kansas","21":"Kentucky","22":"Louisiana","23":"Maine","24":"Maryland",
    "25":"Massachusetts","26":"Michigan","27":"Minnesota","28":"Mississippi","29":"Missouri",
    "30":"Montana","31":"Nebraska","32":"Nevada","33":"New Hampshire","34":"New Jersey",
    "35":"New Mexico","36":"New York","37":"North Carolina","38":"North Dakota","39":"Ohio",
    "40":"Oklahoma","41":"Oregon","42":"Pennsylvania","44":"Rhode Island","45":"South Carolina",
    "46":"South Dakota","47":"Tennessee","48":"Texas","49":"Utah","50":"Vermont",
    "51":"Virginia","53":"Washington","54":"West Virginia","55":"Wisconsin","56":"Wyoming"
}

def parse_args():
    ap = argparse.ArgumentParser(description="Auto-merge weekly/monthly state CSVs into a US summary.")
    ap.add_argument("--indir", default="ghcnd_out_rep", help="Base dir containing FIPS_XX subfolders")
    ap.add_argument("--outfile", default="GHCND_US_period_summary.csv",
                    help="Output CSV filename (auto-suffixed if uniform frequency and not absolute)")
    ap.add_argument("--states", default=",".join(ALL_STATE_FIPS_50),
                    help="Comma-separated 2-digit FIPS list (default: 50 states only)")
    ap.add_argument("--long", action="store_true", help="Also write tidy/long version as *_long.csv")
    ap.add_argument("--require_all", action="store_true", help="Error out if any state's file is missing")
    ap.add_argument("--sort", action="store_true", help="Sort rows in the merged output")
    ap.add_argument("--strict_same_freq", action="store_true",
                    help="Require all inputs to share the same frequency (weekly OR monthly)")
    return ap.parse_args()

def read_one_state(indir: str, fips: str) -> Tuple[pd.DataFrame, str]:
    """Return (df, frequency) for a FIPS, or (empty, '') if not found."""
    base = os.path.join(indir, f"FIPS_{fips}")
    mo = os.path.join(base, "monthly_selected_station.csv")
    wk = os.path.join(base, "weekly_selected_station.csv")

    path, freq = (mo, "monthly") if os.path.exists(mo) else ((wk, "weekly") if os.path.exists(wk) else ("",""))
    if not path:
        return pd.DataFrame(), ""

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}", file=sys.stderr)
        return pd.DataFrame(), ""

    if df.empty:
        return pd.DataFrame(), ""

    # normalize period column
    if freq == "monthly":
        if "month_start" not in df.columns:
            print(f"[WARN] month_start missing in {path}; skipping.", file=sys.stderr)
            return pd.DataFrame(), ""
        df = df.rename(columns={"month_start": "period_start"})
    else:
        if "week_start" not in df.columns:
            print(f"[WARN] week_start missing in {path}; skipping.", file=sys.stderr)
            return pd.DataFrame(), ""
        df = df.rename(columns={"week_start": "period_start"})

    # ensure datetime
    df["period_start"] = pd.to_datetime(df["period_start"], errors="coerce")
    df.insert(0, "frequency", freq)
    df.insert(0, "state", FIPS_TO_STATE.get(fips, fips))
    df.insert(0, "fips", fips)
    return df, freq

def main():
    args = parse_args()
    states: List[str] = [s.strip() for s in args.states.split(",") if s.strip()]

    frames = []
    union_vars = set()
    missing = []
    freqs_seen = set()

    for fips in states:
        df, freq = read_one_state(args.indir, fips)
        if df.empty:
            missing.append(fips)
            continue

        # track variable columns (exclude keys)
        keys = {"period_start", "fips", "state", "frequency"}
        union_vars.update([c for c in df.columns if c not in keys])
        frames.append(df)
        freqs_seen.add(freq)

    if missing:
        msg = "[INFO] Missing state files for FIPS: " + ", ".join(missing)
        if args.require_all:
            print(msg, file=sys.stderr)
            sys.exit(2)
        else:
            print(msg)

    if not frames:
        print("❌ No inputs found. Nothing to merge.", file=sys.stderr)
        sys.exit(1)

    # strict frequency check (optional)
    if args.strict_same_freq and len(freqs_seen) > 1:
        print(f"❌ Mixed frequencies found: {sorted(freqs_seen)}; use weekly OR monthly only, or omit --strict_same_freq.",
              file=sys.stderr)
        sys.exit(3)

    # union of variables across all states (ensure consistent columns)
    var_cols = sorted(union_vars)
    ordered_cols = ["period_start", "frequency", "fips", "state"] + var_cols

    normed = []
    for df in frames:
        for c in var_cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df.reindex(columns=ordered_cols)
        normed.append(df)

    merged = pd.concat(normed, ignore_index=True)

    # optional sort
    if args.sort:
        if len(freqs_seen) > 1:
            merged = merged.sort_values(by=["period_start", "frequency", "fips"]).reset_index(drop=True)
        else:
            merged = merged.sort_values(by=["period_start", "fips"]).reset_index(drop=True)

    # choose outfile path
    out_path = args.outfile if os.path.isabs(args.outfile) else os.path.join(args.indir, args.outfile)

    # if uniform frequency and outfile is generic, auto-suffix for convenience
    if not os.path.isabs(args.outfile) and os.path.basename(args.outfile) == "GHCND_US_period_summary.csv":
        if freqs_seen == {"weekly"}:
            out_path = os.path.join(args.indir, "GHCND_US_weekly_summary.csv")
        elif freqs_seen == {"monthly"}:
            out_path = os.path.join(args.indir, "GHCND_US_monthly_summary.csv")

    merged.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[DONE] US-wide merged (wide) -> {out_path}")

    # optional long/tidy
    if args.long:
        long_df = merged.melt(
            id_vars=["period_start", "frequency", "fips", "state"],
            value_vars=var_cols,
            var_name="variable", value_name="value"
        )
        if args.sort:
            if len(freqs_seen) > 1:
                long_df = long_df.sort_values(by=["period_start", "variable", "frequency", "fips"]).reset_index(drop=True)
            else:
                long_df = long_df.sort_values(by=["period_start", "variable", "fips"]).reset_index(drop=True)

        long_path = out_path.replace(".csv", "_long.csv")
        long_df.to_csv(long_path, index=False, encoding="utf-8")
        print(f"[DONE] US-wide merged (long/tidy) -> {long_path}")

if __name__ == "__main__":
    main()
