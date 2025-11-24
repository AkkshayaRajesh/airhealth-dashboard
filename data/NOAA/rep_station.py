#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Per-state representative station -> fetch daily (2002-2025) -> weekly or monthly aggregation

- Variables: AWND, PRCP, SNOW, SNWD, TAVG, TMAX, TMIN
- Pick ONE representative station per FIPS:
  1) prefer datacoverage≈1 (>= 0.999999)
  2) else max datacoverage
  3) tie-break: longest span_days -> earliest mindate -> id asc
  4) optional: --prefer_usw to prefer USW stations before selection
- Fetch daily by year, cache (parts/), then weekly or monthly aggregate:
  - PRCP,SNOW: weekly/monthly SUM
  - others: weekly/monthly MEAN
"""

import os
import sys
import time
import argparse
import json
import random
from typing import List, Optional, Tuple
from datetime import date
import pandas as pd
import requests

BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2"
PAGE_LIMIT = 1000
TIMEOUT = 40
RETRY = 3
PAUSE = 0.2

DEFAULT_VARS = ["AWND","PRCP","SNOW","SNWD","TAVG","TMAX","TMIN"]


ALL_STATE_FIPS_50 = [
    "01","02","04","05","06","08","09","10","12","13",
    "15","16","17","18","19","20","21","22","23","24",
    "25","26","27","28","29","30","31","32","33","34",
    "35","36","37","38","39","40","41","42","44","45",
    "46","47","48","49","50","51","53","54","55","56"
]

def ensure_dir(p: str):
    if not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)

def already_done(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0

def backoff_sleep(attempt: int):
    delay = min(30.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
    time.sleep(delay)

def req_json(path: str, token: str, params: dict,
             *, timeout: int = TIMEOUT, max_retry: int = RETRY) -> dict:
    headers = {"token": token}
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retry + 1):
        try:
            r = requests.get(f"{BASE}/{path}", headers=headers, params=params, timeout=timeout)
            if r.status_code == 429:
                backoff_sleep(attempt); continue
            if r.status_code in (500,502,503,504):
                last_exc = requests.HTTPError(f"{r.status_code} {r.reason}")
                backoff_sleep(attempt); continue
            r.raise_for_status()
            if r.status_code == 204 or not r.text.strip():
                return {}
            try:
                return r.json()
            except ValueError as e:
                last_exc = e
                if attempt < max_retry:
                    backoff_sleep(attempt); continue
                raise
        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as e:
            last_exc = e
            if attempt < max_retry:
                backoff_sleep(attempt); continue
            raise last_exc
        except BaseException:
            raise
    if isinstance(last_exc, BaseException):
        raise last_exc
    raise RuntimeError(f"Unknown error in req_json for path={path!r}, params={params!r}")

def year_slices(mind: date, maxd: date) -> List[Tuple[date, date]]:
    """Intersect with [2002-01-01, 2025-12-31] and split by year."""
    if not mind or not maxd or mind > maxd:
        return []
    start_limit = date(2002,1,1)
    end_limit   = date(2025,12,31)
    start = max(mind, start_limit)
    end   = min(maxd, end_limit)
    if start > end:
        return []
    slices = []
    for y in range(start.year, end.year + 1):
        ys = date(y,1,1)
        ye = date(y,12,31)
        if y == start.year: ys = start
        if y == end.year:   ye = end
        slices.append((ys, ye))
    return slices

def list_stations_for_state(token: str, fips: str, datatypeids: List[str]) -> pd.DataFrame:
    rows = []; offset = 1
    base = {"datasetid":"GHCND","locationid":f"FIPS:{fips}","limit":PAGE_LIMIT,"datatypeid":datatypeids}
    while True:
        js = req_json("stations", token, {**base,"offset":offset})
        res = js.get("results", []) if js else []
        if not res: break
        rows.extend(res); offset += PAGE_LIMIT; time.sleep(PAUSE)
    if not rows:
        return pd.DataFrame(columns=["id","name","latitude","longitude","elevation","mindate","maxdate","datacoverage"])
    df = pd.DataFrame(rows).drop_duplicates(subset=["id"]).reset_index(drop=True)
    for c in ("mindate","maxdate"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df

def pick_representative_station(stations: pd.DataFrame, prefer_usw: bool=False) -> pd.DataFrame:
    
    if stations.empty:
        return stations

    st = stations.copy()
    st["datacoverage"] = pd.to_numeric(st.get("datacoverage"), errors="coerce")
    st["mindate"] = pd.to_datetime(st.get("mindate"), errors="coerce")
    st["maxdate"] = pd.to_datetime(st.get("maxdate"), errors="coerce")
    st["span_days"] = (st["maxdate"] - st["mindate"]).dt.days.fillna(-1).astype(int)
    st["is_usw"] = st["id"].astype(str).str.startswith("GHCND:USW").astype(int)  # USW=1, 그외=0

    cand = st
    if prefer_usw:
        mask_usw = cand["is_usw"] == 1
        if mask_usw.any():
            cand = cand.loc[mask_usw].copy()

    tol = 1e-6

    def _select(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return (df.sort_values(by=["is_usw", "span_days", "mindate", "id"],
                               ascending=[False,    False,      True,     True])
                  .head(1).reset_index(drop=True))

    # 1) coverage ≈ 1
    cov1 = cand.loc[cand["datacoverage"] >= 1.0 - tol]
    if not cov1.empty:
        return _select(cov1)

    # 2) max coverage (tie-break)
    max_cov = cand["datacoverage"].max(skipna=True)
    if pd.isna(max_cov):
        
        return _select(cand)

    best = cand.loc[cand["datacoverage"].between(max_cov - tol, max_cov + tol, inclusive="both")]
    return _select(best)


def fetch_station_year(token: str, station_id: str,
                       ys: date, ye: date,
                       datatypeids: List[str], units: str="standard") -> pd.DataFrame:
    rows = []; offset = 1
    base = {"datasetid":"GHCND","stationid":station_id,"startdate":ys.isoformat(),
            "enddate":ye.isoformat(),"units":units,"limit":PAGE_LIMIT,"datatypeid":datatypeids}
    while True:
        js = req_json("data", token, {**base,"offset":offset})
        res = js.get("results", []) if js else []
        if not res: break
        rows.extend(res); offset += PAGE_LIMIT; time.sleep(PAUSE)
    if not rows:
        return pd.DataFrame(columns=["date","datatype","station","attributes","value"])
    return pd.DataFrame(rows)

def daily_to_period_station(daily_df: pd.DataFrame,
                            wanted_vars: List[str],
                            freq: str = "weekly") -> pd.DataFrame:
    """
    Aggregate a single station's daily data to weekly or monthly:
      - PRCP, SNOW : sum over period
      - others     : mean over period
    Weekly uses Monday-based weeks (W-MON).
    Monthly uses calendar months (start-of-month label).
    """
    if daily_df.empty:
        return pd.DataFrame()

    d = daily_df.loc[daily_df["datatype"].isin(wanted_vars)].copy()
    if d.empty:
        return pd.DataFrame()

    d["date"] = pd.to_datetime(d["date"], errors="coerce")

    if freq == "weekly":
        # Monday-start weeks
        d["period_start"] = d["date"].dt.to_period("W-MON").apply(lambda p: p.start_time.normalize())
        label_col = "week_start"
    else:  # monthly
        # Calendar month
        d["period_start"] = d["date"].dt.to_period("M").apply(lambda p: p.start_time.normalize())
        label_col = "month_start"

    sum_vars  = {"PRCP", "SNOW"}
    mean_vars = set(wanted_vars) - sum_vars
    wide_sum, wide_mean = pd.DataFrame(), pd.DataFrame()

    if not d[d["datatype"].isin(sum_vars)].empty:
        g_sum = (d[d["datatype"].isin(sum_vars)]
                 .groupby(["period_start","datatype"], as_index=False)["value"].sum())
        wide_sum = g_sum.pivot(index="period_start", columns="datatype", values="value")

    if not d[d["datatype"].isin(mean_vars)].empty:
        g_mean = (d[d["datatype"].isin(mean_vars)]
                  .groupby(["period_start","datatype"], as_index=False)["value"].mean())
        wide_mean = g_mean.pivot(index="period_start", columns="datatype", values="value")

    if wide_sum.empty and wide_mean.empty:
        return pd.DataFrame()

    wide = wide_mean if wide_sum.empty else (wide_sum if wide_mean.empty else wide_mean.join(wide_sum, how="outer"))
    wide = wide.reset_index().rename(columns={"period_start": label_col})

    ordered = [label_col] + [v for v in wanted_vars if v in wide.columns]
    return wide.reindex(columns=ordered)


def main():
    ap = argparse.ArgumentParser(description="Per-state representative station -> daily -> weekly")
    ap.add_argument("--outdir", default="ghcnd_out_rep", help="Output directory")
    ap.add_argument("--vars", default=",".join(DEFAULT_VARS),
                    help="Comma-separated GHCND datatype IDs to fetch")
    ap.add_argument("--states", default=",".join(ALL_STATE_FIPS_50),
                    help="Comma-separated 2-digit FIPS (50 states only default)")
    ap.add_argument("--units", default="standard", choices=["standard","metric"], help="Units")
    ap.add_argument("--freq", default="weekly", choices=["weekly","monthly"],
                help="Aggregation frequency: weekly (W-MON) or monthly (calendar month)")
    ap.add_argument("--prefer_usw", action="store_true", help="Prefer USW stations when choosing representative")
    ap.add_argument("--save_raw", action="store_true", help="Save per-year raw CSVs under parts/")
    ap.add_argument("--resume", action="store_true", help="Skip fetching if per-year file already exists")
    args = ap.parse_args()

    token = os.getenv("NOAA_TOKEN") or input("Enter your NOAA API token: ").strip()
    if not token:
        print("❌ Token is required. Exiting."); sys.exit(1)

    outdir = args.outdir; ensure_dir(outdir)
    wanted_vars = [v.strip() for v in args.vars.split(",") if v.strip()]
    state_list = [s.strip() for s in args.states.split(",") if s.strip()]

    with open(os.path.join(outdir, "variables_selected.json"), "w", encoding="utf-8") as f:
        json.dump({"vars": wanted_vars}, f, ensure_ascii=False, indent=2)

    for fips in state_list:
        print(f"\n=== FIPS:{fips} | listing stations for vars={wanted_vars} ===")
        state_dir = os.path.join(outdir, f"FIPS_{fips}")
        ensure_dir(state_dir)
        parts_dir = os.path.join(state_dir, "parts")
        ensure_dir(parts_dir)

        # 1) stations meta
        try:
            stations = list_stations_for_state(token, fips, wanted_vars)
        except Exception as e:
            print(f"[WARN] stations list failed for FIPS:{fips}: {e}", file=sys.stderr)
            continue
        if stations.empty:
            print(f"[INFO] No stations for FIPS:{fips} with selected datatypes."); continue

        meta_path = os.path.join(state_dir, "stations_meta.csv")
        stations.to_csv(meta_path, index=False, encoding="utf-8")
        print(f"[INFO] {len(stations):,} stations saved -> {meta_path}")

        # 2) pick representative
        rep = pick_representative_station(stations, prefer_usw=args.prefer_usw)
        if rep.empty:
            print(f"[INFO] No representative station selected for FIPS:{fips}."); continue
        sel = rep.iloc[0]
        rep_path = os.path.join(state_dir, "selected_station.csv")
        rep.to_csv(rep_path, index=False, encoding="utf-8")
        print(f"[SELECT] {sel['id']} | {sel.get('name','')} | span={int(sel['span_days'])} days")

        # 3) fetch daily for that station (2002-2025)
        per_year_paths = []
        all_daily_parts = []
        mind = sel["mindate"].date() if hasattr(sel["mindate"], "date") else sel["mindate"]
        maxd = sel["maxdate"].date() if hasattr(sel["maxdate"], "date") else sel["maxdate"]
        for ys, ye in year_slices(mind, maxd):
            year = ys.year
            part_path = os.path.join(parts_dir, f"{str(sel['id']).replace(':','_')}_{year}.csv")
            if args.resume and already_done(part_path):
                per_year_paths.append(part_path); continue
            try:
                df = fetch_station_year(token, sel["id"], ys, ye, wanted_vars, units=args.units)
            except requests.HTTPError:
                continue
            except Exception as e:
                print(f"[WARN] fetch failed FIPS:{fips} {sel['id']} {year}: {e}")
                continue
            if df.empty:
                if args.save_raw:
                    df.to_csv(part_path, index=False, encoding="utf-8")
                    per_year_paths.append(part_path)
                continue
            if args.save_raw:
                df.to_csv(part_path, index=False, encoding="utf-8")
                per_year_paths.append(part_path)
            all_daily_parts.append(df)

        # merge daily
        if args.resume and args.save_raw and per_year_paths:
            daily_df = pd.concat([pd.read_csv(p) for p in per_year_paths if os.path.exists(p)], ignore_index=True)
        else:
            daily_df = pd.concat(all_daily_parts, ignore_index=True) if all_daily_parts else pd.DataFrame()

        if daily_df.empty:
            print(f"[INFO] No daily data for selected station in FIPS:{fips}."); continue

        # 4) weekly aggregation for the selected station
        agg = daily_to_period_station(daily_df, wanted_vars, freq=args.freq)
        if agg.empty:
            print(f"[INFO] {args.freq} aggregation empty for FIPS:{fips}."); continue

        # Optional: keep your unit suffix step if you added it
        # agg = add_unit_labels(agg, args.units, wanted_vars)

        # Pick filename based on freq
        if args.freq == "weekly":
            out_name = "weekly_selected_station.csv"
        else:
            out_name = "monthly_selected_station.csv"

        out_path = os.path.join(state_dir, out_name)
        agg.to_csv(out_path, index=False, encoding="utf-8")
        print(f"[DONE] FIPS:{fips} {args.freq} (representative station) -> {out_path}")

    print("\n[ALL DONE] Representative-station aggregation complete.")

if __name__ == "__main__":
    main()
