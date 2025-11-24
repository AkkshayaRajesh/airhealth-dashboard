# NOAA GHCND Representative-Station Weather Dataset Pipeline



This repository provides a fully reproducible, end-to-end pipeline for downloading, processing, and aggregating NOAA GHCND (Global Historical Climatology Network – Daily) weather observations across all U.S. states.

The pipeline uses a representative station per state, aggregates daily data into weekly or monthly summaries, and produces a merged nationwide dataset ready for analysis or visualization.



All steps are automated using two main scripts:



* rep\_station.py – Selects representative stations, downloads daily data, and aggregates into weekly/monthly summaries.
* merge.py – Merges all state-level processed outputs into one U.S. summary dataset.





##### Key Features



* Automatically selects the best representative climate station per state (complete data, long coverage, USW preference optional).
* Downloads daily weather records from 2002–2025 using the NOAA CDO API.
* Supports weekly (W-MON) or monthly aggregation.
* Automatically handles:



* API rate limits
* Pagination
* Data gaps
* Retry with exponential backoff



* Produces:



* Per-state weekly/monthly summaries
* A single nationwide merged dataset (wide + tidy/long formats)
* Designed for large-scale environmental / climatological analysis.



### 1\. Requirements



Install the required Python packages:



pip install pandas requests



(If using a requirements.txt, run pip install -r requirements.txt.)



### 2\. Get Your NOAA API Token

#### 1\. Create a NOAA account



Go to:

https://www.ncdc.noaa.gov/cdo-web/token



Log in or create a free account.



#### 2\. Generate your token



You will receive a long alphanumeric API token.

If the token is not set, the script will prompt you to enter it manually.



### 3\. Step 1 — Download \& Aggregate State-Level Data



Run the script:

rep\_station.py

(Uploaded code: /mnt/data/rep\_station.py)



This script:



✔ Lists all GHCND stations for each state

✔ Selects one representative station using rules:



* datacoverage ≈ 1.0
* else maximum coverage
* tie-break: longest span, earliest start date, USW priority (optional)



✔ Fetches daily data for selected station (2002–2025)

✔ Aggregates to weekly or monthly periods

✔ Saves outputs into per-state folders



Run the script

python rep\_station.py --outdir ghcnd\_out\_rep --freq weekly --vars AWND,PRCP,SNOW,SNWD,TAVG,TMAX,TMIN



#### State Output Example



ghcnd\_out\_rep/

&nbsp;└── FIPS\_06/

&nbsp;     ├── stations\_meta.csv

&nbsp;     ├── selected\_station.csv

&nbsp;     ├── parts/

&nbsp;     │     ├── GHCND\_USW00023174\_2005.csv

&nbsp;     │     └── ...

&nbsp;     ├── weekly\_selected\_station.csv

&nbsp;     └── monthly\_selected\_station.csv   (if freq=monthly)



### 4\. Step 2 — Merge All States into One Nationwide Dataset



Run the script:

merge.py

(Uploaded code: /mnt/data/merge.py)



This script:



✔ Reads aggregated files from each state

✔ Automatically detects weekly vs. monthly frequency

✔ Normalizes column names (week\_start/month\_start → period\_start)

✔ Adds FIPS and state name

✔ Merges all states into one U.S. dataset

✔ Optionally produces long/tidy format



Run the script

python merge.py --indir ghcnd\_out\_rep --sort





### 5\. Folder Structure



project/

&nbsp;├── rep\_station.py

&nbsp;├── merge.py

&nbsp;├── README.md

&nbsp;└── ghcnd\_out\_rep/

&nbsp;      ├── FIPS\_01/

&nbsp;      ├── FIPS\_02/

&nbsp;      ├── ...

&nbsp;      ├── GHCND\_US\_weekly\_summary.csv

&nbsp;      └── GHCND\_US\_weekly\_summary\_long.csv







