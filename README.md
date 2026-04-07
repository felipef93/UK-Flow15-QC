# 15_min_code_26

This package performs quality control (QC) checks on 15-minute river flow data, following the methodology described in [placeholder_doi].

The workflow is structured into two main processing steps:

- `run_station_workflow.py`: runs station-level QC checks on the raw 15-minute flow data, including basic checks, consistency checks, high-flow diagnostics, and rainfall-based tests. It outputs QC flags at each timestep and generates station-level summaries.
- `run_hydro_region_workflow.py`: performs hydro-region-based QC checks by analysing extreme events across neighbouring stations. It uses return period estimates derived from the station data to identify spatial inconsistencies and appends the corresponding hydro-region flags to the station outputs.

The package generates the following outputs in the `outputs/` directory:
- `<station_id>_qced.csv`: QCed station file containing the original time series together with all QC flags evaluated at each timestep.
- `qc_summary.csv`: summary table reporting the number of occurrences of each QC flag for every station.

## Code organisation

The code is organised around a `station` object in `code/station.py`. Each station can run four main groups of checks:

- **Basic QC**: negative values, unrealistically high values, relative spikes, absolute spikes, drops, shifts, and truncation.
- **Consistency checks**: comparison against NRFA daily, AMAX, and POT reference files when available.
- **High-flow QC**: GEV-based return periods, values above 6 standard deviations, values above a 1000-year return period threshold, and values above a multiple of the second-highest AMAX.
- **Rainfall QC**: rainfall-response checks based on high-return-period flow events and associated rainfall.

In a second instance a `hydro_region` object in `code/hydrology/hydro_region.py` groups these stations to identify nearby extrme events from the return period calculated in the previous workflow.

## Main scripts

### 1. `run_station_workflow.py`
Runs the station-level QC workflow for all sample 15-minute station files.

The script:

- reads all raw station files from `sample_stations/15_min`
- looks for matching NRFA support files in `sample_stations/NRFA`
- creates a `station` object for each station
- runs:
  - `basic_qc()`
  - `consistency_qc()`
  - `high_flows()`
  - `rainfall_qc()`
- writes one QCed station file per station to `outputs`
- writes a summary table to `outputs/qc_summary.csv`

### 2. `run_hydro_region_workflow.py`
Runs the hydro-region workflow.

The script:

- rereads the raw 15-minute station files from `sample_stations/15_min`
- creates station objects
- runs **only** `high_flows()` so that station return periods are available for hydro-region checks
- groups stations by the first three digits of the six-digit station identifier
- applies the hydro-region checks defined in `code/hydrology/hydro_region.py`
- appends the hydro-region outputs back into the existing station files in `outputs`
- updates `outputs/qc_summary.csv`

## Folder structure

The package is expected to run from the project root with this structure:

```text
15_min_code_26/
├── run_station_workflow.py          # Runs station-level QC on all 15-min flow files
├── run_hydro_region_workflow.py     # Runs hydro-region QC using station return periods
├── outputs/
├── sample_stations/
│   ├── 15_min/                      # 15-minute flow data (CSV)
│   │   └── <station_id>.csv         # e.g. 038001.csv (6-digit, zero-padded)
│   │                                # Columns: datetime, value
│   └── NRFA/
│       ├── AMAX/                    # Annual maxima series (CSV)
│       │   └── <station_id>.csv     # Columns include: datetime, value
│       ├── Daily/                   # Daily mean flow data (CSV)
│       │   └── <station_id>.csv     # Columns include: datetime, value
│       ├── POT/                     # Peaks-over-threshold series (CSV)
│       │   └── <station_id>.csv     # Columns include: datetime, value
│       └── Rainfall/                # Rainfall data (CSV)
│           └── <station_id>.csv     # Columns include: datetime, value
└── src/
    ├── station.py                   # Core station object: runs QC pipeline and writes outputs
    ├── basic_qc/
    │   ├── drops.py                 # Detects sudden drops in flow values
    │   ├── negatives.py             # Flags negative flow values
    │   ├── shifts.py                # Detects abrupt level/flow shifts
    │   ├── spikes.py                # Identifies relative and absolute spikes
    │   ├── truncated.py             # Detects truncation in time series
    │   └── unrealistically_high.py  # Flags unrealistically high values
    ├── consistency_checks/
    │   ├── auxiliary_functions.py   # Helper functions for NRFA comparisons
    │   ├── nrfa_amax.py             # Consistency checks against NRFA AMAX
    │   └── nrfa_daily.py            # Consistency checks against NRFA daily data
    ├── high_flows_qc/
    │   ├── above_std.py             # Flags extreme values using standard deviation thresholds
    │   ├── auxiliary_calculate_amax.py # Computes annual maxima (AMAX)
    │   ├── return_periods.py        # Fits GEV and computes return periods
    │   └── top_amax.py              # Identifies highest AMAX-related anomalies
    └── hydrology/
        ├── rainfall_checks.py       # Rainfall-flow consistency and event-based checks
        └── hydro_region.py          # Spatial QC using neighbouring stations (hydro-regions)
```

## Data conventions

### Station files

Each raw station file in `sample_stations/15_min` is expected to:

- be a `.csv`
- be named with a **six-digit, zero-padded station id**, for example `038001.csv`
- contain at least:
  - `datetime`
  - `value`

### NRFA support files

Where available, matching NRFA files should use the same six-digit, zero-padded filename and be placed in:

- `sample_stations/NRFA/AMAX`
- `sample_stations/NRFA/Daily`
- `sample_stations/NRFA/POT`
- `sample_stations/NRFA/Rainfall`

The station workflow looks for files with exactly the same stem as the station file.

## How to run

From the project root:

```bash
python run_station_workflow.py
python run_hydro_region_workflow.py
```

Run order matters:

1. `run_station_workflow.py` must be run first to generate the station QC outputs.
2. `run_hydro_region_workflow.py` then appends the hydro-region checks to those outputs.

## Outputs

### Station files

The station workflow writes one QCed file per station to `outputs/` using the naming convention:

```text
<station_id>_qced.csv
```

The final output columns are:

```text
datetime
value
return_period
resolution
negatives
high
rel_spike
abs_spike
drops
shifts
truncation
high_truncation
nrfa_amax
nrfa_pot
nrfa_daily
above_6std
above_1000_rp
top_amax
rainfall_1_intensity
rainfall_2_ratio
hydro_region_2_ratio
hydro_region_1_intensity
```

### QC summary

A summary table is written to:

```text
outputs/qc_summary.csv
```

This contains one row per station and summarises the number of flagged time steps for each QC metric.


## Typical workflow

1. Place raw 15-minute station files in `sample_stations/15_min`.
2. Place any corresponding NRFA support files in the matching folders under `sample_stations/NRFA`.
3. Run the station workflow.
4. Inspect `outputs/qc_summary.csv` and the station `_qced.csv` files.
5. Run the hydro-region workflow.
6. Reinspect the outputs with the appended hydro-region columns.
