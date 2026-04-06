# 15_min_code_26

This package runs quality control (QC) checks on 15-minute river flow data and produces station-level QC outputs together with a QC summary table. It also includes a second workflow for hydro-region checks that append regional-event flags back to the station outputs.

## What the package does

The code is organised around a `station` object in `code/station.py`. Each station can run four main groups of checks:

- **Basic QC**: negative values, unrealistically high values, relative spikes, absolute spikes, drops, shifts, and truncation.
- **Consistency checks**: comparison against NRFA daily, AMAX, and POT reference files when available.
- **High-flow QC**: GEV-based return periods, values above 6 standard deviations, values above a 1000-year return period threshold, and values above a multiple of the second-highest AMAX.
- **Rainfall QC**: rainfall-response checks based on high-return-period flow events and associated rainfall.

A second workflow groups stations by hydro-region and applies hydro-region checks using the station return periods already calculated by the high-flow routines.

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
- applies the hydro-region checks defined in `src/hydrology/hydro_region.py`
- appends the hydro-region outputs back into the existing station files in `outputs`
- updates `outputs/qc_summary.csv`

## Expected folder structure

The package is expected to run from the project root with this structure:

```text
15_min_code_26/
├── run_station_workflow.py
├── run_hydro_region_workflow.py
├── outputs/
├── sample_stations/
│   ├── 15_min/
│   └── NRFA/
│       ├── AMAX/
│       ├── Daily/
│       ├── POT/
│       └── Rainfall/
└── code/
    ├── station.py
    ├── basic_qc/
    ├── consistency_checks/
    ├── high_flows_qc/
    └── hydrology/
        ├── rainfall_checks.py
        └── hydro_region.py
```

## Input data conventions

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


## Important implementation notes

- Station identifiers are expected to remain six-digit, zero-padded strings throughout the workflow.
- `run_station_workflow.py` imports from `code/station.py`.
- `run_hydro_region_workflow.py` uses `code/hydrology/hydro_region.py` for the hydro-region logic.
- The hydro-region workflow does **not** rerun the full station QC; it reruns only the high-flow GEV calculations needed to derive return periods for the regional checks.

## Typical workflow

1. Place raw 15-minute station files in `sample_stations/15_min`.
2. Place any corresponding NRFA support files in the matching folders under `sample_stations/NRFA`.
3. Run the station workflow.
4. Inspect `outputs/qc_summary.csv` and the station `_qced.csv` files.
5. Run the hydro-region workflow.
6. Reinspect the outputs with the appended hydro-region columns.
