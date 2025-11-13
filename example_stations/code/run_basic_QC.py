#!/usr/bin/env python
import os
import argparse
from glob import glob
from multiprocessing import Pool, cpu_count
from pathlib import Path
import sys


def make_root_in_sys_path():
    """
    Ensure the repo root (the directory that contains `qc_code/`) is on sys.path.
    This script lives in: .../example_stations/code/run_basic_QC.py
    So the root is two levels up.
    """
    here = Path(__file__).resolve()
    root = here.parents[2]  # UK-Flow15-QC/
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def process_one(args):
    csv_path, output_dir = args
    from qc_code.station_qc import Station  # import inside so workers can import too
    try:
        st = Station(csv_path)
        # run the basic, station-only checks
        st.run_basic_qc()
        # create the summary flag column
        st.add_master_basic_flag()
        # save to output
        out_path = st.save(output_dir)
        return f"[basic QC] {os.path.basename(csv_path)} -> {out_path}"
    except Exception as e:
        return f"[basic QC] {os.path.basename(csv_path)} FAILED: {e}"


def main():
    make_root_in_sys_path()

    parser = argparse.ArgumentParser(
        description="Run basic QC on all station CSVs in a folder."
    )
    parser.add_argument(
        "--input-dir",
        default=str(Path(__file__).resolve().parents[1] / "input"),
        help="Folder with *.csv station files (default: example_stations/input)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(
            Path(__file__).resolve().parents[1] / "output" / "qc_coded"
        ),
        help="Folder to write QCed CSVs to (will be created).",
    )
    args = parser.parse_args()

    input_dir = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir)

    csv_files = sorted(glob(os.path.join(input_dir, "*.csv")))
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    worker_args = [(p, output_dir) for p in csv_files]

    # don't spawn more processes than files
    max_procs = max(1, cpu_count() - 1)
    n_proc = min(len(worker_args), max_procs)

    with Pool(processes=n_proc) as pool:
        for msg in pool.imap_unordered(process_one, worker_args):
            print(msg)


if __name__ == "__main__":
    main()