import os
import argparse
from glob import glob
from multiprocessing import Pool, cpu_count
from pathlib import Path
import sys


def make_root_in_sys_path():
    here = Path(__file__).resolve()
    root = here.parents[2]  # UK-Flow15-QC/
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def build_reference_paths(ref_base: str, station_name: str):
    """
    Build full paths to the three optional reference files for one station.
    They may or may not exist — the Station class already copes with missing ones.
    """
    daily = os.path.join(ref_base, "nrfa_daily", f"{station_name}.csv")
    amax = os.path.join(ref_base, "nrfa_amax", f"{station_name}.csv")
    pot = os.path.join(ref_base, "nrfa_pot", f"{station_name}.csv")
    return daily, amax, pot


def process_one(args):
    csv_path, output_dir, ref_dir = args
    from qc_code.station_qc import Station
    try:
        station_name = Path(csv_path).stem
        daily_path, amax_path, pot_path = build_reference_paths(ref_dir, station_name)

        st = Station(csv_path)
        st.run_consistency_checks(
            nrfa_daily_path=daily_path,
            nrfa_amax_path=amax_path,
            nrfa_pot_path=pot_path,
        )
        out_path = st.save(output_dir)
        return f"[consistency] {station_name} -> {out_path}"
    except Exception as e:
        return f"[consistency] {os.path.basename(csv_path)} FAILED: {e}"


def main():
    make_root_in_sys_path()

    parser = argparse.ArgumentParser(
        description="Run consistency checks against reference NRFA-style CSVs."
    )
    parser.add_argument(
        "--input-dir",
        default=str(Path(__file__).resolve().parents[1] / "input"),
        help="Folder with station *.csv files.",
    )
    parser.add_argument(
        "--ref-dir",
        default=str(
            Path(__file__).resolve().parents[1] / "input" / "consistency_checks"
        ),
        help="Folder that contains nrfa_daily/, nrfa_amax/, nrfa_pot/ subfolders.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(
            Path(__file__).resolve().parents[1] / "output" / "qc_coded"
        ),
        help="Folder to write QCed CSVs to.",
    )
    args = parser.parse_args()

    input_dir = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir)
    ref_dir = os.path.abspath(args.ref_dir)

    csv_files = sorted(glob(os.path.join(input_dir, "*.csv")))
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    worker_args = [(p, output_dir, ref_dir) for p in csv_files]

    max_procs = max(1, cpu_count() - 1)
    n_proc = min(len(worker_args), max_procs)

    with Pool(processes=n_proc) as pool:
        for msg in pool.imap_unordered(process_one, worker_args):
            print(msg)


if __name__ == "__main__":
    main()