# qc_code/station_qc.py
import os
import pandas as pd

# basic QC modules (subfolder)
from .basic_qc.negatives import detect_negatives
from .basic_qc.drops import detect_drops
from .basic_qc.spikes import detect_rel_spikes, detect_abs_spikes
from .basic_qc.truncated import detect_truncated
from .basic_qc.unrealistically_high import detect_high_values
from .basic_qc.shifts import detect_shifts

# consistency checks (another subfolder)
from .consistency_checks.nrfa_daily import detect_differences_daily
from .consistency_checks.nrfa_amax import detect_differences_amax
from .consistency_checks.nrfa_amax import detect_differences_pot   

class Station:
    """
    Represents one station (one CSV).
    Can run:
      - basic QC (local checks on time series itself)
      - consistency checks (compare to NRFA / other reference datasets)
    """

    def __init__(self, path: str):
        self.path = path
        self.name = os.path.splitext(os.path.basename(path))[0]
        self.df = pd.read_csv(path)

        # normalise expected columns
        if "value" not in self.df.columns:
            # sometimes users call it 'flow'
            if "flow" in self.df.columns:
                self.df = self.df.rename(columns={"flow": "value"})
            else:
                raise ValueError(f"{path} does not have 'value' (or 'flow') column")

        # ensure datetime
        if "datetime" in self.df.columns:
            self.df["datetime"] = pd.to_datetime(self.df["datetime"])
        else:
            raise ValueError(f"{path} does not have 'datetime' column")

    # ------------------------------------------------------------------
    # 1) BASIC QC (renamed from run_all_qc)
    # ------------------------------------------------------------------
    def run_basic_qc(self):
        """
        Run the basic QC steps that only depend on the station time series.
        This is what was previously called run_all_qc.
        """
        # negatives
        self.df = detect_negatives(self.df, value_col="value")

        # drops
        self.df = detect_drops(self.df, value_col="value", threshold=5.0)

        # spikes
        self.df = detect_rel_spikes(self.df, value_col="value", rel_threshold=5.0)
        self.df = detect_abs_spikes(self.df, value_col="value", abs_threshold=0.99)

        # truncation
        self.df = detect_truncated(self.df, value_col="value")

        # unrealistically high
        self.df = detect_high_values(self.df, value_col="value", threshold=5000)

        # shifts / oscillations
        self.df = detect_shifts(self.df, value_col="value")

        return self.df

    # ------------------------------------------------------------------
    # 2) CONSISTENCY QC (new)
    # ------------------------------------------------------------------
    def run_consistency_checks(
        self,
        nrfa_daily_path: str | None = None,
        nrfa_amax_path: str | None = None,
        nrfa_pot_path: str | None = None,
        daily_threshold: float = 0.05,
        amax_threshold: float = 0.2,
        pot_threshold: float = 0.2,
    ):
        """
        Run consistency checks against external/reference datasets.
        Each of these files is optional — if not found, we skip.
        """
        # daily comparison
        if nrfa_daily_path and os.path.exists(nrfa_daily_path):
            nrfa_daily_df = pd.read_csv(nrfa_daily_path, index_col='datetime', parse_dates=True)
            self.df = detect_differences_daily(
                self.df, nrfa_daily_df, threshold=daily_threshold
            )
        else:
            # if missing, still add the column so downstream code doesn't break
            self.df["nrfa_daily"] = 0

        # AMAX comparison
        if nrfa_amax_path and os.path.exists(nrfa_amax_path):
            nrfa_amax_df = pd.read_csv(nrfa_amax_path)
            self.df = detect_differences_amax(
                self.df, nrfa_amax_df, threshold=amax_threshold
            )
        else:
            self.df["nrfa_amax"] = 0

        # POT comparison
        if nrfa_pot_path and os.path.exists(nrfa_pot_path):
            nrfa_pot_df = pd.read_csv(nrfa_pot_path)
            self.df = detect_differences_pot(
                self.df, nrfa_pot_df, threshold=pot_threshold
            )
        else:
            self.df["nrfa_pot"] = 0

        # master consistency flag (1 if any consistency issue)
        consistency_cols = ["nrfa_daily", "nrfa_amax", "nrfa_pot"]
        self.df["consistency_flag"] = (
            self.df[consistency_cols].sum(axis=1).clip(upper=1)
        )

        return self.df

    # ------------------------------------------------------------------
    # 3) generic helper to create a master flag for basic QC
    # ------------------------------------------------------------------
    def add_master_basic_flag(self):
        basic_cols = [
            col
            for col in self.df.columns
            if col
            in [
                "negatives",
                "drops",
                "rel_spike",
                "abs_spike",
                "truncation",
                "high_truncation",
                "high",
                "shifts",
            ]
        ]
        if basic_cols:
            self.df["qc_flag"] = self.df[basic_cols].sum(axis=1).clip(upper=1)
        else:
            self.df["qc_flag"] = 0
        return self.df

    # ------------------------------------------------------------------
    def save(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{self.name}.csv")
        self.df.to_csv(out_path, index=False)
        return out_path