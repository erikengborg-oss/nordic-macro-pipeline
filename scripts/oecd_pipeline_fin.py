# pipeline.py
import argparse
from pathlib import Path
from io import StringIO

import pandas as pd
import requests

# Configuration
SPECS = [
    {
        "name": "UNEMPLOYMENT_RATE",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC,1.0/NOR+SWE+FIN+DNK..PT_LF_SUB..Y._T.Y_GE15..A?startPeriod=1983&endPeriod=2025&dimensionAtObservation=AllDimensions",
        "indicator_col": "MEASURE",
        "keep_if_indicator_in": ["UNE_LF", "UNE_LF_M"],
        "final_indicator": "UNEMPLOYMENT_RATE",
        "out_file": "oecd_unemployment_clean.csv",
    },
    {
        "name": "CPI_YOY",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0/SWE+NOR+FIN+DNK.A.N.CPI.PA._T.N.GY+_Z?startPeriod=1995&endPeriod=2026&dimensionAtObservation=AllDimensions",
        "indicator_col": "MEASURE",
        "keep_if_indicator_in": None,
        "final_indicator": "CPI_YOY",
        "out_file": "oecd_cpi_clean.csv",
    },
    {
        "name": "GDP_GROWTH_REAL",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD,/A..FIN+SWE+NOR+DNK.S1..B1GQ......GY.?startPeriod=1995&endPeriod=2025&dimensionAtObservation=AllDimensions",
        "indicator_col": "TRANSACTION",
        "keep_if_indicator_in": None,
        "final_indicator": "GDP_GROWTH_REAL",
        "out_file": "oecd_gdp_clean.csv",
    },
    {
        "name": "RATE",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,4.0/DNK+SWE+NOR+FIN.A.IR3TIB....?startPeriod=1994&dimensionAtObservation=AllDimensions",
        "indicator_col": "MEASURE",
        "keep_if_indicator_in": None,
        "final_indicator": "RATE",
        "out_file": "oecd_rate_clean.csv",
    },
]

# Collect data from API URL
def fetch_oecd_csv(url: str) -> pd.DataFrame:
    full_url = url + ("&format=csvfilewithlabels" if "format=" not in url else "")
    r = requests.get(full_url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text))

# Convert the variables
def clean_to_long(
    df: pd.DataFrame,
    indicator_col: str,
    final_indicator: str,
    keep_if_indicator_in=None,
) -> pd.DataFrame:
    out = df[["REF_AREA", indicator_col, "TIME_PERIOD", "OBS_VALUE"]].copy()
    out = out.rename(
        columns={
            "REF_AREA": "country",
            indicator_col: "indicator_raw",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
        }
    )

    out["country"] = out["country"].astype(str).str.strip().str.upper()
    out["indicator_raw"] = out["indicator_raw"].astype(str).str.strip()
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["country", "year", "value"])

    if keep_if_indicator_in is not None:
        out = out[out["indicator_raw"].isin(keep_if_indicator_in)].copy()
        out["prio"] = out["indicator_raw"].map({"UNE_LF": 0, "UNE_LF_M": 1}).fillna(9)
        out = (
            out.sort_values(["country", "year", "prio"])
            .drop_duplicates(["country", "year"], keep="first")
            .drop(columns=["prio"])
        )

    out["indicator"] = final_indicator
    out = out[["country", "year", "indicator", "value"]]
    out = out.drop_duplicates(["country", "indicator", "year"], keep="last")
    out = out.sort_values(["country", "year"]).reset_index(drop=True)
    return out


def qc(df: pd.DataFrame, name: str) -> None:
    print(f"\n=== {name} ===")
    print("shape:", df.shape)
    print("dup keys:", df.duplicated(["country", "indicator", "year"]).sum())
    print(df.groupby("country")["year"].agg(["min", "max", "count"]))

# Output doc
def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    all_series = []
    for s in SPECS:
        raw = fetch_oecd_csv(s["url"])
        clean = clean_to_long(
            raw,
            indicator_col=s["indicator_col"],
            final_indicator=s["final_indicator"],
            keep_if_indicator_in=s["keep_if_indicator_in"],
        )
        clean.to_csv(out_dir / s["out_file"], index=False)
        qc(clean, s["name"])
        all_series.append(clean)

    master = pd.concat(all_series, ignore_index=True)
    master = master.drop_duplicates(["country", "indicator", "year"], keep="last")
    master = master.sort_values(["country", "indicator", "year"]).reset_index(drop=True)
    master.to_csv(out_dir / "oecd_master_clean.csv", index=False)

    wide = (
        master.pivot_table(
            index=["country", "year"],
            columns="indicator",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    cols = ["country", "year", "CPI_YOY", "UNEMPLOYMENT_RATE", "GDP_GROWTH_REAL", "RATE"]
    cols = [c for c in cols if c in wide.columns]
    wide = wide[cols]
    wide.to_csv(out_dir / "oecd_master_wide.csv", index=False)

    need = ["CPI_YOY", "UNEMPLOYMENT_RATE", "GDP_GROWTH_REAL", "RATE"]
    complete = wide.dropna(subset=need).copy()
    complete.to_csv(out_dir / "oecd_master_wide_complete_cases.csv", index=False)

    print("\nSaved files in:", out_dir.resolve())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and clean OECD macro data.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data"),
        help="Output directory for cleaned CSV files (default: ./data)",
    )
    args = parser.parse_args()
    run(args.out_dir)


# Cleaning complete. Dataset is ready for regression analysis.

