"""
Alternative cleaning implementation using SQL (DuckDB).
This script performs the same data transformation as the main pipeline 
but leverages SQL window functions for enhanced structural validation 
and duplicate handling in the panel dataset.
"""

import argparse
from io import StringIO
from pathlib import Path

import duckdb
import pandas as pd
import requests

SPECS = [
    {
        "name": "UNEMPLOYMENT_RATE",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC,1.0/NOR+SWE+FIN+DNK..PT_LF_SUB..Y._T.Y_GE15..A?startPeriod=1983&endPeriod=2025&dimensionAtObservation=AllDimensions",
        "indicator_col": "MEASURE",
        "keep_if_indicator_in": ["UNE_LF", "UNE_LF_M"],
        "final_indicator": "UNEMPLOYMENT_RATE",
        "out_file": "oecd_unemployment_clean_sql.csv",
    },
    {
        "name": "CPI_YOY",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0/SWE+NOR+FIN+DNK.A.N.CPI.PA._T.N.GY+_Z?startPeriod=1995&endPeriod=2026&dimensionAtObservation=AllDimensions",
        "indicator_col": "MEASURE",
        "keep_if_indicator_in": None,
        "final_indicator": "CPI_YOY",
        "out_file": "oecd_cpi_clean_sql.csv",
    },
    {
        "name": "GDP_GROWTH_REAL",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD,/A..FIN+SWE+NOR+DNK.S1..B1GQ......GY.?startPeriod=1995&endPeriod=2025&dimensionAtObservation=AllDimensions",
        "indicator_col": "TRANSACTION",
        "keep_if_indicator_in": None,
        "final_indicator": "GDP_GROWTH_REAL",
        "out_file": "oecd_gdp_clean_sql.csv",
    },
    {
        "name": "RATE",
        "url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,4.0/DNK+SWE+NOR+FIN.A.IR3TIB....?startPeriod=1994&dimensionAtObservation=AllDimensions",
        "indicator_col": "MEASURE",
        "keep_if_indicator_in": None,
        "final_indicator": "RATE",
        "out_file": "oecd_rate_clean_sql.csv",
    },
]


def fetch_oecd_csv(url: str) -> pd.DataFrame:
    full_url = url + ("&format=csvfilewithlabels" if "format=" not in url else "")
    r = requests.get(full_url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text))


def clean_to_long_sql(
    df: pd.DataFrame,
    indicator_col: str,
    final_indicator: str,
    keep_if_indicator_in=None,
) -> pd.DataFrame:
    con = duckdb.connect()
    con.register("raw_df", df)

    if keep_if_indicator_in is None:
        q = f"""
        WITH base AS (
            SELECT
                UPPER(TRIM(CAST(REF_AREA AS VARCHAR))) AS country,
                TRIM(CAST(\"{indicator_col}\" AS VARCHAR)) AS indicator_raw,
                TRY_CAST(TIME_PERIOD AS BIGINT) AS year,
                TRY_CAST(OBS_VALUE AS DOUBLE) AS value
            FROM raw_df
        )
        SELECT country, year, '{final_indicator}' AS indicator, value
        FROM base
        WHERE country IS NOT NULL
          AND year IS NOT NULL
          AND value IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country, '{final_indicator}', year
            ORDER BY year DESC
        ) = 1
        ORDER BY country, year
        """
        return con.execute(q).df()

    keep_vals = [v.replace("'", "''") for v in keep_if_indicator_in]
    in_list = ", ".join([f"'{v}'" for v in keep_vals])
    case_prio = " ".join([f"WHEN indicator_raw = '{v}' THEN {i}" for i, v in enumerate(keep_vals)])

    q = f"""
    WITH base AS (
        SELECT
            UPPER(TRIM(CAST(REF_AREA AS VARCHAR))) AS country,
            TRIM(CAST(\"{indicator_col}\" AS VARCHAR)) AS indicator_raw,
            TRY_CAST(TIME_PERIOD AS BIGINT) AS year,
            TRY_CAST(OBS_VALUE AS DOUBLE) AS value
        FROM raw_df
    ),
    filtered AS (
        SELECT *
        FROM base
        WHERE country IS NOT NULL
          AND year IS NOT NULL
          AND value IS NOT NULL
          AND indicator_raw IN ({in_list})
    ),
    ranked AS (
        SELECT
            country,
            year,
            value,
            ROW_NUMBER() OVER (
                PARTITION BY country, year
                ORDER BY CASE {case_prio} ELSE 999 END
            ) AS rn
        FROM filtered
    )
    SELECT country, year, '{final_indicator}' AS indicator, value
    FROM ranked
    WHERE rn = 1
    ORDER BY country, year
    """
    return con.execute(q).df()


def qc(df: pd.DataFrame, name: str) -> None:
    print(f"\n=== {name} ===")
    print("shape:", df.shape)
    print("dup keys:", df.duplicated(["country", "indicator", "year"]).sum())
    print(df.groupby("country")["year"].agg(["min", "max", "count"]))


def build_wide_sql(master: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.register("master_df", master)

    return con.execute(
        """
        SELECT
            country,
            year,
            MAX(value) FILTER (WHERE indicator = 'CPI_YOY') AS CPI_YOY,
            MAX(value) FILTER (WHERE indicator = 'UNEMPLOYMENT_RATE') AS UNEMPLOYMENT_RATE,
            MAX(value) FILTER (WHERE indicator = 'GDP_GROWTH_REAL') AS GDP_GROWTH_REAL,
            MAX(value) FILTER (WHERE indicator = 'RATE') AS RATE
        FROM master_df
        GROUP BY country, year
        ORDER BY country, year
        """
    ).df()


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    all_series = []
    for s in SPECS:
        raw = fetch_oecd_csv(s["url"])
        clean = clean_to_long_sql(
            raw,
            indicator_col=s["indicator_col"],
            final_indicator=s["final_indicator"],
            keep_if_indicator_in=s["keep_if_indicator_in"],
        )
        clean.to_csv(out_dir / s["out_file"], index=False)
        qc(clean, s["name"])
        all_series.append(clean)

    master = pd.concat(all_series, ignore_index=True)
    con = duckdb.connect()
    con.register("master_in", master)

    master_clean = con.execute(
        """
        SELECT country, indicator, year, value
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY country, indicator, year
                    ORDER BY year DESC
                ) AS rn
            FROM master_in
        )
        WHERE rn = 1
        ORDER BY country, indicator, year
        """
    ).df()
    master_clean.to_csv(out_dir / "oecd_master_clean_sql.csv", index=False)

    wide = build_wide_sql(master_clean)
    wide.to_csv(out_dir / "oecd_master_wide_sql.csv", index=False)

    need = ["CPI_YOY", "UNEMPLOYMENT_RATE", "GDP_GROWTH_REAL", "RATE"]
    complete = wide.dropna(subset=need).copy()
    complete.to_csv(out_dir / "oecd_master_wide_complete_cases_sql.csv", index=False)

    print("\nSaved SQL-based files in:", out_dir.resolve())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and clean OECD macro data with SQL (DuckDB).")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data_sql_test"),
        help="Output directory for SQL-cleaned CSV files (default: ./data_sql_test)",
    )
    args = parser.parse_args()
    run(args.out_dir)
