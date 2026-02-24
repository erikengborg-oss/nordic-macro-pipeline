# Nordic Macroeconomic Panel Pipeline

Automated ETL pipeline for Nordic macroeconomic data via the OECD SDMX API.

## Project Overview

This project fetches and structures key macro indicators for:
- Sweden
- Norway
- Denmark
- Finland

Indicators included:
- GDP Growth (Real)
- Inflation (CPI YoY)
- Unemployment Rate
- Short-term Interest Rate

The pipeline transforms raw API responses into cleaned panel datasets in both long and wide format, ready for econometric analysis.

## Features

- **Automated Extraction:** Direct integration with the OECD SDMX REST API.
- **Robust Data Cleaning:** Handles missing values and prioritizes selected labor market measures.
- **Data Transformation:** Produces both `long` and `wide` datasets.
- **Interactive 3D Visualization:** Rotatable Plotly charts by variable, country, and year.

## Interactive Plots (GitHub Pages)

- **Homepage:** [Nordic Macro 3D Plots](https://erikengborg-oss.github.io/nordic-macro-pipeline/)
- **CPI YoY:** [cpi_yoy_3d.html](https://erikengborg-oss.github.io/nordic-macro-pipeline/cpi_yoy_3d.html)
- **GDP Growth (Real):** [gdp_growth_real_3d.html](https://erikengborg-oss.github.io/nordic-macro-pipeline/gdp_growth_real_3d.html)
- **Rate:** [rate_3d.html](https://erikengborg-oss.github.io/nordic-macro-pipeline/rate_3d.html)
- **Unemployment Rate:** [unemployment_rate_3d.html](https://erikengborg-oss.github.io/nordic-macro-pipeline/unemployment_rate_3d.html)

## Current Status

- [x] API Integration and Pipeline
- [x] Data Cleaning and Panel Construction (Python and SQL)
- [ ] Econometric Analysis (Panel Fixed Effects) - in progress
