# nordic-macro-pipeline
Automated ETL pipeline for Nordic macroeconomic data via OECD API
# Nordic Macroeconomic Panel Pipeline

An automated ETL tool designed to fetch and structure macroeconomic indicators for the Nordic region (Sweden, Norway, Denmark, Finland) via the OECD SDMX API.

## Project Overview
This project automates the collection of key economic signals: 
- GDP Growth (Real)
- Inflation (CPI YoY)
- Unemployment Rate
- Short-term Interest Rates

It transforms raw API responses into cleaned, standardized panel datasets ready for econometric analysis.

## Features
- **Automated Extraction:** Direct integration with OECD's SDMX REST API.
- **Robust Data Cleaning:** Handles missing values and prioritizes specific labor market measures.
- **Data Transformation:** Generates both `long` and `wide` formats for analysis.

## Current Status
- [x] API Integration & Pipeline
- [x] Data Cleaning & Panel Construction (Python & SQL)
- [ ] Econometric Analysis (Panel Fixed Effects) - *In Progress*
