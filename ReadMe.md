# CDC All-Cause Mortality Data Compiler

## How to Use This Project

This project consists of two components that work together:

### Step 1: Run the Data Compiler
First, compile the mortality data from CDC sources:

```bash
python mortality_compiler.py
```

This will generate two CSV files:
- `us_national_mortality_2015_present.csv`
- `state_mortality_2015_present.csv`

### Step 2: Run the Interactive Dashboard
After the data is compiled, launch the visualization dashboard:

```bash
streamlit run mortality_dashboard.py
```

The dashboard will open in your web browser at `http://localhost:8501`

**Requirements for Dashboard:**
```bash
pip install streamlit plotly pandas numpy
```

---

## Mortality Data Dashboard

The interactive Streamlit dashboard provides comprehensive visualization and analysis of the compiled mortality data with four distinct views:

### Dashboard Features

**Four Visualization Modes:**

1. **Raw Deaths** - Absolute weekly death counts over time
   - Shows total mortality trends by week and year
   - Sidebar displays total deaths for selected time period and states

2. **Deaths per 100k Population** - Population-normalized mortality rates
   - Enables fair comparison between states of different sizes
   - Sidebar shows average annual rates for three key periods:
     - **2015-2019 (Baseline)**: Pre-pandemic mortality rates
     - **2020-2022 (Pandemic)**: Pandemic period mortality rates  
     - **2023-2025 (Recent)**: Post-pandemic mortality trends

3. **Deviation from Average** - Comparison to 2015-2019 baseline
   - Shows weekly deaths compared to pre-pandemic averages
   - Highlights periods of excess or deficit mortality
   - Sidebar displays total deaths above average during 2020-2022

4. **Deviation from Expected** - Accounts for population growth
   - Compares actual deaths to expected deaths assuming 1.31% annual growth
   - Adjusts for normal demographic changes over time
   - Sidebar shows total deaths above expected during 2020-2022

### Interactive Controls

**State Selection:**
- Choose between state-level or national data
- Multi-select specific states for comparison
- "All States" option aggregates data across all states

**Visual Design:**
- **Color-coded years**: Green (2015-2019 baseline), Red (2020-2022 pandemic), Pink (2023-2024 recent), Blue (2025 current)
- **Year labels**: Each trend line ends with its year for easy identification
- **Hover details**: Interactive tooltips show exact values and context
- **Zero reference lines**: Deviation charts include horizontal reference lines

### Data Interpretation

**Raw Deaths View:**
- Use for understanding absolute mortality burden
- Compare total deaths between states or time periods
- Identify seasonal patterns and major mortality events

**Deaths per 100k View:**
- Essential for comparing states with different population sizes
- Track changes in mortality rates over time
- Compare baseline vs. pandemic vs. recent period rates

**Deviation Views:**
- Identify periods of excess mortality above historical norms
- Distinguish between expected demographic changes and unusual events
- Quantify the total impact of significant mortality events

The dashboard automatically handles data aggregation, calculates population-weighted averages for multi-state selections, and provides real-time filtering based on user selections.

---

A comprehensive Python tool for compiling CDC all-cause mortality data from multiple sources, covering 2015 to present. This tool addresses gaps in publicly available state-level mortality data by combining historical archives with current API endpoints and a complete 2019 dataset.

## Overview

This script automatically downloads, processes, and standardizes all-cause mortality data from multiple CDC and international sources to create complete time series datasets. The tool is specifically designed to handle the gap in state-level 2019 data that exists in current online sources.

### Key Features

- **Comprehensive Coverage**: 2015 to present (continuously updated)
- **Multiple Data Sources**: Combines World Mortality Dataset, CDC APIs, NCHS archives, and local files
- **Dual Output**: Separate files for US national totals and state-level data
- **Population Integration**: Includes population data and mortality rates per 100k
- **MMWR Week Standardization**: Proper epidemiological week calculations
- **Data Quality Assurance**: Automated cleaning, deduplication, and validation
- **Robust Error Handling**: Retry logic and comprehensive logging

## Data Sources

1. **World Mortality Dataset** - US national weekly deaths (2015-2020)
   - Source: `https://github.com/akarlinsky/world_mortality`
   
2. **CDC Provisional COVID-19 Death Counts** - State-level data (2020-present)  
   - Source: `https://data.cdc.gov/api/views/r8kw-7aab/rows.csv`
   
3. **NCHS Mortality Surveillance System** - Archived state data (2015-2018)
   - Source: Archive.org backup of CDC datasets
   
4. **Local 2019 Complete Dataset** - State-level data for 2019
   - File: `all_state_data_for_2019.csv` (included in repository)
   - Note: This file was generated from live CDC data during 2020-2023 and contains complete state-level mortality data for 2019 that is no longer available through current online sources.

## Requirements

### Python Dependencies

```
requests>=2.25.0
pandas>=1.3.0
numpy>=1.21.0
```

### System Requirements

- Python 3.7+
- Internet connection for API downloads
- ~100MB free disk space for data processing

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/cdc-mortality-compiler.git
cd cdc-mortality-compiler
```

2. Install required packages:
```bash
pip install requests pandas numpy
```

3. Ensure the 2019 data file is present:
```bash
# The file all_state_data_for_2019.csv should be in the project directory
ls all_state_data_for_2019.csv
```

## Usage

### Basic Usage

Simply run the script:

```bash
python mortality_compiler.py
```

The script will:
1. Load the local 2019 state data file
2. Download current data from CDC and international sources
3. Process and standardize all datasets
4. Generate two output files with comprehensive mortality data

### Output Files

**1. `us_national_mortality_2015_present.csv`**
- US national totals only
- Weekly all-cause mortality data
- Population and mortality rates included

**2. `state_mortality_2015_present.csv`**  
- State-level data for all 50 states + DC + Puerto Rico
- Weekly all-cause mortality by state
- Population and mortality rates included

### Data Columns

Both output files contain:
- `year` - Calendar year
- `week` - MMWR epidemiological week (1-53)
- `mmwr_week` - Same as week (for compatibility)
- `week_ending_date` - Saturday date ending the week (when available)
- `state` - State name or "United States" for national data
- `deaths` - All-cause deaths for that week
- `population` - Annual population estimate
- `mortality_rate_per_100k` - Deaths per 100,000 population
- `data_source` - Origin of the data

### Example Usage in Analysis

```python
import pandas as pd

# Load the datasets
us_data = pd.read_csv('us_national_mortality_2015_present.csv')
state_data = pd.read_csv('state_mortality_2015_present.csv')

# Example: Get 2019 total deaths by state
state_2019 = state_data[state_data['year'] == 2019]
deaths_by_state = state_2019.groupby('state')['deaths'].sum().sort_values(ascending=False)
print(deaths_by_state.head(10))

# Example: Calculate excess mortality for 2020
baseline = us_data[us_data['year'].isin([2017, 2018, 2019])].groupby('week')['deaths'].mean()
actual_2020 = us_data[us_data['year'] == 2020].set_index('week')['deaths']
excess = actual_2020 - baseline
```

## Data Quality Notes

### 2019 Data Completeness
The included 2019 dataset (`all_state_data_for_2019.csv`) represents a complete archive of state-level mortality data that was captured from live CDC feeds during 2020-2023. This data fills a critical gap as comprehensive 2019 state-level data is no longer readily available through current CDC online sources.

### MMWR Week Corrections
The script applies a 1-week correction to 2019 data to align with standard MMWR week numbering, ensuring consistency across all years in the dataset.

### Data Lag and Revisions
- Recent weeks (last 4-8 weeks) are provisional and subject to revision
- State-level data may have longer reporting delays than national totals
- The script automatically handles data updates when re-run

## Methodology

### Population Data
Annual population estimates from the US Census Bureau are integrated to enable mortality rate calculations. Missing population data is backfilled using prior year estimates.

### Data Standardization
All sources are standardized to a common schema with consistent state names, week numbering, and data formats. Duplicate records are removed with priority given to more complete data sources.

### Quality Assurance
- Numeric validation and range checking
- Geographic standardization (e.g., combining NYC with NY state)
- Temporal consistency validation
- Cross-source verification where possible

## Troubleshooting

### Common Issues

**File Not Found Error:**
```
Error: all_state_data_for_2019.csv not found
```
- Ensure the 2019 data file is in the same directory as the script
- Check filename spelling and case sensitivity

**Network Connection Issues:**
```
Failed to download after X attempts
```
- Check internet connection
- Some government APIs may have temporary outages
- The script will continue with available data sources

**Memory Issues:**
- The script processes large datasets; ensure adequate RAM (4GB+ recommended)
- Close other memory-intensive applications during processing

### Logging
The script provides detailed logging output. Check the console output for:
- Download progress
- Data processing steps  
- Warning messages about missing data
- Final statistics and file locations

## Contributing

Contributions are welcome! Please consider:

- Reporting issues with data quality or missing sources
- Adding new data sources or improving existing ones
- Enhancing data validation and cleaning routines
- Improving documentation and examples

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Data Attribution

- World Mortality Dataset: Karlinsky & Kobak (2021)
- CDC Data: Centers for Disease Control and Prevention
- Population Data: US Census Bureau Population Estimates Program

Please cite appropriate sources when using this data in research or publications.

## Disclaimer

This tool compiles publicly available mortality data for research and analysis purposes. While every effort is made to ensure data accuracy, users should verify critical analyses against original sources. The authors are not responsible for decisions made based on this compiled data.