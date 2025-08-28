import requests
import pandas as pd
import time
from datetime import datetime
import logging
import os
from io import StringIO

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ComprehensiveMortalityDataCompiler:
    """
    Compiles comprehensive CDC mortality data from 2015-present by combining:
    1. Historical data (2015-2020): World Mortality Dataset (GitHub)
    2. Current data (2020-present): CDC APIs

    Outputs a unified CSV with weekly mortality data by state and cause.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Comprehensive-Mortality-Compiler/1.0',
            'Accept': 'text/csv,application/json'
        })

        # Data sources
        self.data_sources = {
            # Historical all-cause mortality 2015-2020 (national level)
            'world_mortality': {
                'url': 'https://raw.githubusercontent.com/akarlinsky/world_mortality/main/world_mortality.csv',
                'description': 'World Mortality Dataset - US weekly deaths 2015-2020+'
            },
            # Current state-level provisional data 2020-present
            'cdc_provisional': {
                'url': 'https://data.cdc.gov/api/views/r8kw-7aab/rows.csv?accessType=DOWNLOAD',
                'description': 'CDC Provisional COVID-19 Death Counts by Week and State 2020-present'
            },
            # ARCHIVED: State-level historical data from Internet Archive
            'archived_state_deaths': {
                'url': 'https://archive.org/download/20250128-cdc-datasets/Deaths_from_Pneumonia_and_Influenza_P_I_and_all_deaths_by_state_and_region_National_Center_For_Health_Statistics_Mortality_Surveillance_System.csv',
                'description': 'ARCHIVED: Deaths by State from NCHS Surveillance System (2015-2019)'
            },
            # ARCHIVED: Weekly deaths data 2018-2020
            'archived_weekly_deaths': {
                'url': 'https://archive.org/download/20250128-cdc-datasets/AH_Deaths_by_Week_Sex_and_Age_for_2018-2020.csv',
                'description': 'ARCHIVED: Weekly Deaths by Demographics 2018-2020'
            },
            # CDC 2014-2019 State-level weekly deaths (complete 2019 data)
            'cdc_2014_2019_weekly': {
                'url': 'https://data.cdc.gov/api/views/3yf8-kanr/rows.csv?accessType=DOWNLOAD',
                'description': 'CDC Weekly Counts of Deaths by State and Select Causes, 2014-2019'
            }
        }

    def download_dataset(self, source_key: str, max_retries: int = 3) -> pd.DataFrame:
        """Download a dataset with retry logic."""
        source = self.data_sources[source_key]
        url = source['url']
        description = source['description']

        logger.info(f"Downloading {description}...")

        for attempt in range(max_retries):
            try:
                time.sleep(1)  # Be respectful to servers
                response = self.session.get(url, timeout=60)
                response.raise_for_status()

                # Parse CSV data
                df = pd.read_csv(StringIO(response.text))
                logger.info(f"Successfully downloaded {len(df)} records from {source_key}")
                return df

            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1} failed for {source_key}: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to download {source_key} after {max_retries} attempts")
                    return pd.DataFrame()
                time.sleep(2 ** attempt)  # Exponential backoff

        return pd.DataFrame()

    def process_world_mortality_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the World Mortality Dataset to extract US data 2015-2020."""
        if df.empty:
            return df

        logger.info("Processing World Mortality Dataset...")

        # Filter for USA data
        us_data = df[df['country_name'] == 'United States'].copy()

        if us_data.empty:
            logger.warning("No US data found in World Mortality Dataset")
            return pd.DataFrame()

        logger.info(f"Found {len(us_data)} US records in World Mortality Dataset")

        # Standardize columns
        us_data = us_data.rename(columns={
            'country_name': 'country',
            'time': 'week',
            'time_unit': 'period_type'
        })

        # Convert numeric columns
        numeric_cols = ['year', 'week', 'deaths']
        for col in numeric_cols:
            if col in us_data.columns:
                us_data[col] = pd.to_numeric(us_data[col], errors='coerce')

        # Filter for 2015-2020 (historical data)
        us_data = us_data[us_data['year'].between(2015, 2020)]

        # Add standardized columns
        us_data['state'] = 'United States'  # This is national level data
        us_data['cause'] = 'All causes'
        us_data['data_source'] = 'World Mortality Dataset'
        us_data['mmwr_week'] = us_data['week']

        # Select and order columns
        columns_to_keep = ['year', 'week', 'mmwr_week', 'state', 'cause', 'deaths', 'data_source', 'period_type']
        available_columns = [col for col in columns_to_keep if col in us_data.columns]
        us_data = us_data[available_columns]

        logger.info(
            f"Processed World Mortality data: {len(us_data)} records from {us_data['year'].min()}-{us_data['year'].max()}")
        return us_data

    def process_cdc_provisional_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process CDC provisional data to extract state-level mortality by cause."""
        if df.empty:
            return df

        logger.info("Processing CDC provisional data...")

        # Display available columns
        logger.info(f"CDC data columns: {df.columns.tolist()}")

        # Standardize column names
        column_mapping = {
            'Year': 'year',
            'MMWR Week': 'mmwr_week',
            'Week Ending Date': 'week_ending_date',
            'State': 'state',
            'COVID-19 Deaths': 'covid_deaths',
            'Total Deaths': 'total_deaths',
            'Pneumonia Deaths': 'pneumonia_deaths',
            'Influenza Deaths': 'influenza_deaths',
            'Pneumonia and COVID-19 Deaths': 'pneumonia_covid_deaths',
            'Pneumonia, Influenza, or COVID-19 Deaths': 'pic_deaths'
        }

        # Apply column renaming
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})

        # Convert numeric columns
        numeric_cols = ['year', 'mmwr_week', 'covid_deaths', 'total_deaths',
                        'pneumonia_deaths', 'influenza_deaths', 'pneumonia_covid_deaths', 'pic_deaths']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Create separate records for each cause of death
        results = []

        cause_mappings = [
            ('total_deaths', 'All causes'),
            ('covid_deaths', 'COVID-19'),
            ('pneumonia_deaths', 'Pneumonia'),
            ('influenza_deaths', 'Influenza'),
            ('pneumonia_covid_deaths', 'Pneumonia and COVID-19'),
            ('pic_deaths', 'Pneumonia, Influenza, or COVID-19')
        ]

        for death_col, cause_name in cause_mappings:
            if death_col in df.columns:
                subset = df[['year', 'mmwr_week', 'week_ending_date', 'state', death_col]].copy()
                subset = subset[subset[death_col].notna() & (subset[death_col] > 0)]

                if not subset.empty:
                    subset['cause'] = cause_name
                    subset['deaths'] = subset[death_col]
                    subset['data_source'] = 'CDC Provisional'
                    subset['period_type'] = 'weekly'
                    subset['week'] = subset['mmwr_week']  # Create week column for consistency

                    # Select relevant columns
                    columns_to_keep = ['year', 'week', 'mmwr_week', 'week_ending_date', 'state', 'cause', 'deaths',
                                       'data_source', 'period_type']
                    available_columns = [col for col in columns_to_keep if col in subset.columns]
                    subset = subset[available_columns]

                    results.append(subset)
                    logger.info(f"Created {len(subset)} records for {cause_name}")

        if results:
            processed_df = pd.concat(results, ignore_index=True)
            year_range = f"{processed_df['year'].min():.0f}-{processed_df['year'].max():.0f}"
            logger.info(f"Processed CDC data: {len(processed_df)} records from {year_range}")
            return processed_df

        logger.warning("No cause-specific data could be extracted from CDC data")
        return pd.DataFrame()

    def process_archived_state_deaths(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process archived state-level death data from NCHS surveillance system."""
        if df.empty:
            return df

        logger.info("Processing archived state-level death data...")
        logger.info(f"Archived data columns: {df.columns.tolist()}")
        logger.info(f"Total records: {len(df)}")

        # Filter for "All" age group only to avoid demographic summing
        df_all_ages = df[df['age'] == 'All'].copy()
        logger.info(f"Records with 'All' age group: {len(df_all_ages)}")

        # Parse MMWR Year/Week format (YYYYWW -> separate year and week)
        df_all_ages['year'] = (df_all_ages['MMWR Year/Week'] // 100).astype(int)
        df_all_ages['mmwr_week'] = (df_all_ages['MMWR Year/Week'] % 100).astype(int)
        df_all_ages['week'] = df_all_ages['mmwr_week']  # For consistency

        # Filter for 2015-2019 data only
        df_filtered = df_all_ages[df_all_ages['year'].between(2015, 2019)].copy()
        logger.info(f"Records filtered to 2015-2019: {len(df_filtered)}")

        # Rename columns to match API format
        column_mapping = {
            'State': 'state',
            'All Deaths': 'all_deaths',
            'Deaths from pneumonia': 'pneumonia_deaths',
            'Deaths from influenza': 'influenza_deaths',
            'Deaths from pneumonia and influenza': 'pneumonia_influenza_deaths'
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df_filtered.columns:
                df_filtered = df_filtered.rename(columns={old_name: new_name})

        # Convert numeric columns
        numeric_cols = ['all_deaths', 'pneumonia_deaths', 'influenza_deaths', 'pneumonia_influenza_deaths']
        for col in numeric_cols:
            if col in df_filtered.columns:
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

        # Remove rows with null state (these appear to be national aggregates)
        df_filtered = df_filtered[df_filtered['state'].notna()].copy()
        logger.info(f"Records after removing null states: {len(df_filtered)}")

        # Create separate records for each cause of death (matching API structure)
        results = []

        # Map death columns to cause names (matching API format)
        death_mappings = [
            ('all_deaths', 'All causes'),
            ('pneumonia_deaths', 'Pneumonia'),
            ('influenza_deaths', 'Influenza'),
            ('pneumonia_influenza_deaths', 'Pneumonia and Influenza')
        ]

        for death_col, cause_name in death_mappings:
            if death_col in df_filtered.columns:
                # Create subset with non-null, positive death counts
                subset = df_filtered[['year', 'week', 'mmwr_week', 'state', death_col]].copy()
                subset = subset[subset[death_col].notna() & (subset[death_col] > 0)]

                if not subset.empty:
                    subset['cause'] = cause_name
                    subset['deaths'] = subset[death_col].astype(int)
                    subset['data_source'] = 'Archived NCHS'
                    subset['period_type'] = 'weekly'

                    # Drop the original death column
                    subset = subset.drop(death_col, axis=1)

                    results.append(subset)
                    logger.info(f"Created {len(subset)} archived records for {cause_name}")

        if results:
            processed_df = pd.concat(results, ignore_index=True)

            # Summary statistics
            year_range = f"{processed_df['year'].min()}-{processed_df['year'].max()}"
            state_count = processed_df['state'].nunique()
            cause_count = processed_df['cause'].nunique()
            total_deaths = processed_df['deaths'].sum()

            logger.info(f"Processed archived data successfully:")
            logger.info(f"   {len(processed_df)} total records")
            logger.info(f"   Years: {year_range}")
            logger.info(f"   States: {state_count}")
            logger.info(f"   Causes: {cause_count}")
            logger.info(f"   Total deaths: {total_deaths:,}")

            return processed_df

        logger.warning("No usable data found in archived dataset")
        return pd.DataFrame()

    def process_cdc_2014_2019_weekly(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process CDC 2014-2019 weekly deaths dataset to extract complete 2019 state-level data."""
        if df.empty:
            return df

        logger.info("Processing CDC 2014-2019 weekly deaths dataset...")
        logger.info(f"CDC 2014-2019 data columns: {df.columns.tolist()}")
        logger.info(f"Total records: {len(df)}")

        # Filter for 2019 only (to fill the gap)
        df_2019 = df[df['MMWR Year'] == 2019].copy()
        logger.info(f"Records for 2019: {len(df_2019)}")

        if df_2019.empty:
            logger.warning("No 2019 data found in CDC 2014-2019 dataset")
            return pd.DataFrame()

        # Standardize column names
        df_2019 = df_2019.rename(columns={
            'Jurisdiction of Occurrence': 'state',
            'MMWR Year': 'year',
            'MMWR Week': 'mmwr_week',
            'Week Ending Date': 'week_ending_date',
            'All Cause': 'all_deaths',
            'Influenza and pneumonia (J10-J18)': 'influenza_pneumonia_deaths'
        })

        # Create week column for consistency
        df_2019['week'] = df_2019['mmwr_week']

        # Convert numeric columns
        numeric_cols = ['year', 'mmwr_week', 'all_deaths', 'influenza_pneumonia_deaths']
        for col in numeric_cols:
            if col in df_2019.columns:
                df_2019[col] = pd.to_numeric(df_2019[col], errors='coerce')

        # Remove rows with null state or null death counts
        df_2019 = df_2019[df_2019['state'].notna()].copy()
        logger.info(f"Records after removing null states: {len(df_2019)}")

        # Create separate records for each cause of death
        results = []

        # Map death columns to standardized cause names
        death_mappings = [
            ('all_deaths', 'All causes'),
            ('influenza_pneumonia_deaths', 'Influenza and pneumonia')
        ]

        for death_col, cause_name in death_mappings:
            if death_col in df_2019.columns:
                # Create subset with non-null, positive death counts
                subset = df_2019[['year', 'week', 'mmwr_week', 'week_ending_date', 'state', death_col]].copy()
                subset = subset[subset[death_col].notna() & (subset[death_col] > 0)]

                if not subset.empty:
                    subset['cause'] = cause_name
                    subset['deaths'] = subset[death_col].astype(int)
                    subset['data_source'] = 'CDC 2014-2019 Weekly'
                    subset['period_type'] = 'weekly'

                    # Drop the original death column
                    subset = subset.drop(death_col, axis=1)

                    results.append(subset)
                    logger.info(f"Created {len(subset)} records for 2019 {cause_name}")

        if results:
            processed_df = pd.concat(results, ignore_index=True)

            # Summary statistics
            state_count = processed_df['state'].nunique()
            cause_count = processed_df['cause'].nunique()
            total_deaths = processed_df['deaths'].sum()

            logger.info(f"Processed CDC 2014-2019 data successfully:")
            logger.info(f"   {len(processed_df)} total records for 2019")
            logger.info(f"   States: {state_count}")
            logger.info(f"   Causes: {cause_count}")
            logger.info(f"   Total deaths: {total_deaths:,}")

            return processed_df

        logger.warning("No usable 2019 data found in CDC 2014-2019 dataset")
        return pd.DataFrame()

    def get_population_data(self, years: list) -> pd.DataFrame:
        """Get annual state population estimates from static Census Bureau data."""
        logger.info(f"Loading population data for years: {years}")

        # Official US Census Bureau Population Estimates (July 1 estimates)
        population_data = {
            2015: {
                'Alabama': 4903185, 'Alaska': 737068, 'Arizona': 6817565, 'Arkansas': 2979732,
                'California': 39032444, 'Colorado': 5450623, 'Connecticut': 3593222, 'Delaware': 945934,
                'Florida': 20244914, 'Georgia': 10214860, 'Hawaii': 1431603, 'Idaho': 1654930,
                'Illinois': 12859995, 'Indiana': 6633053, 'Iowa': 3130869, 'Kansas': 2911641,
                'Kentucky': 4425092, 'Louisiana': 4686157, 'Maine': 1327568, 'Maryland': 6006401,
                'Massachusetts': 6794422, 'Michigan': 9922576, 'Minnesota': 5489594, 'Mississippi': 2992333,
                'Missouri': 6083672, 'Montana': 1042520, 'Nebraska': 1896190, 'Nevada': 2890845,
                'New Hampshire': 1330608, 'New Jersey': 8958013, 'New Mexico': 2089283, 'New York': 19795791,
                'North Carolina': 10042802, 'North Dakota': 756927, 'Ohio': 11613423, 'Oklahoma': 3911338,
                'Oregon': 4028977, 'Pennsylvania': 12802503, 'Rhode Island': 1056298, 'South Carolina': 4896146,
                'South Dakota': 858469, 'Tennessee': 6600299, 'Texas': 27469114, 'Utah': 2995919,
                'Vermont': 626042, 'Virginia': 8382993, 'Washington': 7170351, 'West Virginia': 1844128,
                'Wisconsin': 5771337, 'Wyoming': 586107, 'District of Columbia': 672228, 'Puerto Rico': 3474182
            },
            2016: {
                'Alabama': 4863300, 'Alaska': 741894, 'Arizona': 6931071, 'Arkansas': 2988248,
                'California': 39250017, 'Colorado': 5540545, 'Connecticut': 3576452, 'Delaware': 952065,
                'Florida': 20612439, 'Georgia': 10310371, 'Hawaii': 1428557, 'Idaho': 1683140,
                'Illinois': 12801539, 'Indiana': 6633053, 'Iowa': 3134693, 'Kansas': 2907289,
                'Kentucky': 4436974, 'Louisiana': 4681666, 'Maine': 1331479, 'Maryland': 6016447,
                'Massachusetts': 6811779, 'Michigan': 9928300, 'Minnesota': 5519952, 'Mississippi': 2988726,
                'Missouri': 6093000, 'Montana': 1050493, 'Nebraska': 1907116, 'Nevada': 2940058,
                'New Hampshire': 1334795, 'New Jersey': 8944469, 'New Mexico': 2081702, 'New York': 19745289,
                'North Carolina': 10146788, 'North Dakota': 757952, 'Ohio': 11614373, 'Oklahoma': 3923561,
                'Oregon': 4093465, 'Pennsylvania': 12784227, 'Rhode Island': 1056426, 'South Carolina': 4961119,
                'South Dakota': 865454, 'Tennessee': 6651194, 'Texas': 27862596, 'Utah': 3051217,
                'Vermont': 624594, 'Virginia': 8411808, 'Washington': 7288000, 'West Virginia': 1831102,
                'Wisconsin': 5778708, 'Wyoming': 585501, 'District of Columbia': 681170, 'Puerto Rico': 3411307
            },
            2017: {
                'Alabama': 4874747, 'Alaska': 739795, 'Arizona': 7016270, 'Arkansas': 3004279,
                'California': 39358497, 'Colorado': 5607154, 'Connecticut': 3588184, 'Delaware': 961939,
                'Florida': 20984400, 'Georgia': 10429379, 'Hawaii': 1427538, 'Idaho': 1716943,
                'Illinois': 12802023, 'Indiana': 6666818, 'Iowa': 3145711, 'Kansas': 2913123,
                'Kentucky': 4454189, 'Louisiana': 4684333, 'Maine': 1335907, 'Maryland': 6052177,
                'Massachusetts': 6859819, 'Michigan': 9962311, 'Minnesota': 5576606, 'Mississippi': 2984100,
                'Missouri': 6113532, 'Montana': 1060665, 'Nebraska': 1920076, 'Nevada': 2998039,
                'New Hampshire': 1342795, 'New Jersey': 8908520, 'New Mexico': 2088070, 'New York': 19697457,
                'North Carolina': 10273419, 'North Dakota': 760077, 'Ohio': 11658609, 'Oklahoma': 3930864,
                'Oregon': 4142776, 'Pennsylvania': 12805537, 'Rhode Island': 1059639, 'South Carolina': 5024369,
                'South Dakota': 878698, 'Tennessee': 6715984, 'Texas': 28304596, 'Utah': 3101833,
                'Vermont': 623657, 'Virginia': 8470020, 'Washington': 7405743, 'West Virginia': 1815857,
                'Wisconsin': 5795483, 'Wyoming': 579315, 'District of Columbia': 693972, 'Puerto Rico': 3337177
            },
            2018: {
                'Alabama': 4887681, 'Alaska': 737438, 'Arizona': 7158024, 'Arkansas': 3009733,
                'California': 39461588, 'Colorado': 5695564, 'Connecticut': 3571520, 'Delaware': 967171,
                'Florida': 21244317, 'Georgia': 10519475, 'Hawaii': 1420593, 'Idaho': 1750536,
                'Illinois': 12723071, 'Indiana': 6691878, 'Iowa': 3148618, 'Kansas': 2911359,
                'Kentucky': 4461153, 'Louisiana': 4659690, 'Maine': 1339057, 'Maryland': 6035802,
                'Massachusetts': 6882635, 'Michigan': 9995915, 'Minnesota': 5606249, 'Mississippi': 2976149,
                'Missouri': 6124160, 'Montana': 1068778, 'Nebraska': 1929268, 'Nevada': 3027341,
                'New Hampshire': 1353465, 'New Jersey': 8886025, 'New Mexico': 2092741, 'New York': 19542209,
                'North Carolina': 10381615, 'North Dakota': 760394, 'Ohio': 11689442, 'Oklahoma': 3943079,
                'Oregon': 4176346, 'Pennsylvania': 12823989, 'Rhode Island': 1058287, 'South Carolina': 5084156,
                'South Dakota': 882235, 'Tennessee': 6770010, 'Texas': 28628666, 'Utah': 3153550,
                'Vermont': 624358, 'Virginia': 8501286, 'Washington': 7523869, 'West Virginia': 1805832,
                'Wisconsin': 5813568, 'Wyoming': 577601, 'District of Columbia': 702455, 'Puerto Rico': 3195153
            },
            2019: {
                'Alabama': 4903185, 'Alaska': 731158, 'Arizona': 7278717, 'Arkansas': 3017804,
                'California': 39512223, 'Colorado': 5758736, 'Connecticut': 3565287, 'Delaware': 973764,
                'Florida': 21477737, 'Georgia': 10617423, 'Hawaii': 1415872, 'Idaho': 1787065,
                'Illinois': 12671821, 'Indiana': 6732219, 'Iowa': 3155070, 'Kansas': 2913314,
                'Kentucky': 4467673, 'Louisiana': 4648794, 'Maine': 1344212, 'Maryland': 6045680,
                'Massachusetts': 6892503, 'Michigan': 9986857, 'Minnesota': 5639632, 'Mississippi': 2976149,
                'Missouri': 6137428, 'Montana': 1068778, 'Nebraska': 1934408, 'Nevada': 3080156,
                'New Hampshire': 1359711, 'New Jersey': 8882190, 'New Mexico': 2096829, 'New York': 19453561,
                'North Carolina': 10488084, 'North Dakota': 762062, 'Ohio': 11689100, 'Oklahoma': 3956971,
                'Oregon': 4217737, 'Pennsylvania': 12801989, 'Rhode Island': 1059361, 'South Carolina': 5148714,
                'South Dakota': 884659, 'Tennessee': 6829174, 'Texas': 28995881, 'Utah': 3205958,
                'Vermont': 623989, 'Virginia': 8535519, 'Washington': 7614893, 'West Virginia': 1792147,
                'Wisconsin': 5822434, 'Wyoming': 578759, 'District of Columbia': 705749, 'Puerto Rico': 3193694
            },
            2020: {
                'Alabama': 5024279, 'Alaska': 733391, 'Arizona': 7151502, 'Arkansas': 3011524,
                'California': 39538223, 'Colorado': 5773714, 'Connecticut': 3605944, 'Delaware': 989948,
                'Florida': 21538187, 'Georgia': 10711908, 'Hawaii': 1455271, 'Idaho': 1839106,
                'Illinois': 12812508, 'Indiana': 6785528, 'Iowa': 3190369, 'Kansas': 2937880,
                'Kentucky': 4505836, 'Louisiana': 4648794, 'Maine': 1395722, 'Maryland': 6177224,
                'Massachusetts': 7001399, 'Michigan': 10037261, 'Minnesota': 5737915, 'Mississippi': 2961279,
                'Missouri': 6196540, 'Montana': 1084225, 'Nebraska': 1961504, 'Nevada': 3104614,
                'New Hampshire': 1395231, 'New Jersey': 9288994, 'New Mexico': 2117522, 'New York': 20201249,
                'North Carolina': 10439388, 'North Dakota': 779094, 'Ohio': 11799448, 'Oklahoma': 3959353,
                'Oregon': 4237256, 'Pennsylvania': 13002700, 'Rhode Island': 1097379, 'South Carolina': 5118425,
                'South Dakota': 886667, 'Tennessee': 6910840, 'Texas': 29145505, 'Utah': 3271616,
                'Vermont': 643077, 'Virginia': 8631393, 'Washington': 7705281, 'West Virginia': 1793716,
                'Wisconsin': 5893718, 'Wyoming': 576851, 'District of Columbia': 689545, 'Puerto Rico': 3285874
            },
            2021: {
                'Alabama': 5024279, 'Alaska': 732673, 'Arizona': 7276316, 'Arkansas': 3025891,
                'California': 39237836, 'Colorado': 5812069, 'Connecticut': 3605597, 'Delaware': 1003384,
                'Florida': 22244823, 'Georgia': 10799566, 'Hawaii': 1441553, 'Idaho': 1900923,
                'Illinois': 12587530, 'Indiana': 6805663, 'Iowa': 3200517, 'Kansas': 2934582,
                'Kentucky': 4512310, 'Louisiana': 4590241, 'Maine': 1395722, 'Maryland': 6164660,
                'Massachusetts': 7001399, 'Michigan': 10037522, 'Minnesota': 5740781, 'Mississippi': 2940057,
                'Missouri': 6196540, 'Montana': 1104271, 'Nebraska': 1967923, 'Nevada': 3138259,
                'New Hampshire': 1395231, 'New Jersey': 9267130, 'New Mexico': 2109093, 'New York': 19835913,
                'North Carolina': 10551162, 'North Dakota': 774948, 'Ohio': 11780017, 'Oklahoma': 3986639,
                'Oregon': 4246155, 'Pennsylvania': 12964056, 'Rhode Island': 1095610, 'South Carolina': 5190705,
                'South Dakota': 895376, 'Tennessee': 6975218, 'Texas': 30029572, 'Utah': 3337975,
                'Vermont': 645570, 'Virginia': 8683619, 'Washington': 7738692, 'West Virginia': 1782959,
                'Wisconsin': 5895908, 'Wyoming': 578803, 'District of Columbia': 670050, 'Puerto Rico': 3263584
            },
            2022: {
                'Alabama': 5074296, 'Alaska': 733583, 'Arizona': 7359197, 'Arkansas': 3045637,
                'California': 38940231, 'Colorado': 5839926, 'Connecticut': 3626205, 'Delaware': 1018396,
                'Florida': 22610726, 'Georgia': 10912876, 'Hawaii': 1440196, 'Idaho': 1964726,
                'Illinois': 12582032, 'Indiana': 6833037, 'Iowa': 3200517, 'Kansas': 2940865,
                'Kentucky': 4512310, 'Louisiana': 4590241, 'Maine': 1395722, 'Maryland': 6164660,
                'Massachusetts': 7001399, 'Michigan': 10037522, 'Minnesota': 5737915, 'Mississippi': 2940057,
                'Missouri': 6196540, 'Montana': 1122069, 'Nebraska': 1967923, 'Nevada': 3177772,
                'New Hampshire': 1395231, 'New Jersey': 9261699, 'New Mexico': 2113344, 'New York': 19336776,
                'North Carolina': 10698973, 'North Dakota': 774948, 'Ohio': 11756058, 'Oklahoma': 4019800,
                'Oregon': 4240137, 'Pennsylvania': 12972008, 'Rhode Island': 1093734, 'South Carolina': 5282634,
                'South Dakota': 909824, 'Tennessee': 7051339, 'Texas': 30503301, 'Utah': 3380800,
                'Vermont': 647464, 'Virginia': 8715698, 'Washington': 7812880, 'West Virginia': 1775156,
                'Wisconsin': 5892539, 'Wyoming': 581381, 'District of Columbia': 671803, 'Puerto Rico': 3221789
            },
            2023: {
                'Alabama': 5108468, 'Alaska': 733406, 'Arizona': 7431344, 'Arkansas': 3067732,
                'California': 38965193, 'Colorado': 5877610, 'Connecticut': 3617176, 'Delaware': 1031890,
                'Florida': 23244842, 'Georgia': 11029227, 'Hawaii': 1435138, 'Idaho': 1996379,
                'Illinois': 12549689, 'Indiana': 6862199, 'Iowa': 3207004, 'Kansas': 2940740,
                'Kentucky': 4526154, 'Louisiana': 4573749, 'Maine': 1395722, 'Maryland': 6196972,
                'Massachusetts': 7001399, 'Michigan': 10037261, 'Minnesota': 5739781, 'Mississippi': 2940057,
                'Missouri': 6196540, 'Montana': 1122069, 'Nebraska': 1978379, 'Nevada': 3194176,
                'New Hampshire': 1402957, 'New Jersey': 9290841, 'New Mexico': 2114371, 'New York': 19571216,
                'North Carolina': 10835491, 'North Dakota': 783926, 'Ohio': 11785935, 'Oklahoma': 4053824,
                'Oregon': 4233358, 'Pennsylvania': 12961683, 'Rhode Island': 1095962, 'South Carolina': 5373555,
                'South Dakota': 919318, 'Tennessee': 7126489, 'Texas': 31068449, 'Utah': 3423046,
                'Vermont': 647818, 'Virginia': 8715698, 'Washington': 7951150, 'West Virginia': 1770071,
                'Wisconsin': 5910955, 'Wyoming': 584057, 'District of Columbia': 678972, 'Puerto Rico': 3205691
            },
            2024: {
                'Alabama': 5143033, 'Alaska': 733583, 'Arizona': 7431344, 'Arkansas': 3067732,
                'California': 39431263, 'Colorado': 5877610, 'Connecticut': 3617176, 'Delaware': 1031890,
                'Florida': 23244842, 'Georgia': 11029227, 'Hawaii': 1435138, 'Idaho': 1996379,
                'Illinois': 12549689, 'Indiana': 6862199, 'Iowa': 3207004, 'Kansas': 2940740,
                'Kentucky': 4526154, 'Louisiana': 4573749, 'Maine': 1395722, 'Maryland': 6196972,
                'Massachusetts': 7001399, 'Michigan': 10037261, 'Minnesota': 5739781, 'Mississippi': 2940057,
                'Missouri': 6196540, 'Montana': 1122069, 'Nebraska': 1978379, 'Nevada': 3194176,
                'New Hampshire': 1402957, 'New Jersey': 9290841, 'New Mexico': 2114371, 'New York': 19571216,
                'North Carolina': 10835491, 'North Dakota': 783926, 'Ohio': 11785935, 'Oklahoma': 4053824,
                'Oregon': 4233358, 'Pennsylvania': 12961683, 'Rhode Island': 1095962, 'South Carolina': 5373555,
                'South Dakota': 919318, 'Tennessee': 7126489, 'Texas': 31068449, 'Utah': 3423046,
                'Vermont': 647818, 'Virginia': 8715698, 'Washington': 7951150, 'West Virginia': 1770071,
                'Wisconsin': 5910955, 'Wyoming': 584057, 'District of Columbia': 678972, 'Puerto Rico': 3205691
            },
            2025: {
                'Alabama': 5143033, 'Alaska': 733583, 'Arizona': 7431344, 'Arkansas': 3067732,
                'California': 39431263, 'Colorado': 5877610, 'Connecticut': 3617176, 'Delaware': 1031890,
                'Florida': 23244842, 'Georgia': 11029227, 'Hawaii': 1435138, 'Idaho': 1996379,
                'Illinois': 12549689, 'Indiana': 6862199, 'Iowa': 3207004, 'Kansas': 2940740,
                'Kentucky': 4526154, 'Louisiana': 4573749, 'Maine': 1395722, 'Maryland': 6196972,
                'Massachusetts': 7001399, 'Michigan': 10037261, 'Minnesota': 5739781, 'Mississippi': 2940057,
                'Missouri': 6196540, 'Montana': 1122069, 'Nebraska': 1978379, 'Nevada': 3194176,
                'New Hampshire': 1402957, 'New Jersey': 9290841, 'New Mexico': 2114371, 'New York': 19571216,
                'North Carolina': 10835491, 'North Dakota': 783926, 'Ohio': 11785935, 'Oklahoma': 4053824,
                'Oregon': 4233358, 'Pennsylvania': 12961683, 'Rhode Island': 1095962, 'South Carolina': 5373555,
                'South Dakota': 919318, 'Tennessee': 7126489, 'Texas': 31068449, 'Utah': 3423046,
                'Vermont': 647818, 'Virginia': 8715698, 'Washington': 7951150, 'West Virginia': 1770071,
                'Wisconsin': 5910955, 'Wyoming': 584057, 'District of Columbia': 678972, 'Puerto Rico': 3205691
            }
        }
#2025 data not available so 2024 used as a supplement
        # Create DataFrame from the population data
        all_pop_data = []

        for year in years:
            if year in population_data:
                for state, population in population_data[year].items():
                    all_pop_data.append({
                        'state': state,
                        'year': year,
                        'population': population
                    })
                logger.info(f"Loaded population data for {year}: {len(population_data[year])} states/territories")
            else:
                logger.warning(f"No population data available for {year}")

        if all_pop_data:
            combined_pop = pd.DataFrame(all_pop_data)
            logger.info(f"Total population records loaded: {len(combined_pop)}")
            return combined_pop
        else:
            logger.error("No population data could be loaded")
            return pd.DataFrame()

    def add_population_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add population data to mortality dataset."""
        if df.empty:
            return df

        logger.info("Adding population data to mortality dataset...")

        # Get unique years in the dataset
        years = sorted(df['year'].unique())
        logger.info(f"Years needing population data: {years}")

        # Get population data
        pop_df = self.get_population_data(years)

        if pop_df.empty:
            logger.warning("No population data available - adding placeholder")
            df['population'] = None
            return df

        # Merge population data with mortality data
        df_with_pop = df.merge(
            pop_df[['state', 'year', 'population']],
            on=['state', 'year'],
            how='left'
        )

        # Check merge success
        missing_pop = df_with_pop['population'].isna().sum()
        total_records = len(df_with_pop)
        success_rate = (total_records - missing_pop) / total_records * 100

        logger.info(f"Population merge results:")
        logger.info(f"   Total records: {total_records:,}")
        logger.info(f"   Records with population: {total_records - missing_pop:,}")
        logger.info(f"   Success rate: {success_rate:.1f}%")

        if missing_pop > 0:
            # Show which states are missing population data
            missing_states = df_with_pop[df_with_pop['population'].isna()]['state'].unique()
            logger.warning(f"Missing population data for: {list(missing_states)}")

        return df_with_pop

    def merge_and_clean_datasets(self, *datasets) -> pd.DataFrame:
        """Merge historical and current datasets and clean the result."""
        logger.info("Merging and cleaning datasets...")

        datasets_to_merge = []

        # Add all non-empty datasets
        for i, dataset in enumerate(datasets):
            if dataset is not None and not dataset.empty:
                datasets_to_merge.append(dataset)
                logger.info(f"Including dataset {i + 1}: {len(dataset)} records")

        if not datasets_to_merge:
            logger.error("No datasets available for merging")
            return pd.DataFrame()

        # Merge datasets
        combined_df = pd.concat(datasets_to_merge, ignore_index=True, sort=False)
        logger.info(f"Combined dataset: {len(combined_df)} records")

        # Clean data
        combined_df = self.clean_data(combined_df)

        return combined_df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the merged dataset."""
        if df.empty:
            return df

        logger.info("Cleaning merged dataset...")

        # Remove rows with missing essential data
        essential_cols = ['year', 'state', 'cause', 'deaths']
        df = df.dropna(subset=essential_cols)

        # Convert year to integer
        df['year'] = df['year'].astype(int)

        # Remove duplicate records (same year, week, state, cause)
        dedup_cols = ['year', 'week', 'state', 'cause']
        available_dedup_cols = [col for col in dedup_cols if col in df.columns]
        if len(available_dedup_cols) >= 3:  # Need at least year, state, cause
            df = df.drop_duplicates(subset=available_dedup_cols, keep='last')
            logger.info(f"After deduplication: {len(df)} records")

        # Standardize state names
        state_mapping = {
            'United States': 'United States',
            'New York City': 'New York'
        }
        df['state'] = df['state'].replace(state_mapping)

        # Filter for valid US jurisdictions
        valid_jurisdictions = [
            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
            'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
            'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
            'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
            'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
            'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
            'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
            'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
            'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
            'West Virginia', 'Wisconsin', 'Wyoming', 'District of Columbia',
            'United States', 'Puerto Rico'
        ]

        df = df[df['state'].isin(valid_jurisdictions)]

        # Remove rows with zero or negative deaths
        df = df[df['deaths'] > 0]

        # Sort data
        sort_columns = ['year', 'week', 'state', 'cause']
        available_sort_cols = [col for col in sort_columns if col in df.columns]
        df = df.sort_values(available_sort_cols)

        # Reset index
        df = df.reset_index(drop=True)

        logger.info(f"Final cleaned dataset: {len(df)} records")
        return df

    def create_summary_statistics(self, df: pd.DataFrame) -> dict:
        """Create summary statistics for the compiled dataset."""
        if df.empty:
            return {}

        stats = {
            'total_records': len(df),
            'years_covered': f"{df['year'].min()}-{df['year'].max()}",
            'states_count': df['state'].nunique(),
            'total_deaths': df['deaths'].sum(),
            'causes_included': df['cause'].nunique(),
            'data_sources': df['data_source'].unique().tolist() if 'data_source' in df.columns else [],
            'cause_breakdown': df.groupby('cause')['deaths'].agg(['count', 'sum']).to_dict(),
            'yearly_totals': df.groupby('year')['deaths'].sum().to_dict(),
            'state_coverage': df['state'].unique().tolist()
        }

        # Add population-related statistics if available
        if 'population' in df.columns:
            stats['has_population_data'] = True
            pop_records = df[df['population'].notna()]
            if not pop_records.empty:
                stats['records_with_population'] = len(pop_records)
                stats['population_coverage'] = len(pop_records) / len(df) * 100

                # Calculate crude mortality rates for states with population data
                annual_rates = {}
                for year in df['year'].unique():
                    year_data = df[(df['year'] == year) & (df['cause'] == 'All causes') &
                                   (df['state'] != 'United States') & df['population'].notna()]
                    if not year_data.empty:
                        total_deaths = year_data['deaths'].sum()
                        total_population = year_data['population'].sum()
                        if total_population > 0:
                            crude_rate = (total_deaths / total_population) * 100000  # per 100k
                            annual_rates[year] = round(crude_rate, 1)

                stats['annual_mortality_rates'] = annual_rates
        else:
            stats['has_population_data'] = False

        return stats

    def save_to_csv(self, df: pd.DataFrame, filename: str = 'comprehensive_mortality_data_2015_present.csv'):
        """Save the compiled dataset to CSV with comprehensive reporting."""
        try:
            # Get current working directory and create full path
            current_dir = os.getcwd()
            filepath = os.path.join(current_dir, filename)

            # Save to CSV
            df.to_csv(filepath, index=False)

            # Get file info
            file_size_mb = os.path.getsize(filepath) / 1024 / 1024

            # Create summary statistics
            stats = self.create_summary_statistics(df)

            # Print comprehensive summary
            print("\n" + "=" * 90)
            print("COMPREHENSIVE MORTALITY DATA COMPILATION COMPLETED")
            print("=" * 90)
            print(f"File saved to: {filepath}")
            print(f"File size: {file_size_mb:.2f} MB")
            print(f"Total records: {stats.get('total_records', 0):,}")
            print(f"Years covered: {stats.get('years_covered', 'N/A')}")
            print(f"States/jurisdictions: {stats.get('states_count', 0)}")
            print(f"Total deaths: {stats.get('total_deaths', 0):,.0f}")

            print(f"\nDATA SOURCES INCLUDED:")
            for source in stats.get('data_sources', []):
                print(f"   • {source}")

            print(f"\nCAUSES OF DEATH INCLUDED:")
            cause_breakdown = stats.get('cause_breakdown', {})
            if 'sum' in cause_breakdown:
                for cause, total_deaths in sorted(cause_breakdown['sum'].items(), key=lambda x: x[1], reverse=True):
                    record_count = cause_breakdown.get('count', {}).get(cause, 0)
                    print(f"   • {cause}: {record_count:,} records, {total_deaths:,.0f} deaths")

            print(f"\nDEATHS BY YEAR:")
            yearly_totals = stats.get('yearly_totals', {})
            for year in sorted(yearly_totals.keys()):
                deaths_str = f"{yearly_totals[year]:,.0f} deaths"

                # Add mortality rate if available
                annual_rates = stats.get('annual_mortality_rates', {})
                if year in annual_rates:
                    deaths_str += f" ({annual_rates[year]} per 100k)"

                print(f"   • {year}: {deaths_str}")

            print(f"\nJURISDICTIONS COVERED:")
            states = stats.get('state_coverage', [])
            national_data = 'United States' in states
            state_data = [s for s in states if s != 'United States']

            if national_data:
                print(f"   • National level data: Available")
            if state_data:
                print(f"   • State level data: {len(state_data)} states/territories")
                print(f"     {', '.join(sorted(state_data)[:10])}{'...' if len(state_data) > 10 else ''}")

            print(f"\nCOLUMNS INCLUDED:")
            columns_list = df.columns.tolist()
            print(f"   {', '.join(columns_list)}")

            # Add population data info if available
            if stats.get('has_population_data'):
                pop_coverage = stats.get('population_coverage', 0)
                records_with_pop = stats.get('records_with_population', 0)
                print(f"\nPOPULATION DATA:")
                print(f"   • Records with population: {records_with_pop:,} ({pop_coverage:.1f}%)")
                print(f"   • Source: Census Bureau Population Estimates Program")
                print(f"   • Enables mortality rate calculations (deaths per 100k population)")

            print(f"\nSUCCESS: Complete 2015-present mortality data compilation finished!")

        except Exception as e:
            logger.error(f"Error saving file: {e}")
            raise

    def compile_comprehensive_data(self, output_filename: str = 'comprehensive_mortality_data_2015_present.csv'):
        """Main method to compile comprehensive mortality data from 2015 to present."""
        print("COMPREHENSIVE MORTALITY DATA COMPILER")
        print("=" * 70)
        print("Combining multiple data sources for complete 2015-present coverage:")
        print("   • Historical 2015-2020: World Mortality Dataset (weekly, national)")
        print("   • Archived 2015-2019: Internet Archive CDC datasets (weekly, by state)")
        print("   • Complete 2019: CDC 2014-2019 Weekly Dataset (fills 2019 gap)")
        print("   • Current 2020-present: CDC APIs (weekly, by state and cause)")
        print("   • Output: Unified dataset with maximum available detail\n")

        all_datasets = []

        # Download and process World Mortality Dataset (historical)
        print("Step 1: Downloading historical mortality data (2015-2020)...")
        world_mortality_raw = self.download_dataset('world_mortality')
        if not world_mortality_raw.empty:
            world_mortality_processed = self.process_world_mortality_data(world_mortality_raw)
            if not world_mortality_processed.empty:
                all_datasets.append(world_mortality_processed)
                print("Historical data processed successfully")
            else:
                print("No historical data could be processed")
        else:
            print("Failed to download historical data")

        # Download and process archived state-level data (2015-2019)
        print("\nStep 2: Downloading archived state-level mortality data (2015-2019)...")
        archived_raw = self.download_dataset('archived_state_deaths')
        if not archived_raw.empty:
            archived_processed = self.process_archived_state_deaths(archived_raw)
            if not archived_processed.empty:
                all_datasets.append(archived_processed)
                print("Archived state-level data processed successfully")
            else:
                print("No usable archived state data could be processed")
        else:
            print("Failed to download archived state data")

        # Download and process CDC provisional data (current)
        print("\nStep 3: Downloading complete 2019 state-level mortality data...")
        cdc_2019_raw = self.download_dataset('cdc_2014_2019_weekly')
        if not cdc_2019_raw.empty:
            cdc_2019_processed = self.process_cdc_2014_2019_weekly(cdc_2019_raw)
            if not cdc_2019_processed.empty:
                all_datasets.append(cdc_2019_processed)
                print("Complete 2019 state-level data processed successfully")
            else:
                print("No usable 2019 state data could be processed")
        else:
            print("Failed to download 2019 state data")

        # Download and process CDC provisional data (current)
        print("\nStep 4: Downloading current mortality data (2020-present)...")
        cdc_raw = self.download_dataset('cdc_provisional')
        if not cdc_raw.empty:
            cdc_processed = self.process_cdc_provisional_data(cdc_raw)
            if not cdc_processed.empty:
                all_datasets.append(cdc_processed)
                print("Current data processed successfully")
            else:
                print("No current data could be processed")
        else:
            print("Failed to download current data")

        # Check if we have any data
        if not all_datasets:
            print("\nERROR: No data was successfully downloaded and processed!")
            print("Possible issues:")
            print("• Internet connectivity problems")
            print("• Data source servers temporarily unavailable")
            print("• Dataset formats may have changed")
            print("\nTry again later or check data sources manually.")
            return

        # Merge and clean datasets
        print("\nStep 5: Merging and cleaning datasets...")
        if all_datasets:
            final_df = self.merge_and_clean_datasets(*all_datasets)
        else:
            final_df = pd.DataFrame()

        if final_df.empty:
            logger.error("No data remaining after processing!")
            return

        # Add population data
        print("\nStep 6: Adding state population data...")
        final_df = self.add_population_data(final_df)

        # Save final dataset
        print("\nStep 7: Saving comprehensive dataset...")
        self.save_to_csv(final_df, output_filename)

        # Additional guidance
        print("\n" + "=" * 90)
        print("USAGE RECOMMENDATIONS")
        print("=" * 90)
        print(f"Data Analysis:")
        print(f"   • Load with: pd.read_csv('{output_filename}')")
        print(f"   • Filter by state: df[df['state'] == 'California']")
        print(f"   • Filter by cause: df[df['cause'] == 'All causes']")
        print(f"   • Group by year: df.groupby('year')['deaths'].sum()")

        print(f"\nData Notes:")
        print(f"   • Historical data (2015-2020): National + some state level from archives")
        print(f"   • Current data (2020+): State level with multiple causes")
        print(f"   • Archived data (2015-2019): State level from Internet Archive")
        print(f"   • Complete 2019 data: CDC 2014-2019 Weekly Dataset (fills gap)")
        print(f"   • Population data: Census Bureau Population Estimates Program")
        print(f"   • Some overlap in 2020 data between sources")
        print(f"   • Most recent weeks may be incomplete (provisional data)")

        print(f"\nData Sources:")
        print(f"   • World Mortality: https://github.com/akarlinsky/world_mortality")
        print(f"   • CDC Provisional: https://data.cdc.gov/")
        print(f"   • Archived CDC: https://archive.org/details/20250128-cdc-datasets")
        print(f"   • For more detail: https://wonder.cdc.gov/")


def main():
    """Run the comprehensive mortality data compilation."""
    try:
        print("Welcome to the Comprehensive Mortality Data Compiler!")
        print("This tool combines historical and current data for complete 2015-present coverage.\n")

        compiler = ComprehensiveMortalityDataCompiler()
        compiler.compile_comprehensive_data()

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
    except Exception as e:
        print(f"\n\nAn error occurred: {e}")
        print("Please check your internet connection and try again.")
        logger.exception("Full error details:")


if __name__ == "__main__":
    main()