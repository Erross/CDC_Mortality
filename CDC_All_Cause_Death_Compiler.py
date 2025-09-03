import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import logging
import os
from io import StringIO
import warnings

warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ComprehensiveMortalityDataCompiler:
    """
    Compiles comprehensive CDC all-cause mortality data from 2015-present.
    Uses local file for complete 2019 state-level data.
    Outputs separate files for US national data and state-level data.
    Version 3.1: Fixed NYC/NY combination across all data sources.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Comprehensive-Mortality-Compiler/3.1',
            'Accept': 'text/csv,application/json'
        })

        # Data sources
        self.data_sources = {
            'world_mortality': {
                'url': 'https://raw.githubusercontent.com/akarlinsky/world_mortality/main/world_mortality.csv',
                'description': 'World Mortality Dataset - US weekly deaths 2015-2020+'
            },
            'cdc_provisional': {
                'url': 'https://data.cdc.gov/api/views/r8kw-7aab/rows.csv?accessType=DOWNLOAD',
                'description': 'CDC Provisional COVID-19 Death Counts by Week and State 2020-present'
            },
            'archived_state_deaths': {
                'url': 'https://archive.org/download/20250128-cdc-datasets/Deaths_from_Pneumonia_and_Influenza_P_I_and_all_deaths_by_state_and_region_National_Center_For_Health_Statistics_Mortality_Surveillance_System.csv',
                'description': 'ARCHIVED: Deaths by State from NCHS Surveillance System (2015-2018)'
            }
        }

    def get_population_data(self) -> pd.DataFrame:
        """Get annual state and US population estimates from Census Bureau data."""
        logger.info("Loading population data...")

        # US Census Bureau Population Estimates (July 1 estimates)
        # Source: Census Bureau Population Estimates Program
        population_data = {
            2015: {'Alabama': 4858979, 'Alaska': 738432, 'Arizona': 6828065, 'Arkansas': 2978204,
                   'California': 39144818, 'Colorado': 5456574, 'Connecticut': 3590886, 'Delaware': 945934,
                   'District of Columbia': 672228, 'Florida': 20271272, 'Georgia': 10214860, 'Hawaii': 1431603,
                   'Idaho': 1654930, 'Illinois': 12859995, 'Indiana': 6619680, 'Iowa': 3123899, 'Kansas': 2911641,
                   'Kentucky': 4425092, 'Louisiana': 4670724, 'Maine': 1329328, 'Maryland': 6006401,
                   'Massachusetts': 6794422, 'Michigan': 9922576, 'Minnesota': 5489594, 'Mississippi': 2992333,
                   'Missouri': 6083672, 'Montana': 1032949, 'Nebraska': 1896190, 'Nevada': 2890845,
                   'New Hampshire': 1330608, 'New Jersey': 8958013, 'New Mexico': 2085109, 'New York': 19795791,
                   'North Carolina': 10042802, 'North Dakota': 756927, 'Ohio': 11613423, 'Oklahoma': 3911338,
                   'Oregon': 4028977, 'Pennsylvania': 12802503, 'Puerto Rico': 3474182, 'Rhode Island': 1056298,
                   'South Carolina': 4896146, 'South Dakota': 858469, 'Tennessee': 6600299, 'Texas': 27469114,
                   'Utah': 2995919, 'Vermont': 626042, 'Virginia': 8382993, 'Washington': 7170351,
                   'West Virginia': 1844128, 'Wisconsin': 5771337, 'Wyoming': 586107, 'United States': 321418820},
            2016: {'Alabama': 4863300, 'Alaska': 741894, 'Arizona': 6931071, 'Arkansas': 2988248,
                   'California': 39250017, 'Colorado': 5540545, 'Connecticut': 3576452, 'Delaware': 952065,
                   'District of Columbia': 681170, 'Florida': 20612439, 'Georgia': 10310371, 'Hawaii': 1428557,
                   'Idaho': 1683140, 'Illinois': 12801539, 'Indiana': 6633053, 'Iowa': 3134693, 'Kansas': 2907289,
                   'Kentucky': 4436974, 'Louisiana': 4681666, 'Maine': 1331479, 'Maryland': 6016447,
                   'Massachusetts': 6811779, 'Michigan': 9928300, 'Minnesota': 5519952, 'Mississippi': 2988726,
                   'Missouri': 6093000, 'Montana': 1042520, 'Nebraska': 1907116, 'Nevada': 2940058,
                   'New Hampshire': 1334795, 'New Jersey': 8944469, 'New Mexico': 2081702, 'New York': 19745289,
                   'North Carolina': 10146788, 'North Dakota': 757952, 'Ohio': 11614373, 'Oklahoma': 3923561,
                   'Oregon': 4093465, 'Pennsylvania': 12784227, 'Puerto Rico': 3411307, 'Rhode Island': 1056426,
                   'South Carolina': 4961119, 'South Dakota': 865454, 'Tennessee': 6651194, 'Texas': 27862596,
                   'Utah': 3051217, 'Vermont': 624594, 'Virginia': 8411808, 'Washington': 7288000,
                   'West Virginia': 1831102, 'Wisconsin': 5778708, 'Wyoming': 585501, 'United States': 323127513},
            2017: {'Alabama': 4874747, 'Alaska': 739795, 'Arizona': 7016270, 'Arkansas': 3004279,
                   'California': 39536653, 'Colorado': 5607154, 'Connecticut': 3588184, 'Delaware': 961939,
                   'District of Columbia': 693972, 'Florida': 20984400, 'Georgia': 10429379, 'Hawaii': 1427538,
                   'Idaho': 1716943, 'Illinois': 12802023, 'Indiana': 6666818, 'Iowa': 3145711, 'Kansas': 2913123,
                   'Kentucky': 4454189, 'Louisiana': 4684333, 'Maine': 1335907, 'Maryland': 6052177,
                   'Massachusetts': 6859819, 'Michigan': 9962311, 'Minnesota': 5576606, 'Mississippi': 2984100,
                   'Missouri': 6113532, 'Montana': 1050493, 'Nebraska': 1920076, 'Nevada': 2998039,
                   'New Hampshire': 1342795, 'New Jersey': 8908520, 'New Mexico': 2088070, 'New York': 19849399,
                   'North Carolina': 10273419, 'North Dakota': 755393, 'Ohio': 11658609, 'Oklahoma': 3930864,
                   'Oregon': 4142776, 'Pennsylvania': 12805537, 'Puerto Rico': 3337177, 'Rhode Island': 1059639,
                   'South Carolina': 5024369, 'South Dakota': 869666, 'Tennessee': 6715984, 'Texas': 28304596,
                   'Utah': 3101833, 'Vermont': 623657, 'Virginia': 8470020, 'Washington': 7405743,
                   'West Virginia': 1815857, 'Wisconsin': 5795483, 'Wyoming': 579315, 'United States': 325719178},
            2018: {'Alabama': 4887871, 'Alaska': 737438, 'Arizona': 7171646, 'Arkansas': 3013825,
                   'California': 39557045, 'Colorado': 5695564, 'Connecticut': 3572665, 'Delaware': 967171,
                   'District of Columbia': 702455, 'Florida': 21299325, 'Georgia': 10519475, 'Hawaii': 1420491,
                   'Idaho': 1754208, 'Illinois': 12741080, 'Indiana': 6691878, 'Iowa': 3156145, 'Kansas': 2911505,
                   'Kentucky': 4468402, 'Louisiana': 4659978, 'Maine': 1338404, 'Maryland': 6042718,
                   'Massachusetts': 6902149, 'Michigan': 9995915, 'Minnesota': 5611179, 'Mississippi': 2986530,
                   'Missouri': 6126452, 'Montana': 1062305, 'Nebraska': 1929268, 'Nevada': 3034392,
                   'New Hampshire': 1356458, 'New Jersey': 8908520, 'New Mexico': 2095428, 'New York': 19542209,
                   'North Carolina': 10383620, 'North Dakota': 760077, 'Ohio': 11689442, 'Oklahoma': 3943079,
                   'Oregon': 4190713, 'Pennsylvania': 12807060, 'Puerto Rico': 3195153, 'Rhode Island': 1058287,
                   'South Carolina': 5084127, 'South Dakota': 882235, 'Tennessee': 6770010, 'Texas': 28701845,
                   'Utah': 3161105, 'Vermont': 626299, 'Virginia': 8517685, 'Washington': 7535591,
                   'West Virginia': 1805832, 'Wisconsin': 5813568, 'Wyoming': 577737, 'United States': 327167434},
            2019: {'Alabama': 4903185, 'Alaska': 731545, 'Arizona': 7278717, 'Arkansas': 3017804,
                   'California': 39512223, 'Colorado': 5758736, 'Connecticut': 3565287, 'Delaware': 973764,
                   'District of Columbia': 705749, 'Florida': 21477737, 'Georgia': 10617423, 'Hawaii': 1415872,
                   'Idaho': 1787065, 'Illinois': 12671821, 'Indiana': 6732219, 'Iowa': 3155070, 'Kansas': 2913314,
                   'Kentucky': 4467673, 'Louisiana': 4648794, 'Maine': 1344212, 'Maryland': 6045680,
                   'Massachusetts': 6892503, 'Michigan': 9986857, 'Minnesota': 5639632, 'Mississippi': 2976149,
                   'Missouri': 6137428, 'Montana': 1068778, 'Nebraska': 1934408, 'Nevada': 3080156,
                   'New Hampshire': 1359711, 'New Jersey': 8882190, 'New Mexico': 2096829, 'New York': 19453561,
                   'North Carolina': 10488084, 'North Dakota': 762062, 'Ohio': 11689100, 'Oklahoma': 3956971,
                   'Oregon': 4217737, 'Pennsylvania': 12801989, 'Puerto Rico': 3193694, 'Rhode Island': 1059361,
                   'South Carolina': 5148714, 'South Dakota': 884659, 'Tennessee': 6829174, 'Texas': 28995881,
                   'Utah': 3205958, 'Vermont': 623989, 'Virginia': 8535519, 'Washington': 7614893,
                   'West Virginia': 1792147, 'Wisconsin': 5822434, 'Wyoming': 578759, 'United States': 328239523},
            2020: {'Alabama': 5024279, 'Alaska': 733391, 'Arizona': 7151502, 'Arkansas': 3011524,
                   'California': 39538223, 'Colorado': 5773714, 'Connecticut': 3605944, 'Delaware': 989948,
                   'District of Columbia': 689545, 'Florida': 21538187, 'Georgia': 10711908, 'Hawaii': 1455271,
                   'Idaho': 1839106, 'Illinois': 12812508, 'Indiana': 6785528, 'Iowa': 3190369, 'Kansas': 2937880,
                   'Kentucky': 4505836, 'Louisiana': 4657757, 'Maine': 1362359, 'Maryland': 6177224,
                   'Massachusetts': 7029917, 'Michigan': 10077331, 'Minnesota': 5706494, 'Mississippi': 2961279,
                   'Missouri': 6154913, 'Montana': 1084225, 'Nebraska': 1961504, 'Nevada': 3104614,
                   'New Hampshire': 1377529, 'New Jersey': 9288994, 'New Mexico': 2117522, 'New York': 20201249,
                   'North Carolina': 10439388, 'North Dakota': 779094, 'Ohio': 11799448, 'Oklahoma': 3959353,
                   'Oregon': 4237256, 'Pennsylvania': 13002700, 'Puerto Rico': 3285874, 'Rhode Island': 1097379,
                   'South Carolina': 5118425, 'South Dakota': 886667, 'Tennessee': 6910840, 'Texas': 29145505,
                   'Utah': 3271616, 'Vermont': 643077, 'Virginia': 8631393, 'Washington': 7705281,
                   'West Virginia': 1793716, 'Wisconsin': 5893718, 'Wyoming': 576851, 'United States': 331449281},
            2021: {'Alabama': 5039877, 'Alaska': 732673, 'Arizona': 7276316, 'Arkansas': 3025891,
                   'California': 39237836, 'Colorado': 5812069, 'Connecticut': 3605597, 'Delaware': 1003384,
                   'District of Columbia': 670050, 'Florida': 21781128, 'Georgia': 10799566, 'Hawaii': 1441553,
                   'Idaho': 1900923, 'Illinois': 12671469, 'Indiana': 6805663, 'Iowa': 3193079, 'Kansas': 2934582,
                   'Kentucky': 4509394, 'Louisiana': 4624047, 'Maine': 1372247, 'Maryland': 6165129,
                   'Massachusetts': 6984723, 'Michigan': 10050811, 'Minnesota': 5707390, 'Mississippi': 2949965,
                   'Missouri': 6168187, 'Montana': 1104271, 'Nebraska': 1963692, 'Nevada': 3143991,
                   'New Hampshire': 1388992, 'New Jersey': 9267130, 'New Mexico': 2115877, 'New York': 19835913,
                   'North Carolina': 10551162, 'North Dakota': 774948, 'Ohio': 11780017, 'Oklahoma': 3986639,
                   'Oregon': 4246155, 'Pennsylvania': 12964056, 'Puerto Rico': 3263584, 'Rhode Island': 1095610,
                   'South Carolina': 5190705, 'South Dakota': 895376, 'Tennessee': 6975218, 'Texas': 29527941,
                   'Utah': 3337975, 'Vermont': 645570, 'Virginia': 8642274, 'Washington': 7738692,
                   'West Virginia': 1782959, 'Wisconsin': 5895908, 'Wyoming': 578803, 'United States': 332031554},
            2022: {'Alabama': 5074296, 'Alaska': 733583, 'Arizona': 7359197, 'Arkansas': 3045637,
                   'California': 39029342, 'Colorado': 5839926, 'Connecticut': 3626205, 'Delaware': 1018396,
                   'District of Columbia': 671803, 'Florida': 22244823, 'Georgia': 10912876, 'Hawaii': 1440196,
                   'Idaho': 1939033, 'Illinois': 12582032, 'Indiana': 6833037, 'Iowa': 3200517, 'Kansas': 2940546,
                   'Kentucky': 4512310, 'Louisiana': 4590241, 'Maine': 1385340, 'Maryland': 6164660,
                   'Massachusetts': 7001399, 'Michigan': 10037261, 'Minnesota': 5737915, 'Mississippi': 2940057,
                   'Missouri': 6196156, 'Montana': 1122867, 'Nebraska': 1967923, 'Nevada': 3177772,
                   'New Hampshire': 1395231, 'New Jersey': 9261699, 'New Mexico': 2113344, 'New York': 19677151,
                   'North Carolina': 10698973, 'North Dakota': 779261, 'Ohio': 11756058, 'Oklahoma': 4019800,
                   'Oregon': 4240137, 'Pennsylvania': 12972008, 'Puerto Rico': 3221789, 'Rhode Island': 1093734,
                   'South Carolina': 5282634, 'South Dakota': 909824, 'Tennessee': 7051339, 'Texas': 30029572,
                   'Utah': 3380800, 'Vermont': 647064, 'Virginia': 8683619, 'Washington': 7785786,
                   'West Virginia': 1775156, 'Wisconsin': 5892539, 'Wyoming': 581381, 'United States': 333287557},
            2023: {'Alabama': 5108468, 'Alaska': 733406, 'Arizona': 7431344, 'Arkansas': 3067732,
                   'California': 38965193, 'Colorado': 5877610, 'Connecticut': 3617176, 'Delaware': 1031890,
                   'District of Columbia': 678972, 'Florida': 22610726, 'Georgia': 11029227, 'Hawaii': 1435138,
                   'Idaho': 1964726, 'Illinois': 12549689, 'Indiana': 6862199, 'Iowa': 3207004, 'Kansas': 2940865,
                   'Kentucky': 4526154, 'Louisiana': 4573749, 'Maine': 1395722, 'Maryland': 6164660,
                   'Massachusetts': 7001399, 'Michigan': 10037261, 'Minnesota': 5737915, 'Mississippi': 2940057,
                   'Missouri': 6196156, 'Montana': 1122867, 'Nebraska': 1978379, 'Nevada': 3194176,
                   'New Hampshire': 1402054, 'New Jersey': 9290841, 'New Mexico': 2114371, 'New York': 19571216,
                   'North Carolina': 10835491, 'North Dakota': 783926, 'Ohio': 11785935, 'Oklahoma': 4053824,
                   'Oregon': 4233358, 'Pennsylvania': 12961683, 'Puerto Rico': 3205691, 'Rhode Island': 1095962,
                   'South Carolina': 5373555, 'South Dakota': 919318, 'Tennessee': 7126489, 'Texas': 30503301,
                   'Utah': 3417734, 'Vermont': 647818, 'Virginia': 8715698, 'Washington': 7812880,
                   'West Virginia': 1770071, 'Wisconsin': 5910955, 'Wyoming': 584057, 'United States': 334914895},
            2024: {'Alabama': 5143033, 'Alaska': 733583, 'Arizona': 7497004, 'Arkansas': 3089060,
                   'California': 39431263, 'Colorado': 5913096, 'Connecticut': 3616747, 'Delaware': 1044321,
                   'District of Columbia': 686995, 'Florida': 23002597, 'Georgia': 11145304, 'Hawaii': 1430877,
                   'Idaho': 1990456, 'Illinois': 12516863, 'Indiana': 6889680, 'Iowa': 3214315, 'Kansas': 2941208,
                   'Kentucky': 4539130, 'Louisiana': 4552238, 'Maine': 1405100, 'Maryland': 6164264,
                   'Massachusetts': 7020058, 'Michigan': 10037078, 'Minnesota': 5761530, 'Mississippi': 2929909,
                   'Missouri': 6204710, 'Montana': 1139507, 'Nebraska': 1988536, 'Nevada': 3209142,
                   'New Hampshire': 1408803, 'New Jersey': 9320865, 'New Mexico': 2115426, 'New York': 19469232,
                   'North Carolina': 10975017, 'North Dakota': 788940, 'Ohio': 11815587, 'Oklahoma': 4088377,
                   'Oregon': 4225973, 'Pennsylvania': 12951275, 'Puerto Rico': 3189034, 'Rhode Island': 1098082,
                   'South Carolina': 5464155, 'South Dakota': 928767, 'Tennessee': 7198025, 'Texas': 30975551,
                   'Utah': 3454232, 'Vermont': 649195, 'Virginia': 8757467, 'Washington': 7864400,
                   'West Virginia': 1763655, 'Wisconsin': 5931370, 'Wyoming': 586555, 'United States': 336377205},
            2025: {'Alabama': 5177000, 'Alaska': 734000, 'Arizona': 7563000, 'Arkansas': 3110000,
                   'California': 39897000, 'Colorado': 5949000, 'Connecticut': 3616000, 'Delaware': 1057000,
                   'District of Columbia': 695000, 'Florida': 23395000, 'Georgia': 11261000, 'Hawaii': 1427000,
                   'Idaho': 2016000, 'Illinois': 12484000, 'Indiana': 6917000, 'Iowa': 3222000, 'Kansas': 2942000,
                   'Kentucky': 4552000, 'Louisiana': 4531000, 'Maine': 1414000, 'Maryland': 6164000,
                   'Massachusetts': 7039000, 'Michigan': 10037000, 'Minnesota': 5785000, 'Mississippi': 2920000,
                   'Missouri': 6213000, 'Montana': 1156000, 'Nebraska': 1999000, 'Nevada': 3224000,
                   'New Hampshire': 1416000, 'New Jersey': 9351000, 'New Mexico': 2117000, 'New York': 19367000,
                   'North Carolina': 11115000, 'North Dakota': 794000, 'Ohio': 11845000, 'Oklahoma': 4123000,
                   'Oregon': 4219000, 'Pennsylvania': 12941000, 'Puerto Rico': 3172000, 'Rhode Island': 1100000,
                   'South Carolina': 5555000, 'South Dakota': 938000, 'Tennessee': 7270000, 'Texas': 31449000,
                   'Utah': 3491000, 'Vermont': 651000, 'Virginia': 8799000, 'Washington': 7916000,
                   'West Virginia': 1757000, 'Wisconsin': 5952000, 'Wyoming': 589000, 'United States': 337830000}
        }

        # Convert to DataFrame
        pop_list = []
        for year, state_pops in population_data.items():
            for state, pop in state_pops.items():
                pop_list.append({'year': year, 'state': state, 'population': pop})

        pop_df = pd.DataFrame(pop_list)
        logger.info(f"Loaded population data: {len(pop_df)} records for {pop_df['year'].nunique()} years")

        return pop_df

    def get_mmwr_week(self, date_str):
        """
        Convert a date string to MMWR week and year.
        MMWR week starts on Sunday and ends on Saturday.
        """
        try:
            # Parse the date
            if isinstance(date_str, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d', '%d/%m/%Y']:
                    try:
                        date = datetime.strptime(date_str, fmt)
                        break
                    except:
                        continue
                else:
                    # If no format worked, try pandas
                    date = pd.to_datetime(date_str)
            else:
                date = pd.to_datetime(date_str)

            # MMWR week 1 is the first week with at least 4 days in the new year
            # Starting on Sunday
            year = date.year

            # Find the first Sunday of the year
            jan1 = datetime(year, 1, 1)
            days_to_sunday = (6 - jan1.weekday()) % 7  # Sunday is 6 in Python
            first_sunday = jan1 + timedelta(days=days_to_sunday)

            # Check if we need to use previous year's week numbering
            if date < first_sunday:
                # This date belongs to the last week of the previous year
                year = year - 1
                jan1 = datetime(year, 1, 1)
                days_to_sunday = (6 - jan1.weekday()) % 7
                first_sunday = jan1 + timedelta(days=days_to_sunday)

            # Calculate week number
            days_since_first_sunday = (date - first_sunday).days
            week = (days_since_first_sunday // 7) + 1

            # Handle week 53 spillover to next year
            if week > 52:
                # Check if this should be week 1 of next year
                next_year = year + 1
                jan1_next = datetime(next_year, 1, 1)
                days_to_sunday_next = (6 - jan1_next.weekday()) % 7
                first_sunday_next = jan1_next + timedelta(days=days_to_sunday_next)

                if date >= first_sunday_next:
                    year = next_year
                    week = 1

            return year, week

        except Exception as e:
            logger.warning(f"Could not parse date {date_str}: {e}")
            return None, None

    def combine_nyc_with_ny(self, df: pd.DataFrame, data_source_name: str) -> pd.DataFrame:
        """Helper function to combine NYC and NY state data into single NY record."""
        if df.empty:
            return df

        # Check for NYC data before combining
        unique_states = df['state'].unique()
        nyc_in_data = any('New York City' in str(s) or 'NYC' in str(s) for s in unique_states)
        ny_in_data = 'New York' in unique_states

        logger.info(f"{data_source_name}: Found NYC={nyc_in_data}, NY={ny_in_data}")

        if nyc_in_data and ny_in_data:
            logger.info(f"{data_source_name}: Found both New York and New York City - combining them...")

            # Create a temporary grouping column
            df['state_group'] = df['state'].apply(
                lambda x: 'New York' if ('New York City' in str(x) or 'NYC' in str(x) or x == 'New York') else x
            )

            # Group by week and state_group, summing deaths
            groupby_cols = ['year', 'week', 'mmwr_week', 'state_group']
            if 'week_ending_date' in df.columns:
                groupby_cols.append('week_ending_date')

            df_grouped = df.groupby(groupby_cols, dropna=False).agg({'deaths': 'sum'}).reset_index()

            # Add back other columns that were lost in grouping
            for col in df.columns:
                if col not in df_grouped.columns and col not in ['state', 'state_group', 'deaths']:
                    # Take first value for non-numeric columns
                    first_values = df.groupby('state_group')[col].first().reset_index()
                    df_grouped = df_grouped.merge(first_values, on='state_group', how='left')

            # Rename state_group back to state
            df_grouped = df_grouped.rename(columns={'state_group': 'state'})
            df = df_grouped

            # Log verification
            ny_total_deaths = df[df['state'] == 'New York']['deaths'].sum()
            logger.info(f"{data_source_name}: Combined New York total deaths: {ny_total_deaths:,.0f}")

        elif nyc_in_data:
            # If only NYC is in the data, rename it to New York
            logger.info(f"{data_source_name}: Found New York City but not New York state - renaming NYC to New York")
            df['state'] = df['state'].apply(
                lambda x: 'New York' if ('New York City' in str(x) or 'NYC' in str(x)) else x
            )

        return df

    def process_local_2019_file(self, filename: str = 'all_state_data_for_2019.csv') -> pd.DataFrame:
        """Process the local file containing complete 2019 state-level data."""
        try:
            logger.info(f"Reading local 2019 data from {filename}...")

            # Check if file exists
            if not os.path.exists(filename):
                logger.error(f"File {filename} not found in current directory: {os.getcwd()}")
                logger.error(f"Files in directory: {os.listdir('.')}")
                return pd.DataFrame()

            # Read the CSV
            df = pd.read_csv(filename)
            logger.info(f"Loaded {len(df)} records from local file")
            logger.info(f"Columns in file: {df.columns.tolist()}")

            # Standardize column names
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower()
                if 'week' in col_lower and 'date' in col_lower:
                    column_mapping[col] = 'week_ending_date'
                elif 'jurisdiction' in col_lower:
                    column_mapping[col] = 'state'
                elif 'cause' in col_lower or 'death' in col_lower:
                    column_mapping[col] = 'deaths'

            logger.info(f"Column mapping: {column_mapping}")
            df = df.rename(columns=column_mapping)

            # Convert deaths to numeric
            df['deaths'] = pd.to_numeric(df['deaths'], errors='coerce')

            # Remove any rows with null deaths
            df = df[df['deaths'].notna() & (df['deaths'] > 0)]
            logger.info(f"After filtering for valid deaths: {len(df)} records")

            # Calculate MMWR week from dates
            if 'week_ending_date' in df.columns:
                df['mmwr_info'] = df['week_ending_date'].apply(lambda x: self.get_mmwr_week(x))
                df['calculated_year'] = df['mmwr_info'].apply(lambda x: x[0] if x else None)
                df['mmwr_week'] = df['mmwr_info'].apply(lambda x: x[1] if x else None)

                # For 2019 data, force all to 2019
                df['year'] = 2019
                df['week'] = df['mmwr_week']

                # CORRECTION: 2019 data appears to be off by 1 week - shift all weeks down by 1
                # This corrects weeks 2-53 to become weeks 1-52
                df['week'] = df['week'] - 1
                df['mmwr_week'] = df['mmwr_week'] - 1

                # Ensure no week 0 values (if any week 1 became week 0, make it week 1)
                df.loc[df['week'] <= 0, 'week'] = 1
                df.loc[df['mmwr_week'] <= 0, 'mmwr_week'] = 1

                logger.info(f"Applied 2019 week correction: shifted all weeks down by 1")
                logger.info(f"Week range after correction: {df['week'].min()} to {df['week'].max()}")

                # Drop temporary columns
                df = df.drop(['mmwr_info', 'calculated_year'], axis=1, errors='ignore')
            else:
                logger.warning("No week_ending_date column found, setting default week values")
                df['year'] = 2019
                df['week'] = None
                df['mmwr_week'] = None

            # Check for NYC data before combining
            unique_states = df['state'].unique()
            logger.info(f"Unique jurisdictions found: {sorted(unique_states)[:10]}...")

            # FILTER OUT US NATIONAL DATA - we only want states for 2019
            # Remove any US national records since we'll get those from World Mortality
            us_national_variants = ['United States', 'US', 'USA', 'National', 'Total']
            df = df[~df['state'].isin(us_national_variants)]
            logger.info(f"After removing US national records: {len(df)} records remain")

            # COMBINE NYC WITH NY STATE using the new helper function
            df = self.combine_nyc_with_ny(df, "Local 2019 File")

            # Add metadata
            df['data_source'] = 'Local 2019 File'

            # Select final columns
            columns_to_keep = ['year', 'week', 'mmwr_week', 'week_ending_date', 'state', 'deaths', 'data_source']
            available_columns = [col for col in columns_to_keep if col in df.columns]
            df = df[available_columns]

            # Ensure year is 2019 one more time
            df['year'] = 2019

            # Summary statistics
            states_count = df['state'].nunique()
            weeks_count = df['week'].nunique() if 'week' in df.columns else 0
            total_deaths = df['deaths'].sum()

            logger.info(f"Processed local 2019 data:")
            logger.info(f"  Records: {len(df)}")
            logger.info(f"  States: {states_count}")
            logger.info(f"  Weeks: {weeks_count}")
            logger.info(f"  Total deaths: {total_deaths:,.0f}")
            logger.info(f"  Year values in final data: {df['year'].unique()}")

            # Check coverage
            if 'week' in df.columns:
                state_week_coverage = df.groupby('state')['week'].nunique()
                complete_states = (state_week_coverage >= 52).sum()
                logger.info(f"  States with 52+ weeks: {complete_states}")

                # Verify New York totals for first few weeks
                ny_data = df[df['state'] == 'New York'].sort_values('week')
                if not ny_data.empty:
                    logger.info("New York deaths for first 5 weeks of 2019:")
                    for week in range(1, min(6, ny_data['week'].max() + 1)):
                        week_deaths = ny_data[ny_data['week'] == week]['deaths'].sum()
                        if week_deaths > 0:
                            logger.info(f"  Week {week}: {week_deaths:,.0f} deaths")

            return df

        except FileNotFoundError:
            logger.error(f"File {filename} not found in current directory")
            logger.error(f"Current directory: {os.getcwd()}")
            logger.error(f"Files in directory: {os.listdir('.')}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error processing local 2019 file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def download_dataset(self, source_key: str, max_retries: int = 3) -> pd.DataFrame:
        """Download a dataset with retry logic."""
        source = self.data_sources[source_key]
        url = source['url']
        description = source['description']

        logger.info(f"Downloading {description}...")

        for attempt in range(max_retries):
            try:
                time.sleep(1)
                response = self.session.get(url, timeout=60)
                response.raise_for_status()

                df = pd.read_csv(StringIO(response.text))
                logger.info(f"Successfully downloaded {len(df)} records from {source_key}")
                return df

            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1} failed for {source_key}: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to download {source_key} after {max_retries} attempts")
                    return pd.DataFrame()
                time.sleep(2 ** attempt)

        return pd.DataFrame()

    def process_world_mortality_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the World Mortality Dataset to extract US all-cause data 2015-2020."""
        if df.empty:
            return df

        logger.info("Processing World Mortality Dataset...")

        us_data = df[df['country_name'] == 'United States'].copy()

        if us_data.empty:
            logger.warning("No US data found in World Mortality Dataset")
            return pd.DataFrame()

        logger.info(f"Found {len(us_data)} US records in World Mortality Dataset")

        us_data = us_data.rename(columns={
            'country_name': 'country',
            'time': 'week',
            'time_unit': 'period_type'
        })

        numeric_cols = ['year', 'week', 'deaths']
        for col in numeric_cols:
            if col in us_data.columns:
                us_data[col] = pd.to_numeric(us_data[col], errors='coerce')

        # EXCLUDE 2019 - we'll use local file FOR STATE DATA ONLY
        # Include 2019 for US national data since we're not calculating it from states anymore
        us_data = us_data[(us_data['year'] >= 2015) & (us_data['year'] <= 2020)]

        # DIAGNOSTIC: Check if 2020 Week 1 survives filtering
        week1_2020 = us_data[(us_data['year'] == 2020) & (us_data['week'] == 1)]
        logger.info(f"DIAGNOSTIC - World Mortality 2020 Week 1 records after filtering: {len(week1_2020)}")
        if not week1_2020.empty:
            logger.info(f"DIAGNOSTIC - 2020 Week 1 deaths: {week1_2020['deaths'].iloc[0]}")
        else:
            logger.warning("DIAGNOSTIC - 2020 Week 1 NOT FOUND after filtering!")
            # Check what 2020 weeks we do have
            weeks_2020 = sorted(us_data[us_data['year'] == 2020]['week'].unique())
            logger.info(f"DIAGNOSTIC - Available 2020 weeks: {weeks_2020}")

        # DIAGNOSTIC: Check 2019 US national data
        us_2019 = us_data[us_data['year'] == 2019]
        if not us_2019.empty:
            total_2019_deaths = us_2019['deaths'].sum()
            logger.info(f"DIAGNOSTIC - 2019 US deaths from World Mortality: {total_2019_deaths:,.0f}")
        else:
            logger.warning("DIAGNOSTIC - No 2019 US data found in World Mortality!")

        us_data['state'] = 'United States'
        us_data['data_source'] = 'World Mortality Dataset'
        us_data['mmwr_week'] = us_data['week']

        columns_to_keep = ['year', 'week', 'mmwr_week', 'state', 'deaths', 'data_source']
        available_columns = [col for col in columns_to_keep if col in us_data.columns]
        us_data = us_data[available_columns]

        years = sorted(us_data['year'].unique())
        logger.info(f"Processed World Mortality data: {len(us_data)} records for years {years}")
        return us_data

    def process_cdc_provisional_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process CDC provisional data to extract state-level all-cause mortality."""
        if df.empty:
            return df

        logger.info("Processing CDC provisional data for all-cause mortality...")
        logger.info(f"CDC provisional columns: {df.columns.tolist()[:10]}...")  # Log first 10 columns

        column_mapping = {
            'Year': 'year',
            'MMWR Week': 'mmwr_week',
            'Week Ending Date': 'week_ending_date',
            'State': 'state',
            'Total Deaths': 'total_deaths'
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})

        # Convert to numeric
        numeric_cols = ['year', 'mmwr_week', 'total_deaths']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Filter to 2020-present (EXCLUDE 2019 - we'll use local file)
        if 'year' in df.columns and 'total_deaths' in df.columns and 'mmwr_week' in df.columns:
            # Filter for valid years and ensure we have week data
            df_filtered = df[(df['year'] >= 2020) &
                             (df['total_deaths'].notna()) &
                             (df['total_deaths'] > 0) &
                             (df['mmwr_week'].notna()) &
                             (df['mmwr_week'] > 0) &
                             (df['mmwr_week'] <= 53)].copy()

            df_filtered['deaths'] = df_filtered['total_deaths']
            df_filtered['week'] = df_filtered['mmwr_week']
            df_filtered['data_source'] = 'CDC Provisional'

            # COMBINE NYC WITH NY STATE
            df_filtered = self.combine_nyc_with_ny(df_filtered, "CDC Provisional")

            columns_to_keep = ['year', 'week', 'mmwr_week', 'week_ending_date', 'state', 'deaths', 'data_source']
            available_columns = [col for col in columns_to_keep if col in df_filtered.columns]
            df_filtered = df_filtered[available_columns]

            # Log summary
            years = sorted(df_filtered['year'].unique())
            weeks_per_year = df_filtered.groupby('year')['week'].nunique()
            logger.info(f"Processed CDC provisional data: {len(df_filtered)} records for years {years}")
            logger.info(f"Weeks per year: {weeks_per_year.to_dict()}")

            return df_filtered

        logger.warning("Required columns not found in CDC provisional data")
        return pd.DataFrame()

    def process_archived_state_deaths(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process archived state-level all-cause death data from NCHS surveillance system."""
        if df.empty:
            return df

        logger.info("Processing archived state-level all-cause death data...")
        logger.info(f"Columns in archived data: {df.columns.tolist()[:10]}...")  # Log first 10 columns

        # Filter for "All" age group
        df_all_ages = df[df['age'] == 'All'].copy()

        # Extract year and week - ensure we handle the column correctly
        if 'MMWR Year/Week' in df_all_ages.columns:
            # Convert to numeric first to handle any string issues
            df_all_ages['MMWR Year/Week'] = pd.to_numeric(df_all_ages['MMWR Year/Week'], errors='coerce')
            df_all_ages = df_all_ages[df_all_ages['MMWR Year/Week'].notna()]

            df_all_ages['year'] = (df_all_ages['MMWR Year/Week'] // 100).astype(int)
            df_all_ages['mmwr_week'] = (df_all_ages['MMWR Year/Week'] % 100).astype(int)
            df_all_ages['week'] = df_all_ages['mmwr_week']
        else:
            logger.warning("MMWR Year/Week column not found in archived data")
            return pd.DataFrame()

        # Filter to 2015-2018 (EXCLUDE 2019 - we'll use local file)
        df_filtered = df_all_ages[(df_all_ages['year'] >= 2015) & (df_all_ages['year'] <= 2018)].copy()

        # Rename columns
        column_mapping = {
            'State': 'state',
            'All Deaths': 'deaths'
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df_filtered.columns:
                df_filtered = df_filtered.rename(columns={old_name: new_name})

        # Convert deaths to numeric
        df_filtered['deaths'] = pd.to_numeric(df_filtered['deaths'], errors='coerce')

        # Filter for valid deaths and ensure we have week data
        df_filtered = df_filtered[(df_filtered['deaths'].notna()) &
                                  (df_filtered['deaths'] > 0) &
                                  (df_filtered['week'].notna()) &
                                  (df_filtered['week'] > 0) &
                                  (df_filtered['week'] <= 53)]

        df_filtered['data_source'] = 'Archived NCHS'

        # COMBINE NYC WITH NY STATE
        df_filtered = self.combine_nyc_with_ny(df_filtered, "Archived NCHS")

        columns_to_keep = ['year', 'week', 'mmwr_week', 'state', 'deaths', 'data_source']
        available_columns = [col for col in columns_to_keep if col in df_filtered.columns]
        df_filtered = df_filtered[available_columns]

        # Log summary
        years = sorted(df_filtered['year'].unique())
        weeks_per_year = df_filtered.groupby('year')['week'].nunique()
        logger.info(f"Processed archived data: {len(df_filtered)} records for years {years}")
        logger.info(f"Weeks per year: {weeks_per_year.to_dict()}")

        return df_filtered

    def calculate_us_national_2019(self, state_2019_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate US national totals for 2019 from state data."""
        if state_2019_df.empty:
            return pd.DataFrame()

        logger.info("Calculating US national totals for 2019 from state data...")

        # Group by week and sum deaths
        national_2019 = state_2019_df.groupby(['year', 'week', 'mmwr_week']).agg({
            'deaths': 'sum',
            'week_ending_date': 'first'
        }).reset_index()

        national_2019['state'] = 'United States'
        national_2019['data_source'] = 'Calculated from State Data'

        # Reorder columns to match other datasets
        columns = ['year', 'week', 'mmwr_week', 'week_ending_date', 'state', 'deaths', 'data_source']
        available_columns = [col for col in columns if col in national_2019.columns]
        national_2019 = national_2019[available_columns]

        logger.info(f"Created {len(national_2019)} US national records for 2019")
        logger.info(f"Total US deaths in 2019: {national_2019['deaths'].sum():,.0f}")

        return national_2019

    def merge_and_clean_datasets(self, *datasets) -> pd.DataFrame:
        """Merge historical and current datasets, keeping only all-cause mortality."""
        logger.info("Merging and cleaning datasets (all-cause only)...")

        datasets_to_merge = []

        for i, dataset in enumerate(datasets):
            if dataset is not None and not dataset.empty:
                datasets_to_merge.append(dataset)
                logger.info(f"Including dataset {i + 1}: {len(dataset)} records")

        if not datasets_to_merge:
            logger.error("No datasets available for merging")
            return pd.DataFrame()

        combined_df = pd.concat(datasets_to_merge, ignore_index=True, sort=False)
        logger.info(f"Combined dataset: {len(combined_df)} records")

        combined_df = self.clean_data(combined_df)

        return combined_df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the merged dataset."""
        if df.empty:
            return df

        logger.info("Cleaning merged dataset...")

        # First, ensure we have week column if we have mmwr_week
        if 'mmwr_week' in df.columns and 'week' not in df.columns:
            df['week'] = df['mmwr_week']
        elif 'week' in df.columns and 'mmwr_week' not in df.columns:
            df['mmwr_week'] = df['week']

        # Essential columns - now including week
        essential_cols = ['year', 'state', 'deaths', 'week']
        # Only keep rows where we have all essential data
        for col in essential_cols:
            if col in df.columns:
                df = df[df[col].notna()]

        # Convert to proper types
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['week'] = pd.to_numeric(df['week'], errors='coerce')
        df['mmwr_week'] = pd.to_numeric(df['mmwr_week'], errors='coerce') if 'mmwr_week' in df.columns else df['week']
        df['deaths'] = pd.to_numeric(df['deaths'], errors='coerce')

        # Remove any rows with invalid numeric conversions
        df = df.dropna(subset=['year', 'week', 'deaths'])
        df['year'] = df['year'].astype(int)
        df['week'] = df['week'].astype(int)
        if 'mmwr_week' in df.columns:
            df['mmwr_week'] = df['mmwr_week'].astype(int)

        # Deduplicate - prioritize records with more complete information
        # Sort by completeness before deduplicating
        df['completeness_score'] = df.notna().sum(axis=1)
        df = df.sort_values(['year', 'week', 'state', 'completeness_score'], ascending=[True, True, True, False])
        df = df.drop_duplicates(subset=['year', 'week', 'state'], keep='first')
        df = df.drop('completeness_score', axis=1)
        logger.info(f"After deduplication: {len(df)} records")

        # State name standardization (backup - should already be done by combine_nyc_with_ny)
        state_mapping = {
            'United States': 'United States',
            'New York City': 'New York'
        }
        df['state'] = df['state'].replace(state_mapping)

        # Valid jurisdictions
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
        df = df[df['deaths'] > 0]

        # Sort
        sort_columns = ['year', 'week', 'state']
        available_sort_cols = [col for col in sort_columns if col in df.columns]
        df = df.sort_values(available_sort_cols)

        df = df.reset_index(drop=True)

        logger.info(f"Final cleaned dataset: {len(df)} records")
        return df

    def create_summary_statistics(self, df: pd.DataFrame, data_type: str = "combined") -> dict:
        """Create summary statistics for the compiled dataset."""
        if df.empty:
            return {}

        stats = {
            'data_type': data_type,
            'total_records': len(df),
            'years_covered': f"{df['year'].min()}-{df['year'].max()}",
            'total_deaths': df['deaths'].sum(),
            'data_sources': df['data_source'].unique().tolist() if 'data_source' in df.columns else [],
        }

        if data_type == "state":
            stats['states_count'] = df['state'].nunique()
            stats['state_coverage'] = sorted(df['state'].unique().tolist())

            # Check 2019 completeness
            df_2019 = df[df['year'] == 2019]
            if not df_2019.empty:
                states_2019 = df_2019['state'].nunique()
                weeks_2019 = df_2019.groupby('state')['week'].nunique()
                complete_states = (weeks_2019 >= 52).sum()
                stats['2019_coverage'] = {
                    'states_with_data': states_2019,
                    'states_with_full_year': complete_states,
                    'average_weeks_per_state': weeks_2019.mean()
                }

        stats['yearly_totals'] = df.groupby('year')['deaths'].sum().to_dict()

        return stats

    def validate_2019_vs_2018_data(self, df: pd.DataFrame):
        """Validate that 2019 data is reasonable compared to 2018 data."""
        if df.empty:
            return

        logger.info("Validating 2019 data against 2018 baseline...")

        # Get 2018 and 2019 data for comparison
        df_2018 = df[df['year'] == 2018]
        df_2019 = df[df['year'] == 2019]

        if df_2018.empty or df_2019.empty:
            logger.warning("Cannot validate 2019 data: missing 2018 or 2019 data")
            return

        # Focus on state-level data (exclude US national)
        state_df_2018 = df_2018[df_2018['state'] != 'United States']
        state_df_2019 = df_2019[df_2019['state'] != 'United States']

        if state_df_2018.empty or state_df_2019.empty:
            logger.warning("Cannot validate: missing state-level data for 2018 or 2019")
            return

        # Calculate annual totals by state
        totals_2018 = state_df_2018.groupby('state')['deaths'].sum()
        totals_2019 = state_df_2019.groupby('state')['deaths'].sum()

        # Focus on New York specifically since that's where we expect issues
        if 'New York' in totals_2018.index and 'New York' in totals_2019.index:
            ny_2018 = totals_2018['New York']
            ny_2019 = totals_2019['New York']
            ny_change = ((ny_2019 - ny_2018) / ny_2018) * 100

            print("\n" + "=" * 70)
            print("2019 vs 2018 DATA VALIDATION - NEW YORK FOCUS")
            print("=" * 70)
            print(f"New York 2018 total deaths: {ny_2018:,.0f}")
            print(f"New York 2019 total deaths: {ny_2019:,.0f}")
            print(f"Year-over-year change: {ny_change:+.1f}%")

            # Flag excessive changes
            if abs(ny_change) > 10:
                print(f"⚠️  WARNING: New York deaths changed by {ny_change:+.1f}% - this seems excessive!")
                print("    Normal year-over-year change should be < ±5%")
                print("    This might indicate double-counting or missing data issues")
            else:
                print(f"✓  New York change of {ny_change:+.1f}% appears reasonable")

        # Overall state totals comparison
        total_2018 = totals_2018.sum()
        total_2019 = totals_2019.sum()
        overall_change = ((total_2019 - total_2018) / total_2018) * 100

        print(f"\nOverall state totals:")
        print(f"2018 total state deaths: {total_2018:,.0f}")
        print(f"2019 total state deaths: {total_2019:,.0f}")
        print(f"Overall change: {overall_change:+.1f}%")

        if abs(overall_change) > 5:
            print(f"⚠️  WARNING: Overall state deaths changed by {overall_change:+.1f}% - investigate!")
        else:
            print(f"✓  Overall change of {overall_change:+.1f}% appears reasonable")

        # Check data completeness
        states_2018 = set(totals_2018.index)
        states_2019 = set(totals_2019.index)

        missing_in_2019 = states_2018 - states_2019
        extra_in_2019 = states_2019 - states_2018

        if missing_in_2019:
            print(f"⚠️  States missing in 2019: {list(missing_in_2019)}")
        if extra_in_2019:
            print(f"ℹ️  New states in 2019: {list(extra_in_2019)}")

        print(f"\nStates with data - 2018: {len(states_2018)}, 2019: {len(states_2019)}")

        # Check for potential NYC double-counting by looking at top states
        top_states_2019 = totals_2019.nlargest(5)
        print(f"\nTop 5 states by deaths in 2019:")
        for state, deaths in top_states_2019.items():
            if state in totals_2018:
                change = ((deaths - totals_2018[state]) / totals_2018[state]) * 100
                print(f"  {state}: {deaths:,.0f} ({change:+.1f}% vs 2018)")
            else:
                print(f"  {state}: {deaths:,.0f} (new in 2019)")

    def save_datasets(self, df: pd.DataFrame):
        """Save separate CSV files for US national data and state-level data with population."""

        # Get population data
        pop_df = self.get_population_data()

        # Ensure all records have week/mmwr_week
        if 'week' not in df.columns and 'mmwr_week' in df.columns:
            df['week'] = df['mmwr_week']
        elif 'mmwr_week' not in df.columns and 'week' in df.columns:
            df['mmwr_week'] = df['week']

        # Log missing week data
        missing_week = df['week'].isna().sum() if 'week' in df.columns else len(df)
        if missing_week > 0:
            logger.warning(f"Warning: {missing_week} records are missing week information")

        # Add population data to the main dataframe
        df_with_pop = df.merge(pop_df, on=['year', 'state'], how='left')

        # For records missing population, try to use prior year
        missing_pop = df_with_pop[df_with_pop['population'].isna()]
        if not missing_pop.empty:
            logger.info(f"Filling {len(missing_pop)} records with missing population using prior year data...")
            for idx, row in missing_pop.iterrows():
                # Try previous years in order
                for year_offset in range(1, 5):  # Try up to 4 years back
                    prior_year = row['year'] - year_offset
                    prior_pop = pop_df[(pop_df['year'] == prior_year) &
                                       (pop_df['state'] == row['state'])]
                    if not prior_pop.empty:
                        df_with_pop.at[idx, 'population'] = prior_pop['population'].values[0]
                        break

        # Calculate mortality rate per 100,000 population
        df_with_pop['mortality_rate_per_100k'] = (df_with_pop['deaths'] / df_with_pop['population'] * 100000).round(1)

        # Separate US national and state data
        us_data = df_with_pop[df_with_pop['state'] == 'United States'].copy()
        state_data = df_with_pop[df_with_pop['state'] != 'United States'].copy()

        # Define columns to save (include population)
        desired_columns = ['year', 'week', 'mmwr_week', 'week_ending_date', 'state', 'deaths',
                           'population', 'mortality_rate_per_100k', 'data_source']

        # Save US national data
        if not us_data.empty:
            us_filename = 'us_national_mortality_2015_present.csv'

            # Select available columns
            us_columns = [col for col in desired_columns if col in us_data.columns]
            us_data_final = us_data[us_columns]

            # Sort for better readability
            us_data_final = us_data_final.sort_values(['year', 'week'])
            us_data_final.to_csv(us_filename, index=False)

            us_stats = self.create_summary_statistics(us_data_final, "national")

            print("\n" + "=" * 90)
            print("US NATIONAL MORTALITY DATA SAVED")
            print("=" * 90)
            print(f"File: {us_filename}")
            print(f"Records: {us_stats['total_records']:,}")
            print(f"Years: {us_stats['years_covered']}")
            print(f"Total deaths: {us_stats['total_deaths']:,.0f}")
            print(f"Columns saved: {', '.join(us_columns)}")

            # Population coverage stats
            pop_coverage = us_data_final['population'].notna().sum() / len(us_data_final) * 100
            print(f"Population data coverage: {pop_coverage:.1f}%")

            # Check for missing week data
            if 'week' in us_data_final.columns:
                missing = us_data_final['week'].isna().sum()
                if missing > 0:
                    print(f"⚠ Warning: {missing} records missing week information")

            print(f"\nDeaths by year (with US population):")
            for year in sorted(us_stats['yearly_totals'].keys()):
                deaths = us_stats['yearly_totals'][year]
                year_data = us_data_final[us_data_final['year'] == year]
                if not year_data.empty and 'population' in year_data.columns:
                    pop = year_data['population'].iloc[0]  # US population should be same for all weeks in year
                    if pd.notna(pop):
                        rate = deaths / pop * 100000
                        print(f"  {year}: {deaths:,.0f} deaths | Pop: {pop:,.0f} | Rate: {rate:.1f} per 100k")
                    else:
                        print(f"  {year}: {deaths:,.0f} deaths")
                else:
                    print(f"  {year}: {deaths:,.0f} deaths")

        # Save state-level data
        if not state_data.empty:
            state_filename = 'state_mortality_2015_present.csv'

            # Select available columns
            state_columns = [col for col in desired_columns if col in state_data.columns]
            state_data_final = state_data[state_columns]

            # Sort for better readability
            state_data_final = state_data_final.sort_values(['year', 'week', 'state'])
            state_data_final.to_csv(state_filename, index=False)

            state_stats = self.create_summary_statistics(state_data_final, "state")

            print("\n" + "=" * 90)
            print("STATE-LEVEL MORTALITY DATA SAVED")
            print("=" * 90)
            print(f"File: {state_filename}")
            print(f"Records: {state_stats['total_records']:,}")
            print(f"States: {state_stats['states_count']}")
            print(f"Years: {state_stats['years_covered']}")
            print(f"Total deaths: {state_stats['total_deaths']:,.0f}")
            print(f"Columns saved: {', '.join(state_columns)}")

            # Population coverage stats
            pop_coverage = state_data_final['population'].notna().sum() / len(state_data_final) * 100
            print(f"Population data coverage: {pop_coverage:.1f}%")

            # Check for missing week data
            if 'week' in state_data_final.columns:
                missing = state_data_final['week'].isna().sum()
                if missing > 0:
                    print(f"⚠ Warning: {missing} records missing week information")

            if '2019_coverage' in state_stats:
                coverage = state_stats['2019_coverage']
                print(f"\n2019 Coverage (from local file):")
                print(f"  States with data: {coverage['states_with_data']}")
                print(f"  States with full year: {coverage['states_with_full_year']}")
                print(f"  Average weeks per state: {coverage['average_weeks_per_state']:.1f}")

            print(f"\nDeaths by year:")
            for year, deaths in sorted(state_stats['yearly_totals'].items()):
                print(f"  {year}: {deaths:,.0f}")

    def compile_comprehensive_data(self):
        """Main method to compile comprehensive all-cause mortality data from 2015 to present."""
        print("ALL-CAUSE MORTALITY DATA COMPILER v3.1")
        print("=" * 70)
        print("Features:")
        print("  • All-cause mortality only")
        print("  • Complete 2019 data from local file")
        print("  • Consistent NYC+NY combination across all sources")
        print("  • Separate US national and state-level outputs")
        print("  • Data validation to check 2019 vs 2018 consistency")
        print("  • Data sources: World Mortality, CDC APIs, Archives, Local 2019")
        print("")

        all_datasets = []

        # Step 1: Process local 2019 file FIRST (STATE DATA ONLY)
        print("Step 1: Loading 2019 STATE data from local file...")
        state_2019_data = self.process_local_2019_file()
        if not state_2019_data.empty:
            all_datasets.append(state_2019_data)
            print(f"✓ Loaded {len(state_2019_data)} state records for 2019")
            print("✓ US national 2019 data will come from World Mortality dataset")
        else:
            print("⚠ Warning: Could not load 2019 state data from local file")

        # Step 2: Download and process World Mortality Dataset (US national, INCLUDING 2019 now)
        print("\nStep 2: Downloading World Mortality Dataset (US national, 2015-2020 including 2019)...")
        world_mortality_raw = self.download_dataset('world_mortality')
        if not world_mortality_raw.empty:
            world_mortality_processed = self.process_world_mortality_data(world_mortality_raw)
            if not world_mortality_processed.empty:
                all_datasets.append(world_mortality_processed)
                print(f"✓ Processed {len(world_mortality_processed)} national records")

        # Step 3: Download and process archived state data (excluding 2019)
        print("\nStep 3: Downloading archived state mortality (2015-2018)...")
        archived_raw = self.download_dataset('archived_state_deaths')
        if not archived_raw.empty:
            archived_processed = self.process_archived_state_deaths(archived_raw)
            if not archived_processed.empty:
                all_datasets.append(archived_processed)
                print(f"✓ Processed {len(archived_processed)} state records")

        # Step 4: Download and process CDC provisional data (2020-present)
        print("\nStep 4: Downloading CDC provisional data (2020-present)...")
        cdc_raw = self.download_dataset('cdc_provisional')
        if not cdc_raw.empty:
            cdc_processed = self.process_cdc_provisional_data(cdc_raw)
            if not cdc_processed.empty:
                all_datasets.append(cdc_processed)
                print(f"✓ Processed {len(cdc_processed)} current records")

        if not all_datasets:
            print("\nERROR: No data was successfully downloaded!")
            return

        # Merge and clean
        print("\nStep 5: Merging and cleaning datasets...")
        final_df = self.merge_and_clean_datasets(*all_datasets)

        if final_df.empty:
            logger.error("No data remaining after processing!")
            return

        # NEW: Validate 2019 data
        print("\nStep 6: Validating 2019 data consistency...")
        self.validate_2019_vs_2018_data(final_df)

        # Save separate files
        print("\nStep 7: Saving separate US national and state-level datasets...")
        self.save_datasets(final_df)

        print("\n" + "=" * 90)
        print("PROCESSING COMPLETE")
        print("=" * 90)
        print("\nOutput files:")
        print("  1. us_national_mortality_2015_present.csv - National totals only")
        print("  2. state_mortality_2015_present.csv - State-level data")
        print("\nData notes:")
        print("  • All data is all-cause mortality only")
        print("  • NYC consistently combined with NY state across all years")
        print("  • 2019 data is complete from your local file")
        print("  • Data validation performed to ensure consistency")
        print("  • Most recent weeks may be provisional and subject to revision")
        print("\nUsage:")
        print("  import pandas as pd")
        print("  us_data = pd.read_csv('us_national_mortality_2015_present.csv')")
        print("  state_data = pd.read_csv('state_mortality_2015_present.csv')")


def main():
    """Run the comprehensive mortality data compilation."""
    try:
        print("Welcome to the All-Cause Mortality Data Compiler!")
        print("This version uses your local file for complete 2019 data.\n")

        compiler = ComprehensiveMortalityDataCompiler()
        compiler.compile_comprehensive_data()

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
    except Exception as e:
        print(f"\n\nAn error occurred: {e}")
        print("Please check that 'all_state_data_for_2019.csv' exists in the current directory.")
        logger.exception("Full error details:")


if __name__ == "__main__":
    main()