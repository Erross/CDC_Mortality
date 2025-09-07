import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Mortality Data Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern styling and compact sidebar
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f1f1f;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 2.5rem;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border-radius: 0.3rem;
        color: #262730;
        font-weight: 600;
        font-size: 0.9rem;
    }
    .stTabs [aria-selected="true"] {
        background-color: #007acc;
        color: white;
    }
    .sidebar .element-container {
        margin-bottom: 0.3rem;
    }
    .sidebar .stSelectbox > div > div {
        font-size: 0.9rem;
    }
    .sidebar .stMultiSelect > div > div {
        font-size: 0.9rem;
    }
    .sidebar .stRadio > div {
        font-size: 0.9rem;
    }
    .sidebar .stMarkdown {
        font-size: 0.85rem;
        margin-bottom: 0.2rem;
    }
    .sidebar .stMetric {
        font-size: 0.85rem;
    }
    .sidebar .stMetric > div {
        padding: 0.2rem 0;
    }
    .sidebar h3 {
        font-size: 1.1rem;
        margin-bottom: 0.3rem;
        margin-top: 0.5rem;
    }
    .sidebar h2 {
        font-size: 1.2rem;
        margin-bottom: 0.3rem;
        margin-top: 0.3rem;
    }
    .sidebar .stButton > button {
        font-size: 0.8rem;
        padding: 0.2rem 0.5rem;
        margin-bottom: 0.2rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    """Load and prepare the datasets"""
    try:
        state_data = pd.read_csv('state_mortality_2015_present.csv')
        us_data = pd.read_csv('us_national_mortality_2015_present.csv')
        return state_data, us_data
    except FileNotFoundError as e:
        st.error(f"Could not find data files: {e}")
        return None, None


def get_color_for_year(year):
    """Return color based on year range"""
    if 2015 <= year <= 2019:
        return '#2E8B57'  # Green
    elif 2020 <= year <= 2022:
        return '#DC143C'  # Red
    elif year in [2023, 2024]:
        return '#FF69B4'  # Pink
    elif year == 2025:
        return '#1E90FF'  # Blue
    else:
        return '#808080'  # Gray


def add_baseline_calculations(df):
    """Add baseline calculations to the dataframe"""
    # Calculate 2015-2019 average for each state/week combination
    baseline_avg = df[(df['year'] >= 2015) & (df['year'] <= 2019)].groupby(['state', 'mmwr_week'])[
        'deaths'].mean().reset_index()
    baseline_avg = baseline_avg.rename(columns={'deaths': 'avg_2015_2019'})

    # Calculate 2015 baseline for each state/week combination
    baseline_2015 = df[df['year'] == 2015].groupby(['state', 'mmwr_week'])['deaths'].mean().reset_index()
    baseline_2015 = baseline_2015.rename(columns={'deaths': 'baseline_2015'})

    # Merge baselines back to main dataframe
    df = df.merge(baseline_avg, on=['state', 'mmwr_week'], how='left')
    df = df.merge(baseline_2015, on=['state', 'mmwr_week'], how='left')

    # Calculate expected deaths with 1.31% annual growth from 2015
    df['years_from_2015'] = df['year'] - 2015
    df['growth_factor'] = (1.0131) ** df['years_from_2015']
    df['expected_deaths'] = df['baseline_2015'] * df['growth_factor']

    # Calculate deviations
    df['deviation_from_avg'] = df['deaths'] - df['avg_2015_2019']
    df['deviation_from_expected'] = df['deaths'] - df['expected_deaths']

    return df


def create_chart(df, selected_states, chart_type):
    """Create charts based on type and state selection"""

    # Filter to selected states
    if selected_states == ['All States']:
        plot_data = df.copy()
        title_suffix = "All States Combined"
    else:
        plot_data = df[df['state'].isin(selected_states)].copy()
        title_suffix = f"{', '.join(selected_states)}"

    # Aggregate data by year/week
    if chart_type == 'raw':
        agg_data = plot_data.groupby(['year', 'mmwr_week'])['deaths'].sum().reset_index()
        y_col = 'deaths'
        title = f'Raw Deaths per MMWR Week - {title_suffix}'
        y_title = 'Deaths'
    elif chart_type == 'deaths_per_100k':
        # Check if population data is available
        if 'population' not in plot_data.columns:
            st.error(
                "Population data not found. Please add a 'population' column to your data for per-capita calculations.")
            return go.Figure()

        # For per-capita calculations, we need to aggregate differently
        # Sum deaths and population, then calculate rate
        agg_data = plot_data.groupby(['year', 'mmwr_week']).agg({
            'deaths': 'sum',
            'population': 'sum'
        }).reset_index()

        # Calculate deaths per 100k
        agg_data['deaths_per_100k'] = (agg_data['deaths'] / agg_data['population']) * 100000
        y_col = 'deaths_per_100k'
        title = f'Deaths per 100k Population per MMWR Week - {title_suffix}'
        y_title = 'Deaths per 100k Population'
    elif chart_type == 'deviation_avg':
        agg_data = plot_data.groupby(['year', 'mmwr_week'])['deviation_from_avg'].sum().reset_index()
        y_col = 'deviation_from_avg'
        title = f'Deviation from 2015-2019 Average - {title_suffix}'
        y_title = 'Deviation from Average Deaths'
    elif chart_type == 'deviation_expected':
        agg_data = plot_data.groupby(['year', 'mmwr_week'])['deviation_from_expected'].sum().reset_index()
        y_col = 'deviation_from_expected'
        title = f'Deviation from Expected Deaths (1.31% Annual Growth) - {title_suffix}'
        y_title = 'Deviation from Expected Deaths'

    # Create figure
    fig = go.Figure()

    # Add zero line for deviation charts
    if chart_type in ['deviation_avg', 'deviation_expected']:
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    # Add lines for each year
    for year in sorted(agg_data['year'].unique()):
        year_data = agg_data[agg_data['year'] == year]

        # Format the hover template based on chart type
        if chart_type == 'deaths_per_100k':
            hover_template = f'<b>Year {year}</b><br>Week: %{{x}}<br>{y_title}: %{{y:,.1f}}<extra></extra>'
        else:
            hover_template = f'<b>Year {year}</b><br>Week: %{{x}}<br>{y_title}: %{{y:,.0f}}<extra></extra>'

        fig.add_trace(go.Scatter(
            x=year_data['mmwr_week'],
            y=year_data[y_col],
            mode='lines',
            name=str(year),
            line=dict(color=get_color_for_year(year), width=2),
            hovertemplate=hover_template,
            showlegend=False
        ))

        # Add year annotation at the end of each line
        if not year_data.empty:
            last_week = year_data['mmwr_week'].max()
            last_value = year_data[year_data['mmwr_week'] == last_week][y_col].iloc[0]
            fig.add_annotation(
                x=last_week,
                y=last_value,
                text=str(year),
                showarrow=False,
                xshift=10,
                font=dict(color=get_color_for_year(year), size=10, family="Arial Black"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor=get_color_for_year(year),
                borderwidth=1
            )

    fig.update_layout(
        title=title,
        xaxis_title='MMWR Week',
        yaxis_title=y_title,
        hovermode='x unified',
        template='plotly_white',
        height=500,
        showlegend=False,
        margin=dict(l=50, r=50, t=50, b=50)
    )

    return fig


def calculate_metric(df, selected_states, metric_type):
    """Calculate metrics for sidebar"""
    if selected_states == ['All States']:
        filtered_data = df.copy()
    else:
        filtered_data = df[df['state'].isin(selected_states)]

    if metric_type == 'total_deaths':
        return int(filtered_data['deaths'].sum())
    elif metric_type == 'avg_deaths_per_100k':
        # Calculate average deaths per 100k across all time periods
        if 'population' not in filtered_data.columns:
            return 0
        total_deaths = filtered_data['deaths'].sum()
        total_population_years = filtered_data['population'].sum()
        if total_population_years == 0:
            return 0
        # Calculate average weekly rate and annualize it (approximately)
        weeks_in_data = len(filtered_data)
        avg_weekly_rate = (total_deaths / total_population_years) * 100000 * weeks_in_data
        return round(avg_weekly_rate * 52.18 / weeks_in_data, 1)  # Annualized rate
    elif metric_type == 'peak_deaths_per_100k':
        # Find the highest weekly deaths per 100k rate
        if 'population' not in filtered_data.columns:
            return 0
        # Group by year/week and calculate rates
        weekly_rates = filtered_data.groupby(['year', 'mmwr_week']).agg({
            'deaths': 'sum',
            'population': 'sum'
        }).reset_index()
        if len(weekly_rates) == 0 or weekly_rates['population'].sum() == 0:
            return 0
        weekly_rates['rate_per_100k'] = (weekly_rates['deaths'] / weekly_rates['population']) * 100000
        return round(weekly_rates['rate_per_100k'].max(), 1)
    elif metric_type == 'total_above_avg':
        # Focus on 2020-2022 period for pandemic impact
        pandemic_data = filtered_data[(filtered_data['year'] >= 2020) & (filtered_data['year'] <= 2022)]
        positive_deviations = pandemic_data[pandemic_data['deviation_from_avg'] > 0]
        return int(positive_deviations['deviation_from_avg'].sum())
    elif metric_type == 'total_above_expected':
        # Focus on 2020-2022 period for pandemic impact
        pandemic_data = filtered_data[(filtered_data['year'] >= 2020) & (filtered_data['year'] <= 2022)]
        positive_deviations = pandemic_data[pandemic_data['deviation_from_expected'] > 0]
        return int(positive_deviations['deviation_from_expected'].sum())

    return 0


def main():
    # Load data
    state_data, us_data = load_data()

    if state_data is None or us_data is None:
        st.stop()

    # Header
    st.markdown('<h1 class="main-header">Mortality Data Dashboard</h1>', unsafe_allow_html=True)

    # ALL SIDEBAR CONTENT GROUPED TOGETHER
    with st.sidebar:
        # Dataset selection
        st.header("Dataset")
        dataset_choice = st.radio(
            "Choose dataset type:",
            ["State-Level Data", "US National Data"],
            label_visibility="collapsed"
        )

        # State filter (only for state-level data)
        if dataset_choice == "State-Level Data":
            current_data = add_baseline_calculations(state_data)
            st.header("State Filter")
            available_states = ['All States'] + sorted(current_data['state'].unique().tolist())
            selected_states = st.multiselect(
                "Select states to include:",
                available_states,
                default=['All States'],
                label_visibility="collapsed"
            )
            if not selected_states:
                selected_states = ['All States']
        else:
            current_data = add_baseline_calculations(us_data)
            selected_states = ['United States']

    # MAIN CONTENT AREA
    # Create radio button for view selection
    view_choice = st.radio(
        "Select View:",
        ["Raw Deaths", "Deaths per 100k", "Deviation from Average", "Deviation from Expected"],
        horizontal=True,
        key="view_selector"
    )

    # Calculate metrics once
    total_deaths = calculate_metric(current_data, selected_states, 'total_deaths')
    avg_deaths_per_100k = calculate_metric(current_data, selected_states, 'avg_deaths_per_100k')
    peak_deaths_per_100k = calculate_metric(current_data, selected_states, 'peak_deaths_per_100k')
    total_above_avg = calculate_metric(current_data, selected_states, 'total_above_avg')
    total_above_expected = calculate_metric(current_data, selected_states, 'total_above_expected')

    # ADD SINGLE SIDEBAR METRIC SECTION BASED ON VIEW
    with st.sidebar:
        st.markdown("---")
        st.header("Current View")

        if view_choice == "Raw Deaths":
            st.metric("Total Deaths", f"{total_deaths:,}")
        elif view_choice == "Deaths per 100k":
            st.metric("Average Annual Rate", f"{avg_deaths_per_100k}")
            st.metric("Peak Weekly Rate", f"{peak_deaths_per_100k}")
        elif view_choice == "Deviation from Average":
            st.metric("Deaths Above Average (2020-2022)", f"{total_above_avg:,}")
        elif view_choice == "Deviation from Expected":
            st.metric("Deaths Above Expected (2020-2022)", f"{total_above_expected:,}")

        # Static sidebar info
        st.markdown("---")
        st.markdown("**Dataset Info**")
        st.text(f"Records: {len(current_data):,}")
        st.text(f"Years: {current_data['year'].min()}-{current_data['year'].max()}")

        st.markdown("---")
        st.markdown("**Year Colors**")
        st.markdown('<span style="color: #2E8B57;">■</span> **2015-2019**: Baseline', unsafe_allow_html=True)
        st.markdown('<span style="color: #DC143C;">■</span> **2020-2022**: Pandemic', unsafe_allow_html=True)
        st.markdown('<span style="color: #FF69B4;">■</span> **2023-2024**: Recent', unsafe_allow_html=True)
        st.markdown('<span style="color: #1E90FF;">■</span> **2025**: Current', unsafe_allow_html=True)

    # SHOW CONTENT BASED ON SELECTED VIEW
    if view_choice == "Raw Deaths":
        st.header("Raw Deaths per MMWR Week")
        st.markdown("This chart shows the absolute number of deaths per week over time.")
        fig = create_chart(current_data, selected_states, 'raw')
        st.plotly_chart(fig, use_container_width=True)

    elif view_choice == "Deaths per 100k":
        st.header("Deaths per 100k Population per MMWR Week")
        st.markdown(
            "This chart shows the deaths per 100,000 population per week, allowing comparison between different population sizes.")
        fig = create_chart(current_data, selected_states, 'deaths_per_100k')
        st.plotly_chart(fig, use_container_width=True)
        st.info(
            "Interpretation: This normalizes for population size, making it easier to compare mortality rates across different states or regions.")

    elif view_choice == "Deviation from Average":
        st.header("Deviation from 2015-2019 Average")
        st.markdown("This chart shows how weekly deaths compare to the 2015-2019 baseline average.")
        fig = create_chart(current_data, selected_states, 'deviation_avg')
        st.plotly_chart(fig, use_container_width=True)
        st.info("Interpretation: Positive values indicate weeks with more deaths than the 2015-2019 average.")

    elif view_choice == "Deviation from Expected":
        st.header("Deviation from Expected Deaths")
        st.markdown("This chart shows deviations from expected deaths assuming 1.31% annual growth from 2015 baseline.")
        fig = create_chart(current_data, selected_states, 'deviation_expected')
        st.plotly_chart(fig, use_container_width=True)
        st.info(
            "Interpretation: This accounts for normal population growth. Large positive deviations may indicate excess mortality events.")


if __name__ == "__main__":
    main()