import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# Database connection
POSTGRES_CONN = f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:{os.getenv('POSTGRES_PASSWORD', 'airflow')}@{os.getenv('POSTGRES_HOST', 'postgres')}/{os.getenv('POSTGRES_DB', 'weather_data')}"

def load_data():
    """Load weather data from PostgreSQL."""
    engine = create_engine(POSTGRES_CONN)
    query = """
    SELECT * FROM weather_data 
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    ORDER BY timestamp DESC
    """
    return pd.read_sql(query, engine)

def main():
    st.title("Weather Data Dashboard")
    st.write("Interactive visualization of weather data across multiple cities")

    # Load data
    try:
        df = load_data()
        
        # Sidebar filters
        st.sidebar.header("Filters")
        cities = st.sidebar.multiselect(
            "Select Cities",
            options=df['city'].unique(),
            default=df['city'].unique()[:3]
        )
        
        # Filter data based on selection
        filtered_df = df[df['city'].isin(cities)]
        
        # Temperature Trends
        st.header("Temperature Trends (Last 30 Days)")
        fig_temp = px.line(
            filtered_df,
            x='timestamp',
            y='temperature',
            color='city',
            title='Temperature Trends by City'
        )
        st.plotly_chart(fig_temp)
        
        # Humidity Trends
        st.header("Humidity Trends (Last 30 Days)")
        fig_humidity = px.line(
            filtered_df,
            x='timestamp',
            y='humidity',
            color='city',
            title='Humidity Trends by City'
        )
        st.plotly_chart(fig_humidity)
        
        # Current Weather Stats
        st.header("Current Weather Statistics")
        latest_data = filtered_df.groupby('city').first().reset_index()
        st.dataframe(latest_data[['city', 'temperature', 'humidity', 'pressure', 'wind_speed', 'weather_description']])
        
        # Weather Description Distribution
        st.header("Weather Conditions Distribution")
        weather_counts = filtered_df['weather_description'].value_counts()
        fig_weather = px.pie(
            values=weather_counts.values,
            names=weather_counts.index,
            title='Weather Conditions Distribution'
        )
        st.plotly_chart(fig_weather)
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info("Please ensure the database is running and contains weather data.")

if __name__ == "__main__":
    main() 