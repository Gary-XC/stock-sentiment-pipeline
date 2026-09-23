"""
dashboard.py

Phase 8: Real-Time Visualization Dashboard.
Polls PostgreSQL for the latest windowed metrics displaying 
interactive charts that show stock price vs. sentiment correlation.
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
import time

# Page configuration
st.set_page_config(
    page_title="Stock Sentiment Dashboard",
    page_icon="📈",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
    .positive { color: #28a745; font-weight: bold; }
    .negative { color: #dc3545; font-weight: bold; }
    .neutral { color: #6c757d; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=5)  # Cache for 5 seconds to reduce DB load
def fetch_data_from_postgres():
    """
    Fetches the latest stock sentiment metrics from PostgreSQL.
    
    Returns:
        pd.DataFrame: DataFrame containing the latest metrics.
    """
    try:
        conn = psycopg2.connect(
            dbname="sentiment_db",
            user="admin",
            password="admin",
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5433")
        )
        
        query = """
        SELECT 
            ticker,
            window_start,
            window_end,
            avg_price,
            min_price,
            max_price,
            price_count,
            avg_sentiment,
            headline_count
        FROM stock_sentiment_metrics
        ORDER BY window_start DESC
        LIMIT 50
        """
        
        df = pd.read_sql_query(query, conn) # type: ignore
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def get_sentiment_color(score):
    """Returns CSS class based on sentiment score."""
    if score > 0.05:
        return "positive"
    elif score < -0.05:
        return "negative"
    else:
        return "neutral"

def main():
    """Main Streamlit dashboard application."""
    st.title(" Real-Time Stock Sentiment Dashboard")
    st.markdown("**Live monitoring of stock prices vs. social media sentiment**")
    st.markdown("---")
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)
    
    # Fetch data
    df = fetch_data_from_postgres()
    
    if df.empty:
        st.warning("️ No data available yet. Waiting for the first window to close...")
        time.sleep(2)
        st.rerun()
        return
    
    # Convert timestamps
    df['window_start'] = pd.to_datetime(df['window_start'])
    df = df.sort_values('window_start')
    
    # Sidebar filters
    st.sidebar.header("Filters")
    tickers = df['ticker'].unique()
    selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)
    
    # Filter data
    filtered_df = df[df['ticker'] == selected_ticker]
    
    # Calculate metrics
    latest_price = filtered_df['avg_price'].iloc[-1] if not filtered_df.empty else 0
    latest_sentiment = filtered_df['avg_sentiment'].iloc[-1] if not filtered_df.empty else 0
    total_windows = len(filtered_df)
    avg_volume = filtered_df['headline_count'].mean()
    
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label=f"Latest Price ({selected_ticker})",
            value=f"${latest_price:.2f}",
            delta=f"{latest_price - filtered_df['avg_price'].iloc[0]:.2f}" if len(filtered_df) > 1 else "0.00"
        )
    
    with col2:
        sentiment_delta = "Neutral"
        if latest_sentiment > 0.1:
            sentiment_delta = "Very Positive 😊"
        elif latest_sentiment > 0.05:
            sentiment_delta = "Positive 🙂"
        elif latest_sentiment < -0.1:
            sentiment_delta = "Very Negative 😞"
        elif latest_sentiment < -0.05:
            sentiment_delta = "Negative 🙁"
        
        st.metric(
            label="Current Sentiment",
            value=f"{latest_sentiment:.3f}",
            delta=sentiment_delta
        )
    
    with col3:
        st.metric(
            label="Time Windows",
            value=total_windows
        )
    
    with col4:
        st.metric(
            label="Avg News Volume",
            value=f"{avg_volume:.1f}"
        )
    
    st.markdown("---")
    
    # Main chart: Price vs Sentiment
    st.subheader("📊 Price & Sentiment Over Time")
    
    # Create dual-axis chart
    chart_data = filtered_df.set_index('window_start')[['avg_price', 'avg_sentiment']].copy()
    
    # Normalize sentiment for better visualization (scale to price range)
    price_range = chart_data['avg_price'].max() - chart_data['avg_price'].min()
    chart_data['sentiment_scaled'] = (
        chart_data['avg_sentiment'] * price_range / 2 + 
        (chart_data['avg_price'].max() + chart_data['avg_price'].min()) / 2
    )
    
    st.line_chart(
        chart_data[['avg_price']],
        use_container_width=True
    )
    
    # Separate sentiment chart
    st.line_chart(
        chart_data[['avg_sentiment']],
        use_container_width=True
    )
    
    # Correlation analysis
    if len(filtered_df) > 2:
        correlation = filtered_df['avg_price'].corr(filtered_df['avg_sentiment'])
        st.markdown(f"**Price-Sentiment Correlation:** `{correlation:.3f}`")
        if correlation > 0.5:
            st.success("Strong positive correlation - sentiment appears to drive price! 📈")
        elif correlation < -0.5:
            st.error("Strong negative correlation - inverse relationship detected! ")
        else:
            st.info("Weak correlation - price and sentiment moving independently. ➡️")
    
    st.markdown("---")
    
    # Recent data table
    st.subheader("📋 Recent Window Metrics")
    st.dataframe(
        filtered_df[['window_start', 'avg_price', 'min_price', 'max_price', 
                     'avg_sentiment', 'headline_count']].tail(10),
        use_container_width=True
    )
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(5)
        st.rerun()

if __name__ == "__main__":
    main()