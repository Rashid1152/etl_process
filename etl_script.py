import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf
import requests
from collections import defaultdict
import unidecode
import numpy as np
import logging
import time

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
OLIST_SELLERS_CSV = "olist_sellers_dataset.csv"
OLIST_ORDERS_CSV = "olist_orders_dataset.csv"
OLIST_ORDER_ITEMS_CSV = "olist_order_items_dataset.csv"
OLIST_GEO_CSV = "olist_geolocation_dataset.csv"
OLIST_CUSTOMERS_CSV = "olist_customers_dataset.csv"
OUTPUT_CSV = "aggregated_seller_order_context.csv"

SP500_TICKER = "^GSPC"
WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEZONE = 'America/Sao_Paulo'
VALID_LAT_RANGE = (-90, 90)
VALID_LON_RANGE = (-180, 180)


def validate_coordinates(lat, lon):
    """
    Validate latitude and longitude ranges.
    Args:
        lat (float): Latitude value.
        lon (float): Longitude value.
    Returns:
        bool: True if coordinates are within valid ranges, False otherwise.
    """
    return VALID_LAT_RANGE[0] <= lat <= VALID_LAT_RANGE[1] and VALID_LON_RANGE[0] <= lon <= VALID_LON_RANGE[1]

def clean_string_column(series):
    """
    Clean string columns: remove accents, lowercase, strip whitespace.
    Args:
        series (pd.Series): Pandas Series of strings.
    Returns:
        pd.Series: Cleaned string series.
    """
    return series.apply(lambda x: unidecode.unidecode(str(x)).lower().strip())

def clean_raw_data(df, file_name):
    """
    Enhanced data cleaning with validation for each Olist dataset.
    Args:
        df (pd.DataFrame): Raw dataframe to clean.
        file_name (str): Name of the file (for logging).
    Returns:
        pd.DataFrame: Cleaned dataframe.
    """
    logging.info(f"Cleaning {file_name}...")
    initial_rows = len(df)
    cleaned_df = df.copy()

    # Clean sellers dataset
    if 'seller_zip_code_prefix' in df.columns:
        cleaned_df['seller_city'] = clean_string_column(cleaned_df['seller_city'])
        cleaned_df = cleaned_df.dropna(subset=['seller_id']).drop_duplicates('seller_id')

    # Clean order_items dataset
    elif 'order_id' in df.columns and 'order_item_id' in df.columns:
        required_cols = ['order_id', 'order_item_id', 'product_id', 'seller_id', 'price']
        cleaned_df = cleaned_df.dropna(subset=required_cols)
        cleaned_df = cleaned_df.drop_duplicates(['order_id', 'order_item_id'])

    # Clean orders dataset
    elif 'order_status' in df.columns:
        required_cols = ['order_id', 'order_purchase_timestamp', 'order_delivered_customer_date']
        cleaned_df = cleaned_df.dropna(subset=required_cols)
        cleaned_df = cleaned_df[cleaned_df['order_status'] == 'delivered']
        cleaned_df = cleaned_df.drop_duplicates('order_id')

    # Clean geolocations dataset
    elif 'geolocation_zip_code_prefix' in df.columns:
        cleaned_df['geolocation_city'] = clean_string_column(cleaned_df['geolocation_city'])
        cleaned_df = cleaned_df.dropna(subset=['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'])
        # Validate coordinates
        cleaned_df = cleaned_df[cleaned_df.apply(lambda x: validate_coordinates(x['geolocation_lat'], x['geolocation_lng']), axis=1)]

    logging.info(f"Cleaned {initial_rows} → {len(cleaned_df)} rows")
    return cleaned_df

def load_olist_data():
    """
    Load and validate all Olist datasets, including customers.
    Returns:
        tuple: sellers, orders, order_items, geolocations, customers (all pd.DataFrame)
    """
    dtype_map = {'seller_zip_code_prefix': 'string', 'geolocation_zip_code_prefix': 'string', 'customer_zip_code_prefix': 'string'}
    
    # Load each dataset
    sellers = pd.read_csv(OLIST_SELLERS_CSV, dtype=dtype_map)
    orders = pd.read_csv(OLIST_ORDERS_CSV, parse_dates=['order_purchase_timestamp', 'order_delivered_customer_date'])
    order_items = pd.read_csv(OLIST_ORDER_ITEMS_CSV)
    geolocations = pd.read_csv(OLIST_GEO_CSV, dtype=dtype_map)
    customers = pd.read_csv(OLIST_CUSTOMERS_CSV, dtype=dtype_map)
    
    # Clean each dataset
    sellers = clean_raw_data(sellers, 'sellers')
    orders = clean_raw_data(orders, 'orders')
    order_items = clean_raw_data(order_items, 'order_items')
    geolocations = clean_raw_data(geolocations, 'geolocations')
    
    # Drop customers with missing zip code
    customers = customers.dropna(subset=['customer_id', 'customer_zip_code_prefix'])
    return sellers, orders, order_items, geolocations, customers

def process_geolocations(geolocations):
    """
    Process geolocation data by averaging coordinates per zip prefix and validating them.
    Args:
        geolocations (pd.DataFrame): Cleaned geolocations dataframe.
    Returns:
        pd.DataFrame: Processed geolocations with valid averaged coordinates.
    """
    # Group by zip prefix and average lat/lon
    geo_processed = geolocations.groupby('geolocation_zip_code_prefix').agg({
        'geolocation_lat': 'mean',
        'geolocation_lng': 'mean',
        'geolocation_city': 'first',
        'geolocation_state': 'first'
    }).reset_index()
   
    # Keep only valid coordinates
    return geo_processed[geo_processed.apply(lambda x: validate_coordinates(x['geolocation_lat'], x['geolocation_lng']), axis=1)]

def join_olist_data(sellers, orders, order_items, geo_processed, customers):
    """
    Join Olist datasets to link sellers to their orders and items, and orders to customer geolocation.
    Args:
        sellers (pd.DataFrame): Cleaned sellers dataframe.
        orders (pd.DataFrame): Cleaned orders dataframe.
        order_items (pd.DataFrame): Cleaned order_items dataframe.
        geo_processed (pd.DataFrame): Processed geolocations dataframe.
        customers (pd.DataFrame): Cleaned customers dataframe.
    Returns:
        pd.DataFrame: Joined dataframe with customer-based geolocation for each order.
    """
    # Merge order_items with orders to get customer_id and timestamps
    orders_items = pd.merge(
        order_items,
        orders[['order_id', 'customer_id', 'order_purchase_timestamp', 'order_delivered_customer_date']],
        on='order_id',
        how='inner'
    )
    # Calculate total price per item
    orders_items['total_price'] = orders_items['price'] + orders_items['freight_value']
    # Aggregate at order, seller, customer, and date level
    order_agg = orders_items.groupby(
        ['order_id', 'seller_id', 'customer_id', 'order_purchase_timestamp', 'order_delivered_customer_date'],
        as_index=False
    ).agg(total_order_value=('total_price', 'sum'))
    
    # Join with customers to get customer_zip_code_prefix
    result = pd.merge(order_agg, customers[['customer_id', 'customer_zip_code_prefix']], on='customer_id', how='left')
    # Join with geolocations using customer_zip_code_prefix
    result = pd.merge(result, geo_processed, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')
    
    # Rename columns for clarity
    result = result.rename(columns={
        'geolocation_lat': 'delivery_location_latitude',
        'geolocation_lng': 'delivery_location_longitude'
    })
    # Drop rows with missing required columns
    required_cols = [
        'order_id', 'seller_id', 'order_purchase_timestamp',
        'order_delivered_customer_date', 'total_order_value',
        'delivery_location_latitude', 'delivery_location_longitude'
    ]
    
    return result.dropna(subset=required_cols)[required_cols]

def batch_fetch_sp500(dates):
    """
    Batch fetch S&P 500 closing prices for a list of dates using yfinance.
    Args:
        dates (list or np.ndarray): List of date strings (YYYY-MM-DD) to fetch prices for.
    Returns:
        dict: Mapping from date string to closing price (float). Missing dates are filled with last available price.
    """
    try:
        # Initialize yfinance Ticker object
        sp500 = yf.Ticker(SP500_TICKER)
        start_date = min(dates)
        end_date = max(dates)
       
        # Fetch historical data for the date range
        history = sp500.history(start=start_date, end=datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1), interval='1d')
        if history.empty:
            return {}
        
        # Convert index to string for mapping
        history.index = history.index.strftime("%Y-%m-%d")
        sp500_data = history['Close'].to_dict()
       
        # Fill missing dates with last available price
        last_price = None
        date_price_map = {}
        for date in dates:
            if date in sp500_data:
                last_price = sp500_data[date]
            date_price_map[date] = last_price
        return date_price_map
    except Exception as e:
        logging.error(f"S&P 500 fetch error: {e}")
        return {}

def retry_request(url, params, retries=3, delay=5):
    """
    Retry a GET request with exponential backoff for robustness.
    Args:
        url (str): The API endpoint URL.
        params (dict): Query parameters for the request.
        retries (int): Number of retry attempts.
        delay (int): Delay in seconds between retries.
    Returns:
        requests.Response or None: Response object if successful, None otherwise.
    """
    for attempt in range(retries):
        try:
            # Attempt the API request
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response
        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    return None

def batch_fetch_weather(location_dates):
    """
    Batch fetch weather data for multiple (lat, lon, date) combinations using Open-Meteo API.
    Args:
        location_dates (list): List of tuples: ((lat, lon), date_string)
    Returns:
        dict: Mapping from (lat, lon, date) to weather data dict {'mean_temp': float, 'precipitation': float}
    """
    weather_data = {}
    location_groups = defaultdict(list)
    
    # Group requests by location to minimize API calls
    for (lat, lon), date in location_dates:
        location_groups[(lat, lon)].append(date)
    for (lat, lon), dates in location_groups.items():
        if not validate_coordinates(lat, lon):
            continue
        start_date = min(dates)
        end_date = max(dates)
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'daily': ['temperature_2m_mean', 'precipitation_sum'],
            'timezone': TIMEZONE
        }
        
        # Use retry logic for robustness
        response = retry_request(WEATHER_API_URL, params)
        if response:
            data = response.json()
            if 'daily' in data:
                for i, date in enumerate(data['daily']['time']):
                    if date in dates:
                        weather_data[(lat, lon, date)] = {
                            'mean_temp': data['daily']['temperature_2m_mean'][i],
                            'precipitation': data['daily']['precipitation_sum'][i]
                        }
    return weather_data

def enrich_data(df):
    """
    Enrich the joined Olist data with market sentiment and weather data.
    Args:
        df (pd.DataFrame): DataFrame with joined Olist and geolocation data.
    Returns:
        pd.DataFrame: Enriched DataFrame with market sentiment and weather columns added.
    """
    
    # Convert timestamps to timezone-aware datetimes
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp']) \
        .dt.tz_localize(TIMEZONE, ambiguous='NaT', nonexistent='NaT') \
        .dt.tz_convert('UTC')
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date']) \
        .dt.tz_localize(TIMEZONE, ambiguous='NaT', nonexistent='NaT') \
        .dt.tz_convert('UTC')
    
    # Drop rows with invalid timestamps
    df = df.dropna(subset=['order_purchase_timestamp', 'order_delivered_customer_date'])
    # Fetch S&P 500 data for all unique purchase dates
    purchase_dates = df['order_purchase_timestamp'].dt.strftime("%Y-%m-%d").unique()
    sp500_prices = batch_fetch_sp500(purchase_dates)
    
    # Map market sentiment to each order
    df['market_sentiment_on_purchase_date'] = df['order_purchase_timestamp'].dt.strftime("%Y-%m-%d").map(sp500_prices)
    # Prepare location-date tuples for weather enrichment
    location_dates = [((row['delivery_location_latitude'], row['delivery_location_longitude']),
                      row['order_delivered_customer_date'].strftime("%Y-%m-%d")) for _, row in df.iterrows()]
    weather_data = batch_fetch_weather(location_dates)
    
    # Create a key for each row to map weather data
    df['weather_key'] = df.apply(lambda row: (
        row['delivery_location_latitude'],
        row['delivery_location_longitude'],
        row['order_delivered_customer_date'].strftime("%Y-%m-%d")), axis=1)
    
    # Map weather data to each order
    df['delivery_date_mean_temp'] = df['weather_key'].map(lambda x: weather_data.get(x, {}).get('mean_temp'))
    df['delivery_date_precipitation_sum'] = df['weather_key'].map(lambda x: weather_data.get(x, {}).get('precipitation'))
    df = df.drop(columns=['weather_key'])
   
    # Define required output fields
    final_fields = [
        'order_id', 'seller_id', 'order_purchase_timestamp', 'order_delivered_customer_date',
        'total_order_value', 'market_sentiment_on_purchase_date',
        'delivery_location_latitude', 'delivery_location_longitude',
        'delivery_date_mean_temp', 'delivery_date_precipitation_sum'
    ]
   
   # Drop rows with missing required fields
    return df.dropna(subset=final_fields)[final_fields]

def main():
    """
    Main ETL pipeline function. Loads, cleans, joins, enriches, and outputs the final dataset.
    Orchestrates the full ETL process and handles errors.
    Returns:
        None
    """
    try:
        # Load and clean all datasets
        sellers, orders, order_items, geolocations, customers = load_olist_data()
        # Process geolocations (average and validate coordinates)
        geo_processed = process_geolocations(geolocations)
        # Join all datasets to get customer-based geolocation for each order
        joined_data = join_olist_data(sellers, orders, order_items, geo_processed, customers)
        # Enrich with market sentiment and weather data
        enriched_data = enrich_data(joined_data)
        # Output to CSV
        enriched_data.to_csv(OUTPUT_CSV, index=False)
        logging.info(f"ETL completed! Output saved to {OUTPUT_CSV}")
        logging.info(enriched_data.head(3).to_string())
    except Exception as e:
        logging.error(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    main()
