#!/usr/bin/python
import psycopg2
import pandas as pd
import numpy as np
import csv
from config import config

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # Read connection parameters from a configuration
        params = config()

        # Connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # Create a new cursor to execute PostgreSQL commands
        cur = conn.cursor()

        # Execute a statement to fetch and display the PostgreSQL version
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)

        # Fetch data from the 'conversions' table and store it in a DataFrame
        df_conversions = fetch_data(conn, 'conversions')

        # Fetch data from the 'session_sources' table and store it in a DataFrame
        df_session_sources = fetch_data(conn, 'session_sources')

        # Fetch data from the 'session_costs' table and store it in a DataFrame
        df_session_costs = fetch_data(conn, 'session_costs')

        # Close the cursor to free up resources
        cur.close()

        # Process and combine the fetched data to build customer journeys
        build_customer_journeys(df_conversions, df_session_sources, df_session_costs)
        
        # Compute and store the attribution for each customer journey
        df_attribution_customer_journey = create_attribution_customer_journey()

        # Create a report for each channel based on the session sources, costs, attributions, and conversions
        df_channel_reporting = create_channel_reporting(df_session_sources, df_session_costs, df_attribution_customer_journey, df_conversions)
        
        # Write the computed attribution for customer journeys back to the database
        write_to_db(conn, df_attribution_customer_journey, "attribution_customer_journey")
        
        # Write the channel reporting data back to the database
        write_to_db(conn, df_channel_reporting, "channel_reporting")
        
        # Further compute metrics on channel reporting and export the results as a CSV
        final_channel_reporting = compute_metrics_and_export_csv(df_channel_reporting)

    # Handle any exceptions that arise during database operations
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # Ensure the database connection is closed after operations
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def read_csv_data(file_path):
    """ Read data from the specified CSV and return as DataFrame """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as error:
        print(f"Error reading data from {file_path}:", error)
        return None

def insert_data_to_table(conn, table_name, df):
    """ Insert DataFrame data into the specified table """
    try:
        # Use the pandas DataFrame's to_sql function
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data inserted into {table_name} successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting data into {table_name} table:", error)

def fetch_data(conn, table_name):
    """ Fetch data from the specified table and return as DataFrame """
    try:
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql(query, conn)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error fetching data from {table_name} table:", error)
        return None


def build_customer_journeys(df_conversions, df_session_sources,df_session_costs):
    # Merge the two DataFrames on the 'user_id' column
    merged_session_sources_costs=pd.merge(df_session_sources,df_session_costs, on='session_id')
    #print(merged_session_sources_costs)
    merged_df = pd.merge(merged_session_sources_costs, df_conversions, on='user_id')

    # Convert the timestamp columns to datetime objects
    merged_df['event_time'] = pd.to_datetime(merged_df['event_time'])
    merged_df['conv_time'] = pd.to_datetime(merged_df['conv_time'])

    # Initialize an empty list to store customer journeys
    customer_journeys = []

    # Group the data by 'conv_id' and 'user_id'
    grouped = merged_df.groupby(['conv_id', 'user_id'])

    for (conv_id, user_id), group_data in grouped:
        # Sort the group_data by 'event_time' in ascending order
        group_data = group_data.sort_values(by='event_time', ascending=True)

        # Find the index where 'event_time' is greater than or equal to 'conv_time'
        idx = group_data['event_time'] >= group_data['conv_time'].values[0]

        # Filter sessions that happened before the conversion timestamp
        valid_sessions = group_data.loc[idx]
        print(valid_sessions)
        #valid_sessions['revenue'] = np.where(valid_sessions['revenue'].isnull(), 0, 1)

        # Rearrange the columns in the DataFrame to match the specified order
        valid_sessions = valid_sessions[['conv_id', 'session_id', 'event_time', 'channel_name',
                                         'holder_engagement', 'closer_engagement', 'cost', 'impression_interaction']]

        valid_sessions['cost'] = np.where(valid_sessions['cost'].isnull(), 0, 1)
        valid_sessions.rename(columns={'conv_id': 'conversion_id', 'event_time': 'timestamp',
                                       'channel_name':'channel_label','cost':'conversion'}, inplace=True)
        
    

        # Create a list of dictionaries from the valid sessions DataFrame
        customer_journey = valid_sessions.to_dict(orient='records')

        # Append the customer journey to the list
        customer_journeys.extend(customer_journey)

    # Print the resulting customer journeys
    print('Customer journey (List of Dictionaries)')
    for journey in customer_journeys:
        print(journey)

    # Save the customer journeys to a CSV file
    save_customer_journeys_to_csv(customer_journeys)

def save_customer_journeys_to_csv(customer_journeys):
    # Define the CSV file path
    csv_file_path = 'ihc_parameter_training_set.csv'

    # Define the field names for the CSV header
    field_names = customer_journeys[0].keys() if customer_journeys else []

    # Write the customer journeys to the CSV file
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(customer_journeys)

    print(f'Customer journeys saved to {csv_file_path}')


def create_attribution_customer_journey():
    
    # Load the CSV files into dataframes
    df_ihc_channel_weights = pd.read_csv('IHC_channel_weights.csv')
    df_ihc_parameter_training_set = pd.read_csv('ihc_parameter_training_set.csv')

    # Merge the dataframes on the channel columns
    df_attribution_customer_journey = pd.merge(df_ihc_parameter_training_set, df_ihc_channel_weights, 
                         left_on="channel_label", right_on="channel", how="left")
    
    # Select specified columns
    df_attribution_customer_journey = df_attribution_customer_journey[['conversion_id','session_id','channel','impression_interaction','holder_engagement','closer_engagement',
                           'initializer weight','holder weight','closer weight']]
    
    df_attribution_customer_journey.rename(columns={'conversion_id': 'conv_id'}, inplace=True)

    df_attribution_customer_journey['I'] = df_attribution_customer_journey['impression_interaction'] * df_attribution_customer_journey['initializer weight']
    df_attribution_customer_journey['H'] = df_attribution_customer_journey['holder_engagement'] * df_attribution_customer_journey['holder weight']
    df_attribution_customer_journey['C'] = df_attribution_customer_journey['closer_engagement'] * df_attribution_customer_journey['closer weight']  
    
    df_attribution_customer_journey = df_attribution_customer_journey.drop(columns=['channel','impression_interaction','holder_engagement','closer_engagement',
                                        'initializer weight','holder weight','closer weight'])

    
    df_attribution_customer_journey['ihc'] = (df_attribution_customer_journey['I'] + df_attribution_customer_journey['H'] + df_attribution_customer_journey['C']) / 3 
    df_attribution_customer_journey = df_attribution_customer_journey.drop(columns=['I','H','C'])
    return df_attribution_customer_journey

def create_channel_reporting(df_session_sources, df_session_costs, df_attribution_customer_journey, df_conversions):
    
    # Merge the dataframes based on session_id and conv_id
    merged_df = df_session_sources.merge(df_session_costs, on='session_id', how='left').merge(
        df_attribution_customer_journey, on='session_id', how='left').merge(
        df_conversions, on='user_id', how='left')

    # Group by channel_name and event_date to aggregate required data
    df_channel_reporting = merged_df.groupby(['channel_name', 'event_date']).agg({
        'cost': 'sum',
        'ihc': 'sum',
        'revenue': lambda x: (x * merged_df['ihc']).sum()
    }).reset_index()

    # Rename the columns
    df_channel_reporting.columns = ['channel_name', 'date', 'cost', 'ihc', 'ihc_revenue']

    return df_channel_reporting


def write_to_db(conn, df, table_name):
    """ Write the DataFrame to the specified table in the database """
    try:
        # Use the pandas DataFrame's to_sql function
        # Use a dummy sqlalchemy engine since pandas requires it
        from sqlalchemy import create_engine
        engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
        
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data written to {table_name} successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error writing data to {table_name} table:", error)


def compute_metrics_and_export_csv(df_channel_reporting,file_name="channel_reporting_with_metrics.csv"):
    # Compute CPO
    df_channel_reporting['CPO'] = df_channel_reporting['cost'] / df_channel_reporting['ihc']
    
    # Handle cases where ihc is 0 (to avoid infinity values)
    df_channel_reporting['CPO'] = df_channel_reporting['CPO'].replace([float('inf'), -float('inf')], 0)
    
    # Compute ROAS
    df_channel_reporting['ROAS'] = df_channel_reporting['ihc_revenue'] / df_channel_reporting['cost']
    
    # Handle cases where cost is 0 (to avoid infinity values)
    df_channel_reporting['ROAS'] = df_channel_reporting['ROAS'].replace([float('inf'), -float('inf')], 0)
    
    # Export to CSV
    df_channel_reporting.to_csv(file_name, index=False)
    
    print(f"Data exported to {file_name}")


if __name__ == '__main__':
    connect()
