#author: Manel Reghima
import psycopg2
import pandas as pd
import numpy as np
import csv
from config import config
from sqlalchemy import create_engine


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


def build_customer_journeys(df_conversions, df_session_sources, df_session_costs):
    # Merge session sources and session costs DataFrames based on the 'session_id' column
    merged_session_sources_costs = pd.merge(df_session_sources, df_session_costs, on='session_id')

    # Further merge the resulting DataFrame with the conversions DataFrame based on the 'user_id' column
    merged_df = pd.merge(merged_session_sources_costs, df_conversions, on='user_id')

    # Convert 'event_time' and 'conv_time' columns to proper datetime format for further operations
    merged_df['event_time'] = pd.to_datetime(merged_df['event_time'])
    merged_df['conv_time'] = pd.to_datetime(merged_df['conv_time'])

    # Create an empty list to hold individual customer journeys
    customer_journeys = []

    # Group the merged DataFrame by 'conv_id' (conversion ID) and 'user_id' for further processing
    grouped = merged_df.groupby(['conv_id', 'user_id'])

    # Iterate through each group based on the 'conv_id' and 'user_id' combination
    for (conv_id, user_id), group_data in grouped:
        # Sort the data within the group based on 'event_time'
        group_data = group_data.sort_values(by='event_time', ascending=True)

        # Identify entries where the 'event_time' is after the conversion time
        idx = group_data['event_time'] >= group_data['conv_time'].values[0]

        # Keep only the valid sessions that happened before the actual conversion
        valid_sessions = group_data.loc[idx]

        # Reorder the columns in the DataFrame to a specific sequence and handle missing values in 'cost' column
        valid_sessions = valid_sessions[['conv_id', 'session_id', 'event_time', 'channel_name',
                                         'holder_engagement', 'closer_engagement', 'cost', 'impression_interaction']]
        valid_sessions['cost'] = np.where(valid_sessions['cost'].isnull(), 0, 1)

        # Rename specific columns to more descriptive names
        valid_sessions.rename(columns={'conv_id': 'conversion_id', 'event_time': 'timestamp',
                                       'channel_name': 'channel_label', 'cost': 'conversion'}, inplace=True)

        # Convert the valid sessions DataFrame into a list of dictionaries
        customer_journey = valid_sessions.to_dict(orient='records')

        # Append each individual customer journey to the main list
        customer_journeys.extend(customer_journey)

    # Display the constructed customer journeys for verification
    print('Customer journey (List of Dictionaries)')
    for journey in customer_journeys:
        print(journey)

    # Store the final list of customer journeys in a CSV file for further analysis or record-keeping
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
    
    # Load channel weights from CSV into a DataFrame
    df_ihc_channel_weights = pd.read_csv('IHC_channel_weights.csv')
    
    # Load parameter training set from CSV into another DataFrame
    df_ihc_parameter_training_set = pd.read_csv('ihc_parameter_training_set.csv')

    # Merge the training set DataFrame with the channel weights DataFrame on their channel labels
    df_attribution_customer_journey = pd.merge(df_ihc_parameter_training_set, df_ihc_channel_weights, 
                         left_on="channel_label", right_on="channel", how="left")
    
    # Filter and retain only the required columns from the merged DataFrame
    df_attribution_customer_journey = df_attribution_customer_journey[['conversion_id', 'session_id', 'channel', 'impression_interaction', 'holder_engagement', 'closer_engagement',
                           'initializer weight', 'holder weight', 'closer weight']]
    
    # Rename the 'conversion_id' column to 'conv_id'
    df_attribution_customer_journey.rename(columns={'conversion_id': 'conv_id'}, inplace=True)

    # Calculate interaction weights for initializer (I), holder (H), and closer (C) based on engagement values and corresponding weights
    df_attribution_customer_journey['I'] = df_attribution_customer_journey['impression_interaction'] * df_attribution_customer_journey['initializer weight']
    df_attribution_customer_journey['H'] = df_attribution_customer_journey['holder_engagement'] * df_attribution_customer_journey['holder weight']
    df_attribution_customer_journey['C'] = df_attribution_customer_journey['closer_engagement'] * df_attribution_customer_journey['closer weight']
    
    # Remove the intermediate columns that are no longer needed
    df_attribution_customer_journey = df_attribution_customer_journey.drop(columns=['channel', 'impression_interaction', 'holder_engagement', 'closer_engagement',
                                        'initializer weight', 'holder weight', 'closer weight'])

    # Calculate the overall IHC (Initializer, Holder, Closer) score by averaging the weights of I, H, and C
    df_attribution_customer_journey['ihc'] = (df_attribution_customer_journey['I'] + df_attribution_customer_journey['H'] + df_attribution_customer_journey['C']) / 3
    
    # Drop the I, H, and C columns
    df_attribution_customer_journey = df_attribution_customer_journey.drop(columns=['I', 'H', 'C'])
    
    # Return the final DataFrame with customer journey attributions
    return df_attribution_customer_journey

def create_channel_reporting(df_session_sources, df_session_costs, df_attribution_customer_journey, df_conversions):
    
    # Merge session sources, session costs, customer journey attribution, and conversions DataFrames sequentially on session_id and user_id
    merged_df = df_session_sources.merge(df_session_costs, on='session_id', how='left').merge(
        df_attribution_customer_journey, on='session_id', how='left').merge(
        df_conversions, on='user_id', how='left')

    # Group the merged data by 'channel_name' and 'event_date' and then aggregate to compute the sum of costs, sum of ihc scores, and the ihc-weighted revenue
    df_channel_reporting = merged_df.groupby(['channel_name', 'event_date']).agg({
        'cost': 'sum',
        'ihc': 'sum',
        # Calculate the weighted revenue using the 'ihc' (Initializer, Holder, Closer) score
        'revenue': lambda x: (x * merged_df['ihc']).sum()
    }).reset_index()

    # Rename columns for clarity in the resulting DataFrame
    df_channel_reporting.columns = ['channel_name', 'date', 'cost', 'ihc', 'ihc_revenue']

    # Return the channel-wise reporting DataFrame with aggregated values
    return df_channel_reporting

def write_to_db(conn, df, table_name):
    """ Write the DataFrame to the specified table in the database """
    try:
        
        # Create a SQLAlchemy engine using psycopg2 and the given database connection
        engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
        
        # Use pandas to_sql method to write the DataFrame to the database
        # The table is replaced if it already exists
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Notify that the data has been successfully written to the table
        print(f"Data written to {table_name} successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        # Print an error message in case of exceptions during the process
        print(f"Error writing data to {table_name} table:", error)


def compute_metrics_and_export_csv(df_channel_reporting,file_name="channel_reporting_with_metrics.csv"):
    # Compute CPO
    df_channel_reporting['CPO'] = df_channel_reporting['cost'] / df_channel_reporting['ihc']
    
    # Compute ROAS
    df_channel_reporting['ROAS'] = df_channel_reporting['ihc_revenue'] / df_channel_reporting['cost']
    
    # Export to CSV
    df_channel_reporting.to_csv(file_name, index=False)
    
    print(f"Data exported to {file_name}")


if __name__ == '__main__':
    connect()
