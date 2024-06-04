#####################FInalllllll########################################

import pandas as pd
import numpy as np
import os
import sqlite3
import ssl
from urllib.request import urlretrieve

def load_datasets():
    """
    Load datasets from given URLs and return as DataFrames.
    """
    url1 = "https://offenedaten-konstanz.de/sites/default/files/Quellenbezogene_CO2_Emissionen_2010-2017_nach_Sektoren_0.csv"
    url2 = "https://offenedaten-konstanz.de/node/40911/download"
    
    gs = pd.read_csv(url1, delimiter=';')
    co2 = pd.read_csv(url2, delimiter=';')
    
    return gs, co2

def preprocess_gs(gs):
    """
    Preprocess the gs DataFrame: Rename columns, group by year and select first entry for each year.
    """
    gs.rename(columns={'n': 'Inhabitants'}, inplace=True)
    gs1 = gs.groupby('Jahr').first().reset_index()
    return gs1

def preprocess_co2(co2):
    """
    Preprocess the co2 DataFrame: Select specific rows, rename and drop columns.
    """
    co3 = co2[64:72].copy()
    co3.drop(columns="MESS_DATUM_ENDE", inplace=True)
    co3.rename(columns={'MESS_DATUM_BEGINN': 'Jahr'}, inplace=True)
    co3['Jahr'] = co3['Jahr'].astype(str).str[:4]
    return co3
    

def merge_dataframes(gs1, co3):
    """
    Merge the two preprocessed DataFrames on the 'Jahr' column.
    """
    gs1['Jahr'] = gs1['Jahr'].astype(str)
    co3['Jahr'] = co3['Jahr'].astype(str)
    merged_df = pd.merge(gs1, co3, on='Jahr')
    return merged_df

def main():
    # Load datasets
    gs, co2 = load_datasets()
    
    # Preprocess the datasets
    gs1 = preprocess_gs(gs)
    co3 = preprocess_co2(co2)
    
    # Merge the datasets
    merged_df = merge_dataframes(gs1, co3)
    
    # Display the merged DataFrame
    print(merged_df)

    # Create an SQLite database connection
    conn = sqlite3.connect('merged_dataset.db')
    cursor = conn.cursor()
    
    # Store the merged DataFrame in the SQLite database
    merged_df.to_sql('merged_data', conn, if_exists='replace', index=False)
    
    # Commit and close the connection
    conn.commit()
    
    # Query all rows from the merged data table
    query = "SELECT * FROM merged_data LIMIT 5"
    queried_df = pd.read_sql_query(query, conn)
    
    # Display the first few rows of the queried DataFrame to confirm
    print("Queried DataFrame from SQLite:")
    print(queried_df)
    
    # Close the connection
    conn.close()
    
    # Save the merged DataFrame to a CSV file
    output_csv = 'merged_dataset.csv'
    merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"Merged dataset saved as {output_csv}")


if __name__ == "__main__":
    main()
