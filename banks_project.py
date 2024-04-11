import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


# log the progress of the code
def log_progress(message):
    now = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    with open("code_log.txt", "a") as file:
        file.write(now + " : " + message + "\n")


# extract the data from the page
def extract(page_url: str, table_attrs: list) -> pd.DataFrame:
    # get the html content
    page_content = requests.get(page_url).content
    soup = BeautifulSoup(page_content, "html.parser")
    table = soup.find_all("table")[0]
    # extract the table rows and data for data needed
    extracted_data = pd.DataFrame(columns=table_attrs)
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) != 0:
            data_dict = {
                "Name": cols[1].text.strip(),
                "MC_USD_Billion": cols[2].text.strip(),
            }
            extracted_data = pd.concat(
                [extracted_data, pd.DataFrame(data_dict, index=[0])],
                ignore_index=True,
            )
    return extracted_data


# tramsform the data
def transform(extracted_data: pd.DataFrame) -> pd.DataFrame:
    exchange_dataframe = pd.read_csv("exchange_rate.csv")
    exchange_dict = exchange_dataframe.set_index("Currency").to_dict()["Rate"]

    extracted_data["MC_USD_Billion"] = extracted_data["MC_USD_Billion"].astype(float)
    extracted_data["MC_GBP_Billion"] = extracted_data["MC_USD_Billion"].apply(
        lambda x: np.round(x * exchange_dict["GBP"], 2)
    )
    extracted_data["MC_EUR_Billion"] = extracted_data["MC_USD_Billion"].apply(
        lambda x: np.round(x * exchange_dict["EUR"], 2)
    )
    extracted_data["MC_INR_Billion"] = extracted_data["MC_USD_Billion"].apply(
        lambda x: np.round(x * exchange_dict["INR"], 2)
    )
    return extracted_data


# load the data
def load_to_csv(transformed_data: pd.DataFrame, csv_file_path: str) -> None:
    transformed_data.to_csv(csv_file_path)


def load_to_db(
    conn: sqlite3.Connection, table_name: str, data_to_upload: pd.DataFrame
) -> None:
    data_to_upload.to_sql(table_name, conn, if_exists="replace", index=False)


# run custom queries needed
def run_query(query: str, conn: sqlite3.connect) -> pd.DataFrame:
    data_returned = pd.read_sql(query, conn)
    return data_returned


# URL of the webpage to scrape
url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
# Attributes of the table to extract
table_attrs = ["Name", "MC_USD_Billion"]
# Path to save the extracted data as CSV
csv_file_path = "./Largest_banks_data.csv"
# Name of the database file
db_name = "Banks.db"
# Name of the table in the database
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")

# Extract data from the webpage
extracted_data = extract(url, table_attrs)

log_progress("Data extraction complete. Initiating Transformation process")

# Transform the extracted data
transformed_data = transform(extracted_data)

log_progress("Data transformation complete. Initiating Loading process")

# Load transformed data to CSV
load_to_csv(transformed_data, csv_file_path)

log_progress("Data saved to CSV file")

# Connect to the SQLite database
connection = sqlite3.connect(db_name)

log_progress("SQL Connection initiated")

# Load transformed data to the database
load_to_db(connection, table_name, transformed_data)

log_progress("Data loaded to Database as a table, Executing queries")

# Run custom queries
select_all = f"SELECT * FROM {table_name}"
average_market = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
top_5_banks = f"SELECT Name from {table_name} LIMIT 5"

all_from_db = run_query(select_all, connection)
average_from_db = run_query(average_market, connection)
top_5_banks_from_db = run_query(top_5_banks, connection)

# Print the results of the queries
print("all_from_db")
print(all_from_db)
print("average_from_db")
print(average_from_db)
print("top_5_banks_from_db")
print(top_5_banks_from_db)

log_progress("Process Complete")

# Close the database connection
connection.close()

log_progress("Server Connection closed" + "\n")
