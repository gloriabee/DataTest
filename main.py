import os
import pandas as pd
import kaggle
from dotenv import load_dotenv
from sqlalchemy import create_engine

def main():
    #Load environment variables
    load_dotenv()
    DATABASE_URL = os.getenv("POSTGRES_URL")
    if not DATABASE_URL:
        raise ValueError("POSTGRES_URL not found in .env file")

    # 1. Retrieve data from Kaggle API
    os.makedirs("./data", exist_ok=True)
    kaggle.api.authenticate()
    print("Downloading dataset from Kaggle...")
    kaggle.api.dataset_download_files(
        "pavansubhasht/ibm-hr-analytics-attrition-dataset",
        path="./data",
        unzip=True
    )

    # Read CSV file as data source
    csv_path = "./data/WA_Fn-UseC_-HR-Employee-Attrition.csv"
    df = pd.read_csv(csv_path)

    # 2. Do Simple Transform

    # Remove rows with null values
    df = df.dropna()

    # Drop irrelevant columns
    drop_columns = ["Over18", "EmployeeCount", "StandardHours", "EmployeeNumber","StockOptionLevel","TrainingTimesLastYear"]
    df.drop(columns=drop_columns, inplace=True, errors="ignore")

    # Separate employees who left
    left_df = df[df["Attrition"] == "Yes"]

    # Separate employees who stayed 
    stay_df = df[df["Attrition"] == "No"]

    # 3. Connect and insert into PostgreSQL
    print("Loading data into PostgreSQL")
    engine = create_engine(DATABASE_URL)
    df.to_sql("All_employees",con=engine,if_exists="replace",index=False)
    left_df.to_sql("left_employee", con=engine, if_exists="replace", index=False)
    stay_df.to_sql("Stayed_employee",con=engine,if_exists="replace",index=False)

    print("Data loaded to PostgreSQL table")

if __name__ == "__main__":
    main()
