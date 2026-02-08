# Import selenium packages
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Import other packages
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf

# Import src
from etl.src.utils import load_query

# Setup global logger
from etl.src.logger_setup import logger

# Load chrome driver path
load_dotenv()
chrome_driver_path = os.getenv("CHROME_DRIVER_PATH")

# Create db connection string
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_conn_str = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

# Set url for page on tmx website
tmx_link = "https://money.tmx.com/en/quote/%5ETSX/constituents"

# Set name for table in database
table_name = "tickers"


# Create dict for company names and symbols
tsx_constituents = {"company_name": [],
                    "company_s": []}


logger.info(f"Beginning ETL process to extract TSX Tickers.")

# Configure webdriver
service = Service(executable_path=chrome_driver_path)
driver = webdriver.Chrome(service=service)


# Setup page counter
i = 1

# Setup main function
try:
    driver.get(tmx_link)

    # Wait until table is present
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, "ConstituentsList__ConstituentsTable-sc-q9ist-1"))
    )

    # Create while loop
    while True:
        # Load list from page
        tmx_soup = BeautifulSoup(driver.page_source, "html.parser")

        # Get the constituent list table
        constituent_table = tmx_soup.find('div','ConstituentsList__ConstituentsTable-sc-q9ist-1')

        # Get the company names and tickers from this table
        company_names = constituent_table.find_all('div', 'ConstituentsList__CompanyName-sc-q9ist-6')
        company_symbols = constituent_table.find_all('span', 'ConstituentsList__SymbolLink-sc-q9ist-7')

        # Extract names and symbols
        for name_div, symbol_div in zip(company_names, company_symbols):
            tsx_constituents['company_name'].append(name_div.text)
            tsx_constituents['company_s'].append(symbol_div.text)

        # Find the next page button
        next_button = WebDriverWait(driver, 10).until(
            lambda x: x.find_element(By.CSS_SELECTOR, "button[data-testid='paginator-next']")
        )

        # Check if it is disabled
        is_disabled = next_button.get_attribute("disabled")

        if is_disabled is not None:
            logger.info("All symbols have been collected. Terminating data collection and closing driver session.")
            driver.quit()
            break

        # Click button with execute script to bypass sticky banner ads
        driver.execute_script("arguments[0].click();", next_button)
        logger.info(f"Company data extracted from page {i}")
        i+=1

# Catch any exception
except Exception:
    logger.exception(f"An unexpected error occurred on page {i}. Halting program execution.")
    raise

# Put data into a dataframe
df = pd.DataFrame(tsx_constituents)

# Generate yfinance ticker
df["ticker"] = df["company_s"].str.replace(".", "-") + ".TO"

# Set values for exchange
df["exchange"] = "TSX"

# Define function to pull country, industry, and sector from yfinance
@logger.catch(reraise=True)
def get_ticker_info(row):
    info = yf.Ticker(row['ticker']).info
    return info.get('country'), info.get('industry'), info.get('sector'), info.get('financialCurrency')

# Get other company info from yfinance
df[['country', 'industry', 'sector', 'reporting_currency']] = df.apply(get_ticker_info, axis=1, result_type='expand')

# Drop company_s column
df.drop(columns='company_s', inplace=True)

# Reorder column to conform to db
df = df[['ticker', 'company_name', 'exchange', 'country', 'industry', 'sector', 'reporting_currency']]

# Drop columns with nulls
if len(df[df.isnull().any(axis=1)]) > 0:
    logger.warning(
        f"The following company data will not be uploaded due to null values: \n "
        f"{df[df.isnull().any(axis=1)]}"
    )
    df.dropna(inplace=True)

# Load table into database
load_query(table_name=table_name, df=df, append=True, db_conn_str=db_conn_str)
logger.info(f"Process to extract TSX Tickers complete.")