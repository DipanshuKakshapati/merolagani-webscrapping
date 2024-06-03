# %% merolagani_webscrapping.py
# ## Importing requried libraries

# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

import time

# %% [markdown]
# ## Defining the library

# %%
url = 'https://merolagani.com/StockQuote.aspx'

# %% [markdown]
# ## Defning the Safari library

# %%
driver = webdriver.Safari()

time.sleep(5)

# %%
driver.get(url)
time.sleep(5)

# %% [markdown]
# ## Getting the first page

# %% [markdown]
# ### Getting the headers of the table 

# %%
header_element = driver.find_element(by=By.XPATH, value = '//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[3]/table/thead')
header_text = header_element.text 

header_list = [item.strip() for item in header_text.split('\n') if item.strip()]
print(header_list)

# %% [markdown]
# ### Getting the table of the first page

# %%
table_element_1 = driver.find_element(by=By.XPATH, value='//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[3]/table')

# %% [markdown]
# ### Getting the inner html of the table

# %%
inner_html_page_1 = table_element_1.get_attribute('innerHTML')

# %%
print(inner_html_page_1)

# %% [markdown]
# ## Getting the second page

# %%
time.sleep(20)
page_selector_2 = driver.find_element(by=By.XPATH, value = '//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[2]/div[2]/a[2]')
time.sleep(20)
page_selector_2.click()

# %% [markdown]
# ### Getting the table of the second page

# %%
time.sleep(20)
table_element_2 = driver.find_element(by=By.XPATH, value='//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[3]/table')
time.sleep(20)

# %% [markdown]
# ### Getting the inner html of the table

# %%
inner_html_page_2 = table_element_2.get_attribute('innerHTML')

# %%
print(inner_html_page_2)

# %% [markdown]
# ## Getting the third page

# %%
time.sleep(20)
page_selector_3 = driver.find_element(by=By.XPATH, value = '//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[2]/div[2]/a[5]')
time.sleep(20)
page_selector_3.click()

# %% [markdown]
# ### Getting the table of the third page

# %%
time.sleep(20)
table_element_3 = driver.find_element(by=By.XPATH, value='//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[3]/table')
time.sleep(20)

# %% [markdown]
# ### Getting the inner html of the table

# %%
inner_html_page_3 = table_element_3.get_attribute('innerHTML')

# %%
print(inner_html_page_3)

# %% [markdown]
# ## Getting the fourth page

# %%
time.sleep(20)
page_selector_4 = driver.find_element(by=By.XPATH, value = '//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[4]/div[2]/a[6]')
time.sleep(20)
page_selector_4.click()

# %% [markdown]
# ### Getting the table of the fourth page

# %%
time.sleep(20)
table_element_4 = driver.find_element(by=By.XPATH, value='//*[@id="ctl00_ContentPlaceHolder1_divData"]/div[3]/table')
time.sleep(20)

# %% [markdown]
# ### Getting the inner html of the table

# %%
inner_html_page_4 = table_element_4.get_attribute('innerHTML')

# %%
print(inner_html_page_4)

# %% [markdown]
# ## Concating all the inner htmls in one

# %%
# Assuming inner_html_page_1, inner_html_page_2, etc., contain the HTML from each page
inner_htmls = inner_html_page_1 + inner_html_page_2 + inner_html_page_3 + inner_html_page_4

# %% [markdown]
# ## Using BeautifulSoup to get the data from the inner htmls

# %%
from bs4 import BeautifulSoup
import pandas as pd

# parsing the concatenated HTML
soup = BeautifulSoup(inner_htmls, 'html.parser')

# finding all the <tr> tags. Here, <tr> tags represent the individual rows.
rows = soup.find_all('tr')

Sn = []
Symbol = []
LTP = []
Percent_Change = []
High = []
Low = []
Open = []
Qty = []
Turnover = []

# iterating over all the individual rows
for row in rows:
    # finding all the <td> tags within this row. Here, <td> tags represent the individual values/cells of individual rows.
    cells = row.find_all('td')
    if len(cells) == 9:  # ensuring there are exactly 9 columns, as expected.
        Sn.append(cells[0].get_text(strip=True))
        Symbol.append(cells[1].get_text(strip=True))
        LTP.append(cells[2].get_text(strip=True))
        Percent_Change.append(cells[3].get_text(strip=True))
        High.append(cells[4].get_text(strip=True))
        Low.append(cells[5].get_text(strip=True))
        Open.append(cells[6].get_text(strip=True))
        Qty.append(cells[7].get_text(strip=True))
        Turnover.append(cells[8].get_text(strip=True))

# creating a DataFrame from the webscrapped data
df = pd.DataFrame({
    'Sn': Sn,
    'Symbol': Symbol,
    'LTP': LTP,
    'Percent_Change': Percent_Change,
    'High': High,
    'Low': Low,
    'Open': Open,
    'Qty': Qty,
    'Turnover': Turnover
})

df

# %%
# removing commas from the columns and converitng them to float
df['LTP'] = df['LTP'].str.replace(',', '').astype(float)
df['Percent_Change'] = df['Percent_Change'].astype(float)  # assuming there are no commas here
df['High'] = df['High'].str.replace(',', '').astype(float)
df['Low'] = df['Low'].str.replace(',', '').astype(float)
df['Open'] = df['Open'].str.replace(',', '').astype(float)
df['Qty'] = df['Qty'].str.replace(',', '').astype(float)
df['Turnover'] = df['Turnover'].str.replace(',', '').astype(float)

# %%
df.info()

# %%
df

# %% [markdown]
# # Database Connection

# %%
import pyodbc


# ## setting up connection to MSSQL

server = 'localhost'  
database = 'master' 
username = 'sa'  
password = 'Ocacine10#'  

# %%
pyodbc.drivers()

# %%
# creating connection with autocommit set to True
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      f'SERVER={server};'
                      f'DATABASE={database};'
                      f'UID={username};'
                      f'PWD={password}',
                      autocommit=True)  # enabling autocommit

cursor = cnxn.cursor()

# %%
# query to create the database
cursor.execute('''
CREATE DATABASE merolagani;
''')

# %%
# query to use the created database
cursor.execute('''
USE merolagani;
''')

# %%
# query to create a table in the database
cursor.execute('''
CREATE TABLE merolagani (
    SN INT IDENTITY(1,1), 
    Symbol VARCHAR(255), 
    LTP REAL, 
    Percent_Change REAL,
    [High] REAL,
    [Low] REAL,
    [Open] REAL,
    Qty REAL,
    Turnover REAL,
    PRIMARY KEY (Symbol)
)
''')

# %%
# query to remove the primary key constraint for now
cursor.execute(f'''
ALTER TABLE merolagani
DROP CONSTRAINT [PK__merolaga__B7CC3F004B948AF6];
''')

# %%
# preparing the data to insert in the database
data_to_insert = df.copy() 

# %%
# using for loop to insert individual data from each row to the MSSQL
for _, row in data_to_insert.iterrows():
    insert_query = '''
        INSERT INTO merolagani (Symbol, LTP, Percent_Change, [High], [Low], [Open], Qty, Turnover)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''
    params = (row['Symbol'], row['LTP'], row['Percent_Change'], row['High'], row['Low'], row['Open'], row['Qty'], row['Turnover'])
    cursor.execute(insert_query, params)

# %%
cursor.execute('''
SELECT *
FROM merolagani
''')

# %%
records = cursor.fetchall()

# %%
records

# %%
type(records)

# %%
records_as_tuples = [tuple(row) for row in records]

# %%
type(records_as_tuples)

# %% [markdown]
# ## Displaying the data from the SQL query from the MSSQL

# %%
df = pd.DataFrame(records_as_tuples, columns = ['SN','Symbol', 'LTP', 'Percent_Change', 'High', 'Low', 'Open', 'Qty', 'Turnover'])
df


