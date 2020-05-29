'''
multiscrape.py
----------
Jeremiah Haremza
May 9, 2018

Given a tuple of items use main.py to scrape reviews from multiple companies on Glassdoor.
Use 1 tuple per company.
List of tuples to itterate over for each command execution is named pages
each tuple in the list takes the format (url, limit, output_file_name)
each Item in the tuple is a string, hence it will need to be enclosed in quotes.
'''

import os
import pandas as pd
import time
import glob
page_df = pd.read_csv("data/scrape-glassdoor-urls.csv")
files = glob.glob("data/*[0-9].*csv")
already_scraped_keys = [int(file.replace("data/", "").replace(".csv", "")) for file in files]

subset_page_df = page_df[~page_df["gvkey"].isin(already_scraped_keys)]

for index, row in subset_page_df.iterrows():
    command = 'python main.py --headless --credentials data/secret.json --limit 999999 --min_date 2019-09-01 --start_from_url  --url "' + row["webpage_to_scrape_reviews"] + '" -f data/' + str(row["gvkey"]) + '.csv'
    print(command)
    os.system(command)
    time.sleep(60*5)
