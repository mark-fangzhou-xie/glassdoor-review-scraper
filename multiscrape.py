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
import glob
import os
import re
import sqlite3 as lite
import subprocess
import time

import pandas as pd

base_url = 'https://www.glassdoor.com/Overview/Working-at-{}-EI_IE{}.htm'
# ?sort.sortType=RD&sort.ascending=false

if not os.path.exists("data"):
    os.makedirs('data')

conn = lite.connect('education_sector.sqlite')
# c = conn.cursor()
# language=SQL
links = pd.read_sql_query('SELECT * from education group by review_link;', conn)
# extract glassdoor id from review link
links['review_link_sorted'] = links['review_link'] + '?sort.sortType=RD&sort.ascending=false'
links['glassdoor_id'] = [re.search(r'(?<=E)[0-9]+(?=.htm)', i).group(0) for i in links['review_link']]
# links['nm'] = [s.translate(str.maketrans('', '', string.punctuation)) for s in links['company_name']]
# # get overview link
# links['overview_link'] = [base_url.format(row['nm'].replace(' ', '-'), row['glassdoor_id'])
#                           for idx, row in links.iterrows()]

# page_df = pd.read_csv("data/scrape-glassdoor-urls.csv")
files = glob.glob("data/*[0-9].*csv")
# pdb.set_trace()
already_scraped_ids = [file.replace("data/", "").replace(".csv", "") for file in files]

# subset_page_df = page_df[~page_df["gvkey"].isin(already_scraped_keys)]
subset_page_df = links[~links["glassdoor_id"].isin(already_scraped_ids)][
    (links['industry'] == 'K-12 Education') | (links['industry'] == 'Colleges & Universities')]
# subset_page_df = links

# pdb.set_trace()

for index, row in subset_page_df.iterrows():
    # TODO: url needs to be sorted review links
    command = 'python main.py --headless --limit 999999 --search_type reviews --min_date 2019-09-01 ' + \
              '--glassdoor_id ' + row['glassdoor_id'] + \
              ' --start_from_url --url "' + \
              row["review_link_sorted"] + '" -f data/' + str(row["glassdoor_id"]) + '.csv'
    print(command)
    subprocess.run(command, shell=True)
    # subprocess.run('killall chrome', shell=True)
    time.sleep(60 * 2)
