'''
main.py
----------
Matthew Chatham
June 6, 2018

Given a company's landing page on Glassdoor and an output filename, scrape the
following information about each employee review:

Review date
Employee position
Employee location
Employee status (current/former)
Review title
Employee years at company
Number of helpful votes
Pros text
Cons text
Advice to mgmttext
Ratings for each of 5 categories
Overall rating
'''
# import argparse
import datetime as dt
import json
# import logging
import logging.config
import os
import re
import sqlite3 as lite
import time
import traceback
import urllib
from argparse import ArgumentParser
from random import uniform

import numpy as np
import pandas as pd
import selenium
from selenium import webdriver as wd
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from schema import SCHEMA

start = time.time()

DEFAULT_URL = ('https://www.glassdoor.com/Overview/Working-at-'
               'Premise-Data-Corporation-EI_IE952471.11,35.htm')

parser = ArgumentParser()
parser.add_argument('-u', '--url',
                    help='URL of the company\'s Glassdoor landing page.',
                    default=DEFAULT_URL)
parser.add_argument('-f', '--file', default='glassdoor_ratings.csv',
                    help='Output file.')
parser.add_argument('--headless', action='store_true',
                    help='Run Chrome in headless mode.')
parser.add_argument('--username', help='Email address used to sign in to GD.')
parser.add_argument('-p', '--password', help='Password to sign in to GD.')
parser.add_argument('-c', '--credentials', help='Credentials file')
parser.add_argument('-l', '--limit', default=25,
                    action='store', type=int, help='Max reviews to scrape')
parser.add_argument('--start_from_url', action='store_true',
                    help='Start scraping from the passed URL.')
parser.add_argument(
    '--max_date', help='Latest review date to scrape.\
    Only use this option with --start_from_url.\
    You also must have sorted Glassdoor reviews ASCENDING by date.',
    type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d"))
parser.add_argument(
    '--min_date', help='Earliest review date to scrape.\
    Only use this option with --start_from_url.\
    You also must have sorted Glassdoor reviews DESCENDING by date.',
    type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d"))
parser.add_argument(
    '--search_type', help='Whether to scrape reviews or search for companies.\
    Accepts arguments: companies or reviews.\
    --max_date and --min_date are ignored if companies selected.',
    choices=['reviews', 'companies'],
    default='companies'
)
parser.add_argument(
    '--search_company_names', help='Company names to search.',
    nargs='+'
)
parser.add_argument(
    '--loop_education',
    help='Loop over all companies in education sector.',
    default=False,
    nargs='+'
)
parser.add_argument(
    '--glassdoor_id',
    help='Glassdoor company id'
)
args = parser.parse_args()

if not args.start_from_url and (args.max_date or args.min_date):
    raise Exception(
        'Invalid argument combination:\
        No starting url passed, but max/min date specified.'
    )
elif args.max_date and args.min_date:
    raise Exception(
        'Invalid argument combination:\
        Both min_date and max_date specified.'
    )

if args.credentials:
    with open(args.credentials) as f:
        d = json.loads(f.read())
        args.username = d['username']
        args.password = d['password']
else:
    try:
        with open('secret.json') as f:
            d = json.loads(f.read())
            args.username = d['username']
            args.password = d['password']
    except FileNotFoundError:
        msg = 'Please provide Glassdoor credentials.\
        Credentials can be provided as a secret.json file in the working\
        directory, or passed at the command line using the --username and\
        --password flags.'
        raise Exception(msg)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s %(lineno)d\
    :%(filename)s(%(process)d) - %(message)s')
ch.setFormatter(formatter)

logging.getLogger('selenium').setLevel(logging.CRITICAL)
logging.getLogger('selenium').setLevel(logging.CRITICAL)


def scrape(field, review, author):
    def scrape_date(review):
        return review.find_element_by_tag_name(
            'time').get_attribute('datetime')

    def scrape_emp_title(review):
        if 'Anonymous Employee' not in review.text:
            try:
                res = author.find_element_by_class_name(
                    'authorJobTitle').text.split('-')[1]
            except Exception:
                logger.warning('Failed to scrape employee_title')
                res = np.nan
        else:
            res = np.nan
        return res

    def scrape_location(review):
        if 'in' in review.text:
            try:
                res = author.find_element_by_class_name(
                    'authorLocation').text
            except Exception:
                res = np.nan
        else:
            res = np.nan
        return res

    def scrape_status(review):
        try:
            res = author.text.split('-')[0]
        except Exception:
            logger.warning('Failed to scrape employee_status')
            res = np.nan
        return res

    def scrape_rev_title(review):
        return review.find_element_by_class_name('summary').text.strip('"')

    def scrape_years(review):
        res = review.find_element_by_class_name('mainText').text.strip('"')
        return res

    def scrape_helpful(review):
        try:
            helpful = review.find_element_by_class_name(
                'helpfulCount').text.replace('"', '')
            res = helpful[helpful.find('(') + 1: -1]
        except Exception:
            res = 0
        return res

    def expand_show_more(section):
        try:
            # more_content = section.find_element_by_class_name('moreContent')
            more_link = section.find_element_by_class_name(
                'v2__EIReviewDetailsV2__continueReading')
            more_link.click()
        except Exception:
            pass

    def scrape_pros(review):
        try:
            pros = review.find_element_by_class_name(
                'v2__EIReviewDetailsV2__fullWidth')
            expand_show_more(pros)
            res = pros.text.replace('Pros', '')
            res = res.strip()
        except Exception:
            res = np.nan
        return res

    def scrape_cons(review):
        try:
            cons = review.find_elements_by_class_name(
                'v2__EIReviewDetailsV2__fullWidth')[1]
            expand_show_more(cons)
            res = cons.text.replace('Cons', '')
            res = res.strip()
        except Exception:
            res = np.nan
        return res

    def scrape_advice(review):
        try:
            advice = review.find_elements_by_class_name(
                'v2__EIReviewDetailsV2__fullWidth')[2]
            res = advice.text.replace('Advice to Management', '')
            res = res.strip()
        except Exception:
            res = np.nan
        return res

    def scrape_overall_rating(review):
        try:
            ratings = review.find_element_by_class_name('gdStars')
            overall = ratings.find_element_by_class_name(
                'rating').find_element_by_class_name('value-title')
            res = overall.get_attribute('title')
        except Exception:
            res = np.nan
        return res

    def _scrape_subrating(i):
        try:
            ratings = review.find_element_by_class_name('gdStars')
            subratings = ratings.find_element_by_class_name(
                'subRatings').find_element_by_tag_name('ul')
            this_one = subratings.find_elements_by_tag_name('li')[i]
            res = this_one.find_element_by_class_name(
                'gdBars').get_attribute('title')
        except Exception:
            res = np.nan
        return res

    def scrape_work_life_balance(review):
        return _scrape_subrating(0)

    def scrape_culture_and_values(review):
        return _scrape_subrating(1)

    def scrape_career_opportunities(review):
        return _scrape_subrating(2)

    def scrape_comp_and_benefits(review):
        return _scrape_subrating(3)

    def scrape_senior_management(review):
        return _scrape_subrating(4)

    def scrape_recommends(review):
        try:
            res = review.find_element_by_class_name('recommends').text
            res = res.split('\n')
            return res[0]
        except:
            return np.nan

    def scrape_outlook(review):
        try:
            res = review.find_element_by_class_name('recommends').text
            res = res.split('\n')
            if len(res) == 2 or len(res) == 3:
                if 'CEO' in res[1]:
                    return np.nan
                return res[1]
            return np.nan
        except:
            return np.nan

    def scrape_approve_ceo(review):
        try:
            res = review.find_element_by_class_name('recommends').text
            res = res.split('\n')
            if len(res) == 3:
                return res[2]
            if len(res) == 2:
                if 'CEO' in res[1]:
                    return res[1]
            return np.nan
        except:
            return np.nan

    funcs = [
        scrape_date,
        scrape_emp_title,
        scrape_location,
        scrape_status,
        scrape_rev_title,
        scrape_years,
        scrape_helpful,
        scrape_pros,
        scrape_cons,
        scrape_advice,
        scrape_overall_rating,
        scrape_work_life_balance,
        scrape_culture_and_values,
        scrape_career_opportunities,
        scrape_comp_and_benefits,
        scrape_senior_management,
        scrape_recommends,
        scrape_outlook,
        scrape_approve_ceo

    ]

    fdict = dict((s, f) for (s, f) in zip(SCHEMA, funcs))

    return fdict[field](review)


def extract_from_page():
    def is_featured(review):
        try:
            review.find_element_by_class_name('featuredFlag')
            return True
        except selenium.common.exceptions.NoSuchElementException:
            return False

    def extract_review(review):
        author = review.find_element_by_class_name('authorInfo')
        res = {}
        # import pdb;pdb.set_trace()
        for field in SCHEMA:
            res[field] = scrape(field, review, author)

        assert set(res.keys()) == set(SCHEMA)
        return res

    logger.info(f'Extracting reviews from page {page[0]}')

    res = pd.DataFrame([], columns=SCHEMA)

    reviews = browser.find_elements_by_class_name('empReview')
    logger.info(f'Found {len(reviews)} reviews on page {page[0]}')

    for review in reviews:
        if not is_featured(review):
            data = extract_review(review)
            logger.info(f'Scraped data for "{data["review_title"]}"\
({data["date"]})')
            res.loc[idx[0]] = data
        else:
            logger.info('Discarding a featured review')
        idx[0] = idx[0] + 1

    if args.max_date and \
            (pd.to_datetime(res['date'].str.replace(r'\d{2}:\d{2}:\d{2}.*$', '')).max() > args.max_date) or \
            args.min_date and \
            (pd.to_datetime(res['date'].str.replace(r'\d{2}:\d{2}:\d{2}.*$', '')).min() < args.min_date):
        logger.info('Date limit reached, ending process')
        date_limit_reached[0] = True
    # Ending if # reviews on this page is 0
    if len(reviews) == 0:
        logger.info('No reviews found, ending process')
        date_limit_reached[0] = True

    return res


def more_pages():
    try:
        # paging_control = browser.find_element_by_class_name('pagingControls')
        next_ = browser.find_element_by_class_name(
            'pagination__PaginationStyle__next')
        next_.find_element_by_tag_name('a')
        return True
    except selenium.common.exceptions.NoSuchElementException:
        return False


def go_to_next_page():
    logger.info(f'Going to page {page[0] + 1}')
    # paging_control = browser.find_element_by_class_name('pagingControls')
    next_ = browser.find_element_by_class_name(
        'pagination__PaginationStyle__next').find_element_by_tag_name('a')
    browser.get(next_.get_attribute('href'))
    time.sleep(1)
    page[0] = page[0] + 1


def no_reviews():
    return False
    # TODO: Find a company with no reviews to test on


def navigate_to_reviews():
    logger.info('Navigating to company reviews')

    browser.get(args.url)
    time.sleep(1)

    if no_reviews():
        logger.info('No reviews to scrape. Bailing!')
        return False

    reviews_cell = browser.find_element_by_xpath(
        '//a[@data-label="Reviews"]')
    reviews_path = reviews_cell.get_attribute('href')

    # reviews_path = driver.current_url.replace('Overview','Reviews')
    browser.get(reviews_path)
    time.sleep(1)
    return True


def scrape_company_url(listing):
    try:
        company_url = listing.find_element_by_class_name(
            'tightAll').get_attribute('href')
        return company_url
    except:
        return np.nan


def scrape_company_name(listing):
    try:
        company_name = listing.find_element_by_class_name('tightAll').text
        return company_name
    except:
        return np.nan


def scrape_company_webpage(listing):
    try:
        company_webpage = listing.find_element_by_class_name(
            'url').get_attribute('innerHTML')
        return company_webpage
    except:
        return np.nan


def scrape_company_HQ(listing):
    try:
        company_HQ = listing.find_element_by_class_name(
            'hqInfo.adr').find_elements_by_tag_name("span")[0].get_attribute("innerHTML")
        return company_HQ
    except:
        return np.nan


def scrape_n_total_reviews(listing):
    try:
        n_reviews = listing.find_element_by_class_name(
            "eiCell.cell.reviews").find_element_by_class_name("num").text
        return n_reviews
    except:
        np.nan


def search_for_company(company_name_to_search):
    logger.info(f'Searching for company: {company_name_to_search}')
    search_box = browser.find_element_by_css_selector(r"#sc\.keyword")
    type_box = browser.find_element_by_xpath(
        '/html/body/header/div[2]/div[2]/form/div/ul/li[2]')
    location_box = browser.find_element_by_css_selector(r'#sc\.location')
    search_button = browser.find_element_by_css_selector('#HeroSearchButton')

    search_box.clear()
    search_box.send_keys(company_name_to_search)
    location_box.send_keys("")
    type_box.click()
    search_button.click()

    # import pdb;pdb.set_trace()

    company_data = pd.DataFrame([], columns=["company_name_to_search",
                                             "company_url",
                                             "company_name",
                                             "company_webpage",
                                             "company_HQ",
                                             "search_rank",
                                             "n_reviews"])

    # Checking if we've been re-directed to a company page
    pattern = re.compile("reviews")
    if pattern.search(browser.current_url):
        company_listings = browser.find_elements_by_css_selector(
            'div.eiHdrModule')
        for listing in company_listings[0:args.limit]:
            company_url = scrape_company_url(listing)
            company_name = scrape_company_name(listing)
            company_webpage = scrape_company_webpage(listing)
            company_HQ = scrape_company_HQ(listing)
            n_reviews = scrape_n_total_reviews(listing)
            company_data.loc[idx[0]] = [
                company_name_to_search,
                company_url,
                company_name,
                company_webpage,
                company_HQ,
                idx[0],
                n_reviews
            ]
            idx[0] = idx[0] + 1
    else:
        # These aren't so robust
        company_url = browser.find_element_by_css_selector(
            '#EmpHero').get_attribute("data-employer-id")
        company_name = browser.find_element_by_css_selector(
            '.header').find_element_by_tag_name('h1').text
        company_webpage = browser.find_element_by_css_selector('.website').text
        company_HQ = browser.find_element_by_css_selector(
            "div.infoEntity:nth-child(2) > span:nth-child(2)").text
        n_reviews = scrape_n_total_reviews(browser)
        company_data.loc[idx[0]] = [
            company_name_to_search,
            company_url,
            company_name,
            company_webpage,
            company_HQ,
            idx[0],
            n_reviews
        ]
    return company_data


def is_exists(review_link, dbname='education_sector.sqlite'):
    if not os.path.exists(dbname):
        return False
    conn = lite.connect(dbname)
    c = conn.cursor()
    res = c.execute("""SELECT * FROM `education` WHERE review_link = ?;""",
                    (review_link,)).fetchone()
    if len(res) != 0:
        return True
    else:
        return False


def write_scraped_to_db(args, dbname='education_sector.sqlite'):
    conn = lite.connect(dbname)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS `education` (
    icon_link TEXT,
    review_link TEXT,
    company_name TEXT,
    ratings TEXT,
    reviews TEXT,
    salaries TEXT,
    jobs TEXT,
    location TEXT,
    company_size TEXT,
    industry TEXT,
    description TEXT,
    PRIMARY KEY (icon_link, company_name)
    -- page INT -- record page number just in case it fails in the middle
    );""")
    conn.commit()

    c.executemany("""INSERT OR IGNORE INTO `education`
    VALUES (?,?,?,?,?,?,?,?,?,?,?);""", args)
    conn.commit()


def loop_education():
    "loop over the whole education sector and get their GD id"

    state_names = [
        "Alaska",
        "Alabama",
        "Arkansas",
        "Arizona",
        "California",
        "Colorado",
        "Connecticut",
        "District of Columbia",
        "Delaware",
        "Florida",
        "Georgia",
        "Hawaii",
        "Iowa",
        "Idaho",
        "Illinois",
        "Indiana",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Massachusetts",
        "Maryland",
        "Maine",
        "Michigan",
        "Minnesota",
        "Missouri",
        "Mississippi",
        "Montana",
        "North Carolina",
        "North Dakota",
        "Nebraska",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "Nevada",
        "New York",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",  # "Puerto Rico",
        "Rhode Island",
        "South Carolina",
        "South Dakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Virginia",  # "Virgin Islands",
        "Vermont",
        "Washington",
        "Wisconsin",
        "West Virginia",
        "Wyoming"]

    def waiting_element(browser, xpath, multiple=False):
        "wraps waiting piece"
        ignored_exceptions = (NoSuchElementException,
                              StaleElementReferenceException,)
        if multiple:
            element = WebDriverWait(browser, 10, ignored_exceptions=ignored_exceptions).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, xpath)
                )
            )
        else:
            element = WebDriverWait(browser, 10, ignored_exceptions=ignored_exceptions).until(
                EC.presence_of_element_located(
                    (By.XPATH, xpath)
                )
            )
        return element

    def scrape_item(listing, xpath, attr=None):
        "generic function on getting item within a listing"
        try:
            if attr:
                item = listing.find_element_by_xpath(xpath).get_attribute(attr)
            else:
                item = listing.find_element_by_xpath(xpath).text
            return item
        except NoSuchElementException or StaleElementReferenceException:
            # traceback.print_exc(e)
            return None

    def scrape_listing(listing):
        icon_link = scrape_item(listing,
                                ".//img[@data-test='employer-logo']",
                                attr='src')
        company_name = scrape_item(listing,
                                   ".//h2[@data-test='employer-short-name']")
        ratings = scrape_item(listing, ".//span[@data-test='rating']")
        # pdb.set_trace()
        review = listing.find_element_by_xpath(
            ".//div[@data-test='cell-Reviews']")
        reviews = scrape_item(
            review, ".//div[@data-test='cell-Reviews-count']")
        review_link = scrape_item(
            review, ".//a[@data-test='cell-Reviews-url']", attr='href')
        # pdb.set_trace()
        salaries = scrape_item(listing,
                               ".//div[@data-test='cell-Salaries-count']")
        jobs = scrape_item(listing, ".//div[@data-test='cell-Jobs-count']")
        location = scrape_item(listing,
                               ".//span[@data-test='employer-location']")
        company_size = scrape_item(listing,
                                   ".//span[@data-test='employer-size']")
        industry = scrape_item(listing,
                               ".//span[@data-test='employer-industry']")
        description = scrape_item(listing,
                                  ".//p[@class='employerCard__EmployerCardStyles__clamp common__commonStyles__subtleText']")
        # page
        # logger.info(company_name)
        arg = (icon_link, review_link, company_name,
               ratings, reviews, salaries,
               jobs, location, company_size,
               industry, description)
        return arg

    def get_webpage(browser, state):
        logger.info(state)
        browser.refresh()
        location_input = waiting_element(browser,
                                         '//*/input[@name="Location"]')
        # pdb.set_trace()
        time.sleep(1)
        location_input.send_keys(Keys.CONTROL + "a")
        location_input.send_keys(Keys.DELETE)
        time.sleep(1)
        location_input.send_keys(state)
        time.sleep(1)
        location_input.send_keys(Keys.DOWN)
        time.sleep(1)
        location_input.send_keys(Keys.ENTER)
        time.sleep(1)

        # waiting_element(browser,
        #                 "//div[@class='radioButtonBox']",
        #                 multiple=True)[companysizeind].click()
        # time.sleep(3)

    logger.info('getting education')
    # loop over education sector
    browser.get(
        'https://www.glassdoor.com/Explore/browse-companies.htm'
        '?overall_rating_low=0&page=1&isHiringSurge=0&sgoc=1006'
    )
    # browser.implicitly_wait(2)
    # WebDriverWait(browser, 10, ignored_exceptions=ignored_exceptions).until(
    #     EC.presence_of_element_located(
    #         (By.XPATH,
    #          "//*[contains(@class, 'col-md-8')]//div[@data-test='cell-Reviews-count']")
    #     )
    # )

    time.sleep(2)
    # break_signal = False

    # page = 1
    # add for loop over company size

    for state in state_names:
        # logger.info(state)
        # browser.refresh()
        # location_input = waiting_element(browser,
        #                                  '//*/input[@name="Location"]')
        # # pdb.set_trace()
        # time.sleep(1)
        # location_input.send_keys(Keys.CONTROL + "a")
        # location_input.send_keys(Keys.DELETE)
        # time.sleep(1)
        # location_input.send_keys(state)
        # time.sleep(1)
        # location_input.send_keys(Keys.DOWN)
        # time.sleep(1)
        # location_input.send_keys(Keys.ENTER)
        # time.sleep(1)

        # radio_buttons = browser.find_elements_by_xpath(
        #     "//div[@class='radioButtonBox']")
        # pdb.set_trace()
        # radio_buttons[i].click()
        # WebDriverWait(browser, 10, ignored_exceptions=ignored_exceptions).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH,
        #          "//*[contains(@class, 'col-md-8')]//div[@data-test='cell-Reviews-count']")
        #     )
        # )
        # waiting_element(browser,
        #                 "//*[contains(@class, 'col-md-8')]//div[@data-test='cell-Reviews-count']")
        # time.sleep(3)
        # browser.implicitly_wait(3)
        get_webpage(browser, state)

        for i in range(6, -1, -1):
            # if i == 5:
            #     pdb.set_trace()

            try:
                waiting_element(browser,
                                "//div[@class='radioButtonBox']",
                                multiple=True)[i].click()
                time.sleep(3)
                waiting_element(browser,
                                "//*[contains(@class, 'col-md-8')]//div[@data-test='cell-Reviews-count']")
            except TimeoutException:
                browser.refresh()
                get_webpage(browser, state)
                waiting_element(browser,
                                "//div[@class='radioButtonBox']",
                                multiple=True)[i].click()
                time.sleep(3)
            time.sleep(3)

            while True:
                # page_range = (browser
                #               .find_element_by_xpath("//*[contains(@class, 'resultCount')]")
                #               .find_elements_by_xpath('.//strong'))
                # pages = [i.text for i in page_range]

                try:
                    # page_range = (browser
                    #               .find_element_by_xpath("//*[contains(@class, 'resultCount')]")
                    #               .find_elements_by_xpath('.//strong'))
                    pages = [i.text for i in (waiting_element(browser,
                                                              "//*[contains(@class, 'resultCount')]")
                                              .find_elements_by_xpath('.//strong'))]
                except TimeoutException:
                    break
                    browser.refresh()
                    get_webpage(browser, state)
                    waiting_element(browser,
                                    "//div[@class='radioButtonBox']",
                                    multiple=True)[i].click()
                    time.sleep(3)

                    # WebDriverWait(browser, 10, ignored_exceptions=ignored_exceptions).until(
                    #     EC.presence_of_element_located(
                    #         (By.XPATH,
                    #          "//*[contains(@class, 'col-md-8')]//div[@data-test='cell-Reviews-count']")
                    #     )
                    # )
                    # time.sleep(1)
                    pages = [i.text for i in (waiting_element(browser,
                                                              "//*[contains(@class, 'resultCount')]")
                                              .find_elements_by_xpath('.//strong'))]
                # finally:
                #     pages = [i.text for i in page_range]

                # add looping pages feature
                try:
                    listings = waiting_element(
                        browser, "//*[@class='row d-flex flex-wrap']",
                        multiple=True)
                except TimeoutException:
                    browser.refresh()
                    get_webpage(browser, state)
                    waiting_element(browser,
                                    "//div[@class='radioButtonBox']",
                                    multiple=True)[i].click()
                    time.sleep(3)
                    listings = waiting_element(
                        browser, "//*[@class='row d-flex flex-wrap']",
                        multiple=True)
                # listings = browser.find_elements_by_xpath(
                #     "//*[@class='row d-flex flex-wrap']")

                # time.sleep(1)
                # browser.implicitly_wait(1)
                args = []
                for listing in listings:
                    # pdb.set_trace()
                    try:
                        arg = scrape_listing(listing)
                        args.append(arg)
                    except StaleElementReferenceException:
                        # browser.refresh()
                        # get_webpage(browser, state)
                        # waiting_element(browser,
                        #                 "//div[@class='radioButtonBox']",
                        #                 multiple=True)[i].click()
                        # time.sleep(3)
                        pass
                    # if is_exists(arg[1]):
                    #     break
                    # logger.info((company_name, ratings, industry))
                    # if not all(v is None for v in arg):

                # pdb.set_trace()

                write_scraped_to_db(args)

                # pdb.set_trace()

                if len(pages) == 1 or pages[0] == '0' or pages[1] == pages[2]:
                    logger.info('reaching end, breaking')
                    break
                else:
                    logger.info(('chunk', i, pages[0], pages[1], pages[2]))

                browser.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                next_page_button = waiting_element(browser,
                                                   "//button[@aria-label='Next']")
                # browser.find_element_by_xpath(
                #     "//button[@aria-label='Next']")
                next_page_button.click()
                # browser.implicitly_wait(2)
                # time.sleep(uniform(1, 2))
                # WebDriverWait(browser, 10, ignored_exceptions=ignored_exceptions).until(
                #     EC.presence_of_element_located(
                #         (By.XPATH,
                #          "//*[contains(@class, 'col-md-8')]//div[@data-test='cell-Reviews-count']")
                #     )
                # )
                time.sleep(3)
                time.sleep(uniform(0.1, 4))


def get_basic_info(glassdoor_id):
    def write_to_db(arg, dbname='education_sector.sqlite'):
        conn = lite.connect(dbname)
        c = conn.cursor()
        # language=SQL
        c.execute("""CREATE TABLE IF NOT EXISTS company_info (
        glassdoor_id INT,
        overview_link TEXT,
        CEO TEXT,
        website TEXT,
        headquarters TEXT,
        size TEXT,
        founded TEXT,
        type TEXT,
        industry TEXT,
        revenue TEXT,
        primary key (glassdoor_id));""")
        # language=SQL
        c.execute("""INSERT OR IGNORE INTO company_info VALUES (?,?,?,?,?,?,?,?,?,?);""", arg)
        conn.commit()
        conn.close()

    # TODO: get basic information of companies
    browser.get(args.url)
    time.sleep(1)
    # TODO: get CEO name
    ceo_name_box = browser.find_element_by_xpath("//*[contains(@class, 'pl-lg-sm')]")
    time.sleep(1)
    try:
        ceo_name = ceo_name_box.find_element_by_xpath(".//div").text
    except:
        ceo_name = None
    # pdb.set_trace()
    overview_icon = browser.find_element_by_xpath("//*[contains(@class, 'overviews')]")
    time.sleep(1)
    overview_link = overview_icon.get_attribute('href')
    browser.get(overview_link)
    time.sleep(3)
    # pdb.set_trace()
    basic_info = browser.find_element_by_xpath("//*/div[@class='info flexbox row col-hh']")
    # entity_dict = {'glassdoor_id': glassdoor_id}
    # entity_dict = {}
    entities = basic_info.find_elements_by_xpath('.//div[@class="infoEntity"]')
    # TODO: get company information correctly
    entity_dict = {e.find_element_by_xpath('.//label').text: e.find_element_by_xpath('.//span').text
                   for e in entities}
    # for entity in entities:
    #     if entity['class'] == 'value website':
    #         entity_dict['Website'] = entity.find_element_by_xpath('.//span').text
    #     else:
    #         key = entity.find_element_by_xpath('.//label').text
    #         value = entity.find_element_by_xpath('.//span[class="value"]')
    #         entity_dict[key] = value
    # return entity_dict
    arg = (glassdoor_id,
           overview_link,
           ceo_name,
           entity_dict.get('Website'),
           entity_dict.get('Headquarters'),
           entity_dict.get('Size'),
           entity_dict.get('Founded'),
           entity_dict.get('Type'),
           entity_dict.get('Industry'),
           entity_dict.get('Revenue')
           )
    write_to_db(arg)


def sign_in():
    logger.info(f'Signing in to {args.username}')

    url = 'https://www.glassdoor.com/profile/login_input.htm'
    browser.get(url)

    # import pdb;pdb.set_trace()

    email_field = browser.find_element_by_name('username')
    password_field = browser.find_element_by_name('password')
    submit_btn = browser.find_element_by_xpath('//button[@type="submit"]')

    email_field.send_keys(args.username)
    password_field.send_keys(args.password)
    submit_btn.send_keys("\n")

    time.sleep(3)
    # browser.get(args.url)


def get_browser():
    logger.info('Configuring browser')
    logger.info(f'Scraping {args.url}')
    chrome_options = wd.ChromeOptions()
    if args.headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('log-level=3')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    browser = wd.Chrome(options=chrome_options)
    return browser


def get_current_page():
    logger.info('Getting current page number')
    try:
        page_number_elements = browser.find_element_by_css_selector(
            ".eiReviews__EIReviewsPageStyles__pagination").find_elements_by_css_selector(
            "li.pagination__PaginationStyle__page")

        for element in page_number_elements:
            if element.get_attribute(
                    'class') == "pagination__PaginationStyle__page pagination__PaginationStyle__current":
                current = int(element.text)
    except selenium.common.exceptions.NoSuchElementException:
        current = 1  # only one page if page numbers at bottom of page
    return current


def verify_date_sorting():
    # import pdb
    # pdb.set_trace()
    logger.info('Date limit specified, verifying date sorting')
    ascending = urllib.parse.parse_qs(args.url)['sort.ascending'] == ['true']

    if args.min_date and ascending:
        raise Exception(
            'min_date required reviews to be sorted DESCENDING by date.')
    elif args.max_date and not ascending:
        raise Exception(
            'max_date requires reviews to be sorted ASCENDING by date.')


def main():
    logger.info(f'Scraping up to {args.limit} companies/reviews.')

    sign_in()
    if args.search_type == "companies":
        if args.loop_education:
            # loop education sector here
            loop_education()
        else:
            res = pd.DataFrame([], columns=["company_name_to_search",
                                            "company_url",
                                            "company_name",
                                            "company_webpage",
                                            "company_HQ",
                                            "search_rank",
                                            "n_reviews"])
            res.to_csv(args.file, index=False, encoding='utf-8')
            for company_to_search in args.search_company_names:
                try:
                    companies_df = search_for_company(company_to_search)
                except:
                    companies_df = res
                    companies_df.loc[idx[0]] = [
                        company_to_search,
                        np.nan,
                        np.nan,
                        np.nan,
                        np.nan,
                        np.nan,
                        np.nan
                    ]

                companies_df.to_csv(args.file, index=False,
                                    encoding='utf-8', mode='a', header=False)
                time.sleep(30)

    else:
        res = pd.DataFrame([], columns=SCHEMA)

        if args.start_from_url:
            get_basic_info(args.glassdoor_id)

        # pdb.set_trace()
        if not args.start_from_url:
            reviews_exist = navigate_to_reviews()
            if not reviews_exist:
                return
        elif args.max_date or args.min_date:
            verify_date_sorting()
            browser.get(args.url)
            page[0] = get_current_page()
            logger.info(f'Starting from page {page[0]:,}.')
            time.sleep(1)
        else:
            browser.get(args.url)
            page[0] = get_current_page()
            logger.info(f'Starting from page {page[0]:,}.')
            time.sleep(1)

        reviews_df = extract_from_page()
        res = res.append(reviews_df)

        # import pdb;pdb.set_trace()

        while more_pages() and \
                len(res) + 1 < args.limit and \
                not date_limit_reached[0]:
            go_to_next_page()
            reviews_df = extract_from_page()
            res = res.append(reviews_df)
        logger.info(f'Writing {len(res)} reviews to file {args.file}')
        res.to_csv(args.file, index=False, encoding='utf-8')

    end = time.time()
    browser.quit()
    logger.info(f'Finished in {end - start} seconds')


if __name__ == '__main__':
    # browser = get_browser()
    # main()
    try:
        browser = get_browser()
        page = [1]
        idx = [0]
        date_limit_reached = [False]
        main()
    except Exception as e:
        traceback.print_exc(e)
    finally:
        browser.quit()
