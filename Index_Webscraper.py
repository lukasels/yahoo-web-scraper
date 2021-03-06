import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from datetime import datetime
import numpy as np
from time import sleep
import multiprocessing as mp


## extract the index companies

# scrape for the companies
def scrape_index_companies(index_link):
    driver = webdriver.Chrome(executable_path='chromedriver_path')
    driver.get(index_link)
    html = driver.execute_script('return document.body.innerHTML;')
    soup = BeautifulSoup(html, 'lxml')
    requested_features = soup.find_all('tr', class_='Ta(end)')
    driver.quit()
    return requested_features


# create a data frame with the companies
def create_companies_df(requested_features):
    headers = []
    temp_list = []
    final = []
    index = 0
    # create headers
    for item in requested_features[0].find_all('th', class_='Ta(start)'):
        headers.append(item.text)
    # statement contents
    while index <= len(requested_features) - 1:
        # filter for each line of the statement
        temp = requested_features[index].find_all('td', class_='Ta(start)')
        for line in temp:
            # each item adding to a temporary list
            temp_list.append(line.text)
        # temp_list added to final list
        final.append(temp_list)
        # clear temp_list
        temp_list = []
        index += 1
    index_companies_df = pd.DataFrame(final[1:])
    index_companies_df.columns = headers
    return index_companies_df


## scrape the single companies in the index

# driver setup for the single company scrape
def driver_setup(single_company_link):
    error = True

    options = webdriver.ChromeOptions()
    options.add_argument('incognito')
    options.add_argument('--window-size=800,600')
    options.add_argument('disable-popup-blocking')
    driver = webdriver.Chrome(options=options, executable_path='chromedriver_path')
    driver.create_options()
    driver.get(single_company_link)
    while error is True:
        html = driver.execute_script('return document.body.innerHTML;')
        soup = BeautifulSoup(html, 'lxml')
        try:
            driver.find_element_by_id('header-logo')
            error = False
            single_company_features = soup.find_all('div', class_='D(tbr)')
            driver.quit()
        except (NoSuchElementException, WebDriverException):
            sleep(30)
            driver.refresh()
    return single_company_features


# create a data frame with the html
def create_dataframe(features_of_company):
    headers = []
    temp_list = []
    final = []
    index = 0
    # create headers
    for item in features_of_company[0].find_all('div', class_='D(ib)'):
        try:
            dt = datetime.strptime(item.text, '%m/%d/%Y')
            headers.append('year_' + str(dt.year))
        except ValueError:
            headers.append(item.text)

    # statement contents
    while index <= len(features_of_company) - 1:
        # filter for each line of the statement
        temp = features_of_company[index].find_all('div', class_='D(tbc)')
        for line in temp:
            # each item adding to a temporary list
            temp_list.append(line.text)
        # temp_list added to final list
        final.append(temp_list)
        # clear temp_list
        temp_list = []
        index += 1
    df = pd.DataFrame(final[1:])
    headers[1] = 'year_ttm'
    df.columns = headers
    return df


# create a dataframe with the financial information of a single index company
def clean_dataframe(symbol):
    company_link = 'https://finance.yahoo.com/quote/{}/financials'.format(symbol)
    # sleep(randint(5,50))
    single_company_features = driver_setup(company_link)
    company_financials_df = create_dataframe(single_company_features)
    company_financials_df = pd.wide_to_long(
        company_financials_df, ['year_'], i='Breakdown', j='year', suffix='(\d+|\w+)')
    company_financials_df = company_financials_df.reset_index(level=[0, 1])
    company_financials_df.insert(loc=0, column='Company', value=symbol)
    return company_financials_df


## data clean medium: '-' are included and columns all have object dtype

# function that removes commas and makes all values numerical
def convert_to_numeric(column):
    uncleaned_df = [i.replace(',', '') for i in column]
    return uncleaned_df


# function that removes commas in the whole df AND fills NaN with '-'
def convert_dataframe_to_numeric(uncleaned_df):
    for column in list(uncleaned_df.columns[2:]):
        uncleaned_df[column] = convert_to_numeric(uncleaned_df[column])
    processed_df = uncleaned_df.fillna('-')
    return processed_df


# renames the columns
def rename_columns(uncleaned_df):
    uncleaned_df = uncleaned_df.sort_values(by=['Breakdown', 'year'])
    uncleaned_df.rename(columns={'year_': 'Values'}, inplace=True)
    uncleaned_df.rename(columns={'year': 'Year'}, inplace=True)
    return uncleaned_df


# uses previous functions to clean the dataframe
def clean_data(initial_df):
    processed_df = convert_dataframe_to_numeric(initial_df)
    cleaned_df = rename_columns(processed_df)
    return cleaned_df


## data clean final: removed '-' and reformat columns

# returns two dataframes: ttm values and remaining values and formats columns
def final_clean(cleaned_df):
    # replacing of '-'
    cleaned_df['Values'] = cleaned_df['Values'].replace('-', np.nan)
    cleaned_df = cleaned_df.dropna()
    cleaned_df = cleaned_df.reset_index(drop=True)

    # separate ttm in new dataframe
    cleaned_ttm = cleaned_df.loc[cleaned_df['Year'] == 'ttm']
    cleaned_final_ttm = cleaned_ttm.reset_index(drop=True)

    # removing ttm
    cleaned_df = cleaned_df.drop(cleaned_df[cleaned_df['Year'] == 'ttm'].index)
    cleaned_df = cleaned_df.reset_index(drop=True)

    # reformat datatypes for columns
    cleaned_df['Year'] = cleaned_df['Year'].astype('int64')
    cleaned_df['Values'] = cleaned_df['Values'].astype('float')
    cleaned_final_df = cleaned_df
    return cleaned_final_df, cleaned_final_ttm


if __name__ == '__main__':
    # components link (example): https://finance.yahoo.com/quote/%5EDJI/components
    link = 'components_link'
    features = scrape_index_companies(link)
    companies_df = create_companies_df(features)
    companies = companies_df.iloc[0:, 0]
    pool = mp.Pool(mp.cpu_count() - 1)

    split_ratio = list(range(0, len(companies) + 1, 5))

    index_df_split_1 = pd.concat(pool.map(
        clean_dataframe, companies[0:split_ratio[1]]))
    index_df_split_2 = pd.concat(pool.map(
        clean_dataframe, companies[split_ratio[1]:split_ratio[2]]))
    index_df_split_3 = pd.concat(pool.map(
        clean_dataframe, companies[split_ratio[2]:split_ratio[3]]))
    index_df_split_4 = pd.concat(pool.map(
        clean_dataframe, companies[split_ratio[3]:split_ratio[4]]))
    index_df_split_5 = pd.concat(pool.map(
        clean_dataframe, companies[split_ratio[4]:split_ratio[5]]))
    index_df_split_6 = pd.concat(pool.map(
        clean_dataframe, companies[split_ratio[5]:split_ratio[6]]))

    index_df = pd.concat([index_df_split_1, index_df_split_2, index_df_split_3,
                          index_df_split_4, index_df_split_5, index_df_split_6])
    pool.terminate()
    pool.join()
    clean_df = clean_data(index_df)
    final_df, final_ttm = final_clean(clean_df)
