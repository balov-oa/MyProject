import logging
import time
from datetime import datetime
from pathlib import Path
from random import randint
from typing import (
    Any,
    Dict,
    List,
    Set,
)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from sql_connector import (
    get_sqlalchemy_engine,
)

SESSION = requests.Session()

ENGINE = get_sqlalchemy_engine()


def get_soup_by_url(url: str) -> BeautifulSoup:
    html = SESSION.get(url).text
    soup = BeautifulSoup(html, 'lxml')
    return soup


def get_number_last_page() -> int:
    soup = get_soup_by_url(
        'https://www.tomsk.ru09.ru/'
        'realty?type=1&otype=1&district[1]=on&district[2]=on&district[3]'
        '=on&district[4]=on&perpage=50&page=1')
    number_last_page = int(soup.find('td', {'class': 'pager_pages'}).find_all('a')[4].text)
    return number_last_page


def find_district_field(keys: List[str]) -> int:
    for i, j in enumerate(keys):
        if ' район' in j:
            return i


def parse_apartment(url: str) -> Dict[str, Any]:
    soup = get_soup_by_url(url)

    keys = [i.find('span').text.replace('\xa0', '').lower() for i in
            soup.find_all('tr', {'class': 'realty_detail_attr'})]

    district_idx = find_district_field(keys)
    items = {'район': keys[district_idx]}

    keys = [j for i, j in enumerate(keys) if i not in (district_idx - 1, district_idx)]
    values = [i.text.replace('\xa0', ' ') for i in soup.find_all(class_='nowrap')]

    items.update(dict(zip(keys, values)))

    items['адрес'] = soup.find(class_='table_map_link').text.replace('\xa0', ' ')
    items['цена'] = int(
        soup.find('div', {'class': 'realty_detail_price inline'}).text.replace('\xa0', '').replace('руб.', ''))
    items['ид'] = int(soup.find('strong').text)
    items['дата добавления'] = soup.find(class_='realty_detail_date nobr').get('title')
    items['дата истечения'] = soup.find_all(class_='realty_detail_date')[4].get('title')
    items['ссылка'] = url
    return items


def get_urls_pages(start_page: int = 1, end_page: int = None) -> List[str]:
    url_base = 'https://www.tomsk.ru09.ru/' \
               'realty?type=1&otype=1&district[1]=on&district[2]=on&district[3]=on&district[4]=on&perpage=50&page='

    end_page = end_page or get_number_last_page()
    pages_to_parse = range(start_page, end_page + 1)
    urls_pages = [url_base + str(i) for i in pages_to_parse]
    return urls_pages


def get_urls_apartments_by_page(url_page: str) -> Set[str]:
    url_base = 'https://www.tomsk.ru09.ru'

    soup = get_soup_by_url(url_page)
    soup = soup.find_all('a', {'class': 'visited_ads'})

    urls_apartments = {url_base + i.get('href') for i in soup}
    return urls_apartments


def main(start_page: int = 1, end_page: int = None) -> None:
    rename_map = {'район': 'District',
                  'адрес': 'Address',
                  'вид': 'Sales_Type',
                  'год постройки': 'Year_Building',
                  'материал': 'Material',
                  'этаж/этажность': 'Floor_Numbers_Of_Floors',
                  'этажность': 'Floors_In_Building',
                  'тип квартиры': 'Apartment_Type',
                  'цена': 'Price',
                  'общая площадь': 'Square_Total',
                  'жилая': 'Square_Living',
                  'кухня': 'Square_Kitchen',
                  'количество комнат': 'Rooms_Number',
                  'отделка': 'Apartment_Condition',
                  'санузел': 'Bathroom_Type',
                  'балкон/лоджия': 'Balcony_Loggia',
                  'дата добавления': 'Date_Add',
                  'дата истечения': 'Date_Expiration',
                  'ссылка': 'Url_Link',
                  'ид': 'Id'}

    df = pd.DataFrame(columns=list(rename_map.keys()))

    urls_in_database = pd.read_sql('SELECT DISTINCT Url_Link FROM Apartment_Tomsk.dbo.Apartments', ENGINE)
    urls_in_database = set(urls_in_database['Url_Link'])

    len_storage = len(urls_in_database)
    print('Apartments in storage:', len_storage, '\n')
    logging.info(f'Apartments in storage: {len_storage}')

    urls_pages = get_urls_pages(start_page, end_page)
    for url_page in tqdm(urls_pages, desc='Pages', leave=False, ascii=True):
        urls_apartments = get_urls_apartments_by_page(url_page)
        urls_apartments_to_parse = urls_apartments.difference(urls_in_database)

        if urls_apartments_to_parse:
            list_to_dataframe = []
            for url_apartment in tqdm(urls_apartments_to_parse, desc='Apartments', leave=False, ascii=True):
                try:
                    list_to_dataframe.append(parse_apartment(url_apartment))
                except Exception as e:
                    with open(Path(__file__).parent.parent.joinpath('logs').joinpath('bad_links.txt'), 'a') as f:
                        f.write(f'{url_apartment} -- {e}\n')
                        logging.exception(e)
                finally:
                    time.sleep(randint(0, 4))
            df = df.append(list_to_dataframe, ignore_index=True)
        time.sleep(randint(0, 4))

    if not df.empty:
        df.drop_duplicates(inplace=True)
        df.rename(columns=rename_map, inplace=True)
        df['Download_timestamp'] = datetime.now()
        df.to_sql(name='Apartments', con=ENGINE, schema='dbo', if_exists='append', index=False)

    new_apartments = len(df.index)
    print(f'New Apartments: {new_apartments}')
    logging.info(f'New Apartments: {new_apartments}')


if __name__ == '__main__':
    log_file = Path(__file__).parent.parent.joinpath('logs').joinpath('downloader.txt')
    logging.basicConfig(
        format='[%(asctime)s] -- %(levelname).3s -- %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
        level=logging.DEBUG,
        filename=log_file)

    logging.info('Download start')
    try:
        main()
    except Exception as E:
        logging.exception(E)
