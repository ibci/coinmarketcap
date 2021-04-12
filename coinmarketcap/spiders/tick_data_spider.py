
import scrapy
import os
import time
import re
import requests
import json
import pandas as pd
import numpy as np
import urllib.parse as urlparse
from collections import defaultdict
from scrapy.http import Request, HtmlResponse


# execute with: scrapy crawl tick_data

class TickDataSpider(scrapy.Spider):
    name = 'tick_data'
    start_urls = [
        # start with the main page and process the rest from there
        'https://coinmarketcap.com/',
    ]
    custom_settings = {
        'DOWNLOAD_DELAY': 10,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
    }
    tick_data_path = 'data/tick_data.csv'
    tick_data = pd.DataFrame()
    if os.path.exists(tick_data_path):
        tick_data = pd.read_csv(tick_data_path)
        tick_data['timestamp'] = pd.to_datetime(tick_data['timestamp'])


    def parse(self, response):
        # get total number of pages (100 cryptocurrencies per page)
        n_pages = response.css('ul.pagination > li.page')[-1].css('a::text').get()
        n_pages = int(n_pages)
        # process each page
        for i in range(1, n_pages+1):
            link = response.urljoin(f'?page={i}')
            yield Request(link, callback=self.parse_page)


    def parse_page(self, response):
        # parse main page with all the cryptocurrencies list
        # process each coin's subpage
        for row in response.css('table.cmc-table tr'):
            coin_href = row.css('a.cmc-link::attr(href)').extract_first()
            if coin_href:
                link = response.urljoin(coin_href)
                yield Request(link, callback=self.parse_coin)


    def parse_coin(self, response):
        # get coin ID and name
        logo_url = response.css('div.sc-AxhCb.hMIMmV.nameHeader___27HU_ img::attr(src)').get()
        coin_id = int(logo_url.split('/')[-1].split('.')[0])
        coin_name = response.css('h2.sc-fzqBZW::text').get()
        coin_url = urlparse.urlparse(response.url).path.split('/')[-2]
        # process current data )wallets...)
        self.process_current_data(response, coin_id, coin_name, coin_url)


    def process_current_data(self, response, coin_id, coin_name, coin_url):
        params = {
            'id': coin_id,
            'name': coin_name,
            'name_url': coin_url,
            'timestamp': pd.Timestamp.utcnow(),
        }
        # get current main values (market cap, volume 24h...)
        data = self.get_coin_current_data(response)
        # get wallets info
        wallets = self.get_coin_current_wallets(coin_url)
        # get main news
        news = self.get_coin_current_news(coin_url)
        # merge all data
        merge = {**params, **data, **wallets, **news}
        # convert lists to string list representation
        for k, v in merge.items():
            if type(v) == list:
                merge[k] = str(v)
        # concatenate with previous dataframe
        merge_df = pd.DataFrame(merge, index=[0])
        self.tick_data = pd.concat([self.tick_data, merge_df], ignore_index=True)
        self.tick_data\
            .sort_values(['name', 'timestamp'])\
            .to_csv(self.tick_data_path, index=False)


    def get_coin_current_data(self, response):
        # returns the current coin's data given in the response
        # get values
        price = response.css('div.priceValue___11gHJ::text').get()
        values = response.css('div.statsValue___2iaoZ::text').getall()
        market_cap = values[0] if values[0] != '- -' else np.nan
        volume24h = values[2] if values[2] != '- -' else np.nan
        circulating_supply = values[4] if values[4] != '- -' else np.nan
        return {
            'price': float(price.replace('$', '').replace(',', '')),
            'market_cap': int(market_cap.replace('$', '').replace(',', '')),
            'volume24h': int(volume24h.replace('$', '').replace(',', '')),
            'circulating_supply': int(circulating_supply.split(' ')[0].replace(',', ''))
        }


    def get_coin_current_wallets(self, coin_url):
        # returns wallet providers for storing the coin
        wallets_url = f'https://coinmarketcap.com/currencies/{coin_url}/wallets/'
        resp = requests.get(wallets_url)
        # process json script with wallets data
        resp_html = HtmlResponse(wallets_url, body=resp.text, encoding='utf-8')
        script = resp_html.css('script#__NEXT_DATA__').get()
        # replace script tags for processing json data from string
        script = re.sub(r'<script.*?>', '', script)
        script = script.replace('</script>', '')
        data = json.loads(script)
        wallets = data['props']['initialProps']['pageProps']['info']['wallets']
        return {
            'n_wallets': len(wallets),
            'wallets': [w['name'] for w in wallets]
        }

    
    def get_coin_current_news(self, coin_url):
        news_url = f'https://api.coinmarketcap.com/data-api/v3/headlines/alexandria/articles/content?slug={coin_url}&size=8&page=0'
        resp = requests.get(news_url).json()
        return {
            'news_titles': [d['title'] for d in resp['data']],
            'news_urls': [d['url'] for d in resp['data']]
        }