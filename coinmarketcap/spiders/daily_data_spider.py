
import scrapy
import os
import time
import json
import pandas as pd
import urllib.parse as urlparse
from scrapy.http import Request

# execute with: scrapy crawl daily_data -o data/daily_data.csv

class HistoricalDataSpider(scrapy.Spider):
    name = 'daily_data'
    start_urls = [
        # start with the main page and process the rest from there
        'https://coinmarketcap.com/',
    ]
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
    }

    historical_data = pd.DataFrame()
    historical_data_path = 'data/daily_data.csv'
    if os.path.exists(historical_data_path):
        historical_data = pd.read_csv(historical_data_path)
        historical_data['timestamp'] = pd.to_datetime(historical_data['timestamp'])


    def close(self, reason):
        historical_data = pd.read_csv(self.historical_data_path)
        historical_data['timestamp'] = pd.to_datetime(historical_data['timestamp'])
        historical_data\
            .sort_values(['name', 'timestamp'])\
            .to_csv(self.historical_data_path, index=False)


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
        # process history
        coin_history = pd.DataFrame()
        
        # check if there was an entry for this coin
        if len(self.historical_data):
            coin_history = self.historical_data[self.historical_data['id'] == coin_id]

        # decide start time for the new retrieved history
        if len(coin_history):
            time_start = coin_history['timestamp'].max()
        else:
            # TODO get first trading day of the coin in unix timestamp
            time_start = pd.to_datetime('2013-1-1').tz_localize('UTC')
        
        current_timestamp = int(time.time())
        time_start_timestamp = time_start.value // 10**9
        if time_start_timestamp >= current_timestamp:
            raise Exception('Start time is greater than or equal to current time')

        historical_url = ('https://web-api.coinmarketcap.com/'
                          'v1/cryptocurrency/ohlcv/historical?'
                          f'id={coin_id}&convert=USD&'
                          f'time_start={time_start_timestamp}&'
                          f'time_end={current_timestamp}')
        meta = {
            'id': coin_id,
            'name': coin_name,
            'url_name': coin_url,
        }
        yield Request(historical_url, callback=self.parse_coin_history, meta=meta)


    def parse_coin_history(self, response):
        res_json = json.loads(response.text)
        for elem in res_json['data']['quotes']:
            yield {
                'id': response.meta['id'],
                'name': response.meta['name'],
                'url_name': response.meta['url_name'],
                'symbol': res_json['data']['symbol'],
                'timestamp': elem['quote']['USD']['timestamp'],
                'time_open': elem['time_open'],
                'time_close': elem['time_close'],
                'time_high': elem['time_high'],
                'time_low': elem['time_low'],
                'open': elem['quote']['USD']['open'],
                'close': elem['quote']['USD']['close'],
                'high': elem['quote']['USD']['high'],
                'low': elem['quote']['USD']['low'],
                'volume': elem['quote']['USD']['volume'],
                'market_cap': elem['quote']['USD']['market_cap'],
            }