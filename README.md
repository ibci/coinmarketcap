# CoinMarketCap Scraper

Recull dades de l’històric diari per a cadascuna de les criptomonedes de CoinMarketCap (www.coinmarketcap.com), així com de dades actualitzades al moment de l'execució del scraper pertinent.

Per executar la recol·lecció de dades històriques diàries:
```
scrapy crawl daily_data -o data/daily_data.csv
```

Per executar la recol·lecció de dades actuals:
```
scrapy crawl tick_data
```
