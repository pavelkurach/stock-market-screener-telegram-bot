# stock-market-screener-telegram-bot
__v.02__

This is a stock market shock screening bot, that can notify in real-time about unusual stock price movements.

It can only notify about companies included in the following indices: CAC40 (France), S&P500 (USA), FTSE100 (UK), DAX (Germany).

_For debug purposes, it shows all notifications since 2023-01-06 10:00._

## Structure

1. [stock_market_database.py](https://github.com/pavelkurach/stock-market-screener-telegram-bot/blob/master/stock_market_database.py)

A simple interface for creating, initializing, updating, and fetching data from database is implemented. 
The database is a __postgreSQL__ database, connected via __SQLAlchemy__. 
Stock price data is initialized with a 5-day history and a 5-minutes interval.

2. [telegram_bot.py](https://github.com/pavelkurach/stock-market-screener-telegram-bot/blob/master/telegram_bot.py)

Bot-user interaction is implemented.
![image](https://user-images.githubusercontent.com/101255623/211352791-b22582da-e561-4088-9fce-1a9f81268802.png)
