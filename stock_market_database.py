import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import yfinance as yf


class StockPriceHistoryDatabase:
    WIKI = 'https://en.wikipedia.org/wiki/'

    def __init__(self, indices=['CAC40']):

        self.indices = indices
        self.tickers = []

        # get the list of companies in the indices
        if 'CAC40' in self.indices:
            self.tickers.extend(
                pd.read_html(self.WIKI + 'CAC_40', flavor='html5lib')[4]['Ticker'].to_list())

        print(self.tickers)

        # create database engine
        self.engine = create_engine(f'postgresql://pavelkurach@localhost:5432/stock_price_history')
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        print("Successfully created a database")

        # get stock market data
        stock_market_data = []
        for ticker in self.tickers:
            stock_market_data.append(yf.download(tickers=ticker,
                                                 period="5d",
                                                 interval="5m").reset_index().iloc[:-20, :])

        # put stock market data to the database and add a primary key
        # primary key is needed for sqlalchemy automap
        for frame, ticker in zip(stock_market_data, self.tickers):
            frame.to_sql(ticker, self.engine, if_exists='replace', index=False)
            with self.engine.connect() as con:
                con.execute(f'ALTER TABLE "{ticker}" ADD PRIMARY KEY ("{frame.columns[0]}");')

        # create an object-relation mapping with sqlalchemy automap
        self.base = automap_base()
        self.base.prepare(autoload_with=self.engine)
        self.data_classes = {ticker: cls for ticker, cls in zip(self.tickers, self.base.classes)}

        # start a session
        self.session = Session(self.engine)

        # get volatilities
        self.sigma = {}
        for ticker in self.tickers:
            query = self.session.query(self.data_classes[ticker])
            sigma_df = pd.read_sql(query.statement, query.session.bind, index_col='Datetime')
            self.sigma[ticker] = sigma_df.Close.pct_change().std()

        # get datetime of the latest ticks
        self.updates = {ticker: self.session.query(self.data_classes[ticker]).order_by(
            self.data_classes[ticker].Datetime.desc()).first().Datetime for ticker in self.tickers}

    def update_database(self):
        """
        Update database function, should be regularly called.
        """
        for ticker in self.tickers:
            new_data = yf.download(tickers=ticker,
                                   period="1d",
                                   interval="5m", progress=False).reset_index()
            new_data = new_data[new_data['Datetime'] > self.updates[ticker]]
            for _, tick in new_data.iterrows():
                new_tick = self.data_classes[ticker](**tick.to_dict())
                self.session.add(new_tick)
                self.session.commit()
                self.updates[ticker] = tick['Datetime']

    def fetch_shock_history(self, user_indices: list, user_updates: dict) -> list:
        """
        Retrieving stock market shock updates
        :param user_indices: list of indices that contain companies user want to be notified about
        :param user_updates: contains datetime for each ticker, the function notifies about
        shocks happening after this datetime
        :return: list of string (each string contains a message about a recent market shock)
        """
        user_tickers = []
        result = []

        if 'CAC40' in user_indices:
            user_tickers.append(
                pd.read_html(self.WIKI + 'CAC_40', flavor='html5lib')[4]['Ticker'].to_list())

        for ticker in user_tickers:
            query = self.session.query(self.data_classes[ticker]).filter(
                self.data_classes[ticker].Datetime >= user_updates[ticker])
            ticker_data = pd.read_sql(query.statement, query.session.bind)

            last_close = None

            for _, tick in ticker_data.iterrows():
                if _ == 0:
                    last_close = tick["Close"]
                    continue

                change_ratio = (tick["Close"] / last_close - 1) / self.sigma[ticker]
                if change_ratio > 2:
                    result.append(
                        f'{ticker} stock price moved by {(tick["Close"] / last_close - 1) * 100:.2f}% '
                        f'({change_ratio:.2f}x volatility) '
                        f'from {user_updates[ticker].strftime("%H:%M")} '
                        f'to {tick["Datetime"].strftime("%H:%M")}.')
                last_close = tick["Close"]
                user_updates[ticker] = tick['Datetime']

            return result

    def print_first_table(self):
        query = self.session.query(self.data_classes['AIR.PA'])
        print(pd.read_sql(query.statement, query.session.bind))


def main():
    db = StockPriceHistoryDatabase()
    print("before update")
    db.print_first_table()
    db.update_database()
    print("after update")
    db.print_first_table()
    print("sigmas")
    print(db.sigma)


if __name__ == '__main__':
    main()
