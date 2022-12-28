import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import MetaData
import yfinance as yf


class StockPriceHistoryDatabase:
    WIKI = 'https://en.wikipedia.org/wiki/'

    def __init__(self, indices=None):

        if indices is None:
            indices = ['CAC40', 'S&P500', 'FTSE100', 'DAX']

        self.indices = indices
        self.tickers = []

        # Get the list of companies in the indices
        if 'CAC40' in self.indices:
            self.tickers.extend(pd.read_html(self.WIKI + 'CAC_40',
                                             flavor='html5lib')[4]['Ticker'].to_list())

        if 'S&P500' in self.indices:
            self.tickers.extend(pd.read_html(self.WIKI + 'List_of_S%26P_500_companies',
                                             flavor='html5lib')[0]['Symbol'].to_list())

        if 'FTSE100' in self.indices:
            self.tickers.extend(pd.read_html(self.WIKI + 'FTSE_100_Index',
                                             flavor='html5lib')[4]['EPIC'].to_list())

        if 'DAX' in self.indices:
            self.tickers.extend(pd.read_html(self.WIKI + 'DAX',
                                             flavor='html5lib')[4]['Ticker'].to_list())

        # Create database engine
        self.engine = create_engine(
            f'postgresql://pavelkurach@localhost:5432/stock_price_history')
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        else:
            # Clear database
            metadata = MetaData(bind=self.engine)
            metadata.reflect()
            metadata.drop_all()
            self.engine.dispose()

        print("Successfully created a database")

        # Get stock market data
        for ticker in self.tickers:
            frame = yf.download(tickers=ticker,
                                period="5d",
                                interval="5m").reset_index().iloc[:-20, :]

            if (not frame.empty) and 'Datetime' in frame.columns:
                frame.to_sql(ticker, self.engine, if_exists='replace', index=False)
                with self.engine.connect() as con:
                    con.execute(
                        f'ALTER TABLE "{ticker}" ADD PRIMARY KEY ("{frame.columns[0]}");')
            else:
                self.tickers.remove(ticker)

        # Create an object-relation mapping with sqlalchemy automap
        self.base = automap_base()
        self.base.prepare(autoload_with=self.engine)
        self.data_classes = {}
        for cls in self.base.classes:
            self.data_classes[cls.__name__] = cls
        self.tickers = list(self.data_classes.keys())

        # Start a session
        self.session = Session(self.engine)

        # Get volatilities
        self.sigma = {}
        for ticker in self.tickers:
            query = self.session.query(self.data_classes[ticker])
            sigma_df = pd.read_sql(query.statement, query.session.bind)
            self.sigma[ticker] = sigma_df.Close.pct_change().std()

        # Get datetime of the latest ticks
        self.updates = {ticker: self.session.query(self.data_classes[ticker]).order_by(
            self.data_classes[ticker].Datetime.desc()).first().Datetime for ticker in self.tickers}

    def update_database(self) -> None:
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

    def fetch_shock_history(self, user_tickers: list, user_updates: dict) -> list:
        """
        Retrieving stock market shock updates
        :param user_tickers: list of companies user want to be notified about
        :param user_updates: contains datetime for each ticker, the function notifies about
        shocks happening after this datetime
        :return: list of string (each string contains a message about a recent market shock)
        """
        result = []

        for ticker in user_tickers:
            if ticker in self.tickers:
                query = self.session.query(self.data_classes[ticker]).filter(
                    self.data_classes[ticker].Datetime >= user_updates[ticker])
                ticker_data = pd.read_sql(query.statement, query.session.bind)

                last_close = None

                for _, tick in ticker_data.iterrows():
                    if _ == 0:
                        last_close = tick["Close"]
                        continue

                    change_ratio = (tick["Close"] / last_close - 1) / self.sigma[ticker]
                    if change_ratio > 3 or change_ratio < -3:
                        result.append(
                            f'{ticker} stock price moved by '
                            f'{(tick["Close"] / last_close - 1) * 100:.2f}% '
                            f'({change_ratio:.2f}x volatility) '
                            f'from {user_updates[ticker].strftime("%H:%M")} '
                            f'to {tick["Datetime"].strftime("%H:%M")}.')
                    last_close = tick["Close"]
                    user_updates[ticker] = tick['Datetime']

        return result

    def print_tables(self):
        for ticker in self.tickers:
            query = self.session.query(self.data_classes[ticker])
            print(ticker)
            print(pd.read_sql(query.statement, query.session.bind))


def main():
    pass


if __name__ == '__main__':
    main()
