import logging
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import MetaData
import yfinance as yf

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)


class StockPriceHistoryDatabase:
    WIKI = 'https://en.wikipedia.org/wiki/'

    @staticmethod
    def get_symbols(indices=None):
        symbols = []

        if 'CAC40' in indices:
            symbols.extend(pd.read_html(StockPriceHistoryDatabase.WIKI + 'CAC_40',
                                        flavor='html5lib')[4]['Ticker'].to_list())

        if 'S&P500' in indices:
            symbols.extend(pd.read_html(StockPriceHistoryDatabase.WIKI +
                                        'List_of_S%26P_500_companies',
                                        flavor='html5lib')[0]['Symbol'].to_list())

        if 'FTSE100' in indices:
            symbols.extend(pd.read_html(StockPriceHistoryDatabase.WIKI + 'FTSE_100_Index',
                                        flavor='html5lib')[4]['EPIC'].to_list())

        if 'DAX' in indices:
            symbols.extend(pd.read_html(StockPriceHistoryDatabase.WIKI + 'DAX',
                                        flavor='html5lib')[4]['Ticker'].to_list())

        return symbols

    def _create_database_engine(self):
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

        logger.info('Successfully created a database')

    def _get_stock_market_data(self):
        for symbol in self.symbols:
            frame = yf.download(tickers=symbol,
                                period="5d",
                                interval="5m", progress=False).reset_index().iloc[:-20, :]

            if (not frame.empty) and 'Datetime' in frame.columns:
                frame.to_sql(symbol, self.engine, if_exists='replace', index=False)
                with self.engine.connect() as con:
                    con.execute(
                        f'ALTER TABLE "{symbol}" ADD PRIMARY KEY ("{frame.columns[0]}");')
            else:
                self.symbols.remove(symbol)

        logger.info('Downloaded data for the following symbols: %s', ' '.join(self.symbols))

    def _create_orm_with_automap(self):
        self.base = automap_base()
        self.base.prepare(autoload_with=self.engine)
        self.data_classes = {}
        for cls in self.base.classes:
            self.data_classes[cls.__name__] = cls
        self.symbols = list(self.data_classes.keys())
        logger.info('Created ORM with automap.')

    def _get_volatilities(self):
        sigma = {}
        for symbol in self.symbols:
            query = self.session.query(self.data_classes[symbol])
            sigma_df = pd.read_sql(query.statement, query.session.bind)
            sigma[symbol] = sigma_df.Close.pct_change().std()
        logger.info('Calculated volatilities')
        return sigma

    def __init__(self, indices=None):

        if indices is None:
            #indices = ['CAC40', 'S&P500', 'FTSE100', 'DAX']
            indices = ['CAC40']
        self.indices = indices
        self.symbols = StockPriceHistoryDatabase.get_symbols(indices)

        self._create_database_engine()
        self._get_stock_market_data()
        self._create_orm_with_automap()

        # Start a session
        self.session = Session(self.engine)

        self.sigma = self._get_volatilities()

        # Get datetime of the latest ticks
        self.updates = {symbol: self.session.query(self.data_classes[symbol]).order_by(
            self.data_classes[symbol].Datetime.desc()).first().Datetime for symbol in self.symbols}

    def update_database(self) -> None:
        """
        Update database function, should be regularly called.
        """
        for symbol in self.symbols:
            new_data = yf.download(tickers=symbol,
                                   period="1d",
                                   interval="5m", progress=False).reset_index()
            new_data = new_data[new_data['Datetime'] > self.updates[symbol]]
            for _, tick in new_data.iterrows():
                new_tick = self.data_classes[symbol](**tick.to_dict())
                self.session.add(new_tick)
                self.session.commit()
                self.updates[symbol] = tick['Datetime']
        logger.info('Updated database')

    def fetch_shock_history(self, user_symbols: list, user_updates: dict) -> list:
        """
        Retrieving stock market shock updates
        :param user_symbols: list of companies user want to be notified about
        :param user_updates: contains datetime for each symbol, the function notifies about
        shocks happening after this datetime
        :return: list of string (each string contains a message about a recent market shock)
        """
        result = []

        for symbol in user_symbols:
            if symbol in self.symbols:
                query = self.session.query(self.data_classes[symbol]).filter(
                    self.data_classes[symbol].Datetime >= user_updates[symbol])
                symbol_data = pd.read_sql(query.statement, query.session.bind)

                last_close = None

                for _, tick in symbol_data.iterrows():
                    if _ == 0:
                        last_close = tick["Close"]
                        continue

                    change_ratio = (tick["Close"] / last_close - 1) / self.sigma[symbol]
                    if change_ratio > 3 or change_ratio < -3:
                        result.append(
                            f'{symbol} stock price moved by '
                            f'{(tick["Close"] / last_close - 1) * 100:.2f}% '
                            f'({change_ratio:.2f}x volatility) '
                            f'from {user_updates[symbol].strftime("%H:%M")} '
                            f'to {tick["Datetime"].strftime("%H:%M")}.')
                    last_close = tick["Close"]
                    user_updates[symbol] = tick['Datetime']
        logger.info('Sent data to a user.')
        return result

