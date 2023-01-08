import logging
import stock_market_database
import pandas as pd
import yfinance as yf
from datetime import datetime

from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from tokens import BOT_TOKEN

WIKI = 'https://en.wikipedia.org/wiki/'

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


def monitor(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    if not ('indices' in context.user_data.keys()):
        context.user_data['indices'] = []
        context.user_data['tickers'] = []
        context.user_data['updates'] = {}

    args = context.args
    args = [arg.upper() for arg in args]

    msg = []

    for index in args:
        if index in TICKERS_DICT.keys():
            if index in context.user_data['indices']:
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Already monitoring {index} companies.')
            else:
                context.user_data['indices'].append(index)
                context.user_data['tickers'].extend(TICKERS_DICT[index])
                msg.append(index)
                for ticker in TICKERS_DICT[index]:
                    # It should be "datetime.now", I put another datetime for test only
                    context.user_data['updates'][ticker] = datetime.strptime('2023-01-06 10:00:00',
                                                                             '%Y-%m-%d %H:%M:%S')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'{index} is not a supported index.')

    if msg:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'Start monitoring companies from the following indices: ' +
                                      ', '.join(msg) + '.')

    context.job_queue.run_repeating(notify_about_latest_shocks, interval=30, first=5,
                                    context={'updates': context.user_data['updates'],
                                             'tickers': context.user_data['tickers'],
                                             'chat_id': update.message.chat_id})


def stop(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    if not ('indices' in context.user_data.keys()) or context.user_data['indices'] == []:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text='You have not started monitoring yet.')
        return

    args = context.args
    if not args:
        context.user_data['indices'] = []
        context.user_data['tickers'] = []
        context.user_data['updates'] = {}
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text='Stopped monitoring any company.')

    args = [arg.upper() for arg in args]

    msg = []

    for index in args:
        if index in TICKERS_DICT.keys():
            if index in context.user_data['indices']:
                context.user_data['indices'].remove(index)
                msg.append(index)
            else:
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'You are not monitoring {index} companies.')

    context.user_data['tickers'] = []
    for index in context.user_data['indices']:
        context.user_data['tickers'].extend(TICKERS_DICT[index])

    if msg:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'Stopped monitoring companies from the following indices: ' +
                                      ', '.join(msg) + '.')


def help(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /help is issued."""
    update.message.reply_text(
        'To start monitoring, use the following command:'
        '\r\n/monitor index1 index2 …\r\n\r\n'
        'Example: \r\n/monitor S&P500\r\n\r\n'
        'To stop monitoring, use the following commands:\r\n'
        '/stop index1 index2 …\r\n/stop \r\n\r\nExample: \r\n'
        '/stop S&P500\r\n')


# noinspection PyUnresolvedReferences
def notify_about_latest_shocks(context: CallbackContext) -> None:
    """
    Notify the user about the latest stock market shocks
    """
    print(context.job.context['updates'])
    messages = db.fetch_shock_history(user_tickers=context.job.context['tickers'],
                                      user_updates=context.job.context['updates'])
    if messages:
        for msg in messages:
            context.bot.send_message(chat_id=context.job.context['chat_id'],
                                     text=msg)


def update_database(context: CallbackContext) -> None:
    """
    Update the database
    """
    db.update_database()


def main() -> None:
    """Start the bot."""

    # Create and initialize a database
    global db
    db = stock_market_database.StockPriceHistoryDatabase()

    # Get tickers lists
    global TICKERS_DICT
    cac40 = pd.read_html(WIKI + 'CAC_40', flavor='html5lib')[4]['Ticker'].to_list()
    sp500 = pd.read_html(WIKI + 'List_of_S%26P_500_companies',
                         flavor='html5lib')[0]['Symbol'].to_list()
    ftse100 = pd.read_html(WIKI + 'FTSE_100_Index', flavor='html5lib')[4]['EPIC'].to_list()
    dax = pd.read_html(WIKI + 'DAX', flavor='html5lib')[4]['Ticker'].to_list()
    TICKERS_DICT = {
        'CAC40': cac40,
        'S&P500': sp500,
        'FTSE100': ftse100,
        'DAX': dax
    }
    # Create the Updater and pass it your bot's token.
    updater = Updater(BOT_TOKEN)

    # Get the JobQueue
    job_queue = updater.job_queue

    # Regularly update the database
    job = job_queue.run_repeating(update_database, interval=30, first=30)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(CommandHandler("monitor", monitor))
    dispatcher.add_handler(CommandHandler("help", help))
    dispatcher.add_handler(CommandHandler("stop", stop))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
