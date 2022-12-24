import logging
import stock_market_database
import pandas as pd
import yfinance as yf

from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from tokens import BOT_TOKEN

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


def start(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    context.user_data['indices'] = ["CAC40"]
    context.user_data['tickers'] = pd.read_html(
        'https://en.wikipedia.org/wiki/CAC_40', flavor='html5lib')[4]['Ticker'].to_list()
    context.user_data['updates'] = {ticker: '2022-12-23 10:00:00'
                                    for ticker in context.user_data['tickers']}

    context.bot.send_message(chat_id=update.effective_chat.id, text='Monitoring CAC40 companies.')

    context.job_queue.run_repeating(notify_about_latest_shocks, interval=30, first=5,
                                    context={'updates': context.user_data['updates'],
                                             'chat_id': update.message.chat_id})


def help_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /help is issued."""
    update.message.reply_text('Help!')


def notify_about_latest_shocks(context: CallbackContext) -> None:
    """
    Notify the user about the latest stock market shocks
    """
    print(context.job.context['updates'])
    messages = db.fetch_shock_history(user_updates=context.job.context['updates'])
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

    # Create the Updater and pass it your bot's token.
    updater = Updater(BOT_TOKEN)

    # Get the JobQueue
    job_queue = updater.job_queue

    # Regularly update the database
    job = job_queue.run_repeating(update_database, interval=30, first=30)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_command))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
