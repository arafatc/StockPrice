"""
/*
 * This  File is modified/created by Arafat Chaghtai for educational purposes.
 * The author could be contacted at: arafatc@gmail.com for any clarifications.
 *
 * Licensed under GNU General Public License v3.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
 """
import datetime
import boto3
import pandas as pd
import json
import time
import yfinance as yf

stocks = ['MSFT', 'MVIS', 'GOOG', 'SPOT', 'INO', 'OCGN', 'ABML', 'RLLCF', 'JNJ', 'PSFE']

my_stream_name = 'kinesis_stocks'
kinesis = boto3.client('kinesis', region_name="us-east-1")
today = datetime.date.today()
prevday = datetime.date.today() - datetime.timedelta(days=1)


def put_to_stream(stock, timestamp, value, high_52week, low_52week, stockdate):
    payload = {
        'stock': str(stock),
        'timestamp': str(timestamp),
        'current_value': str(value),
        '52week_high': str(high_52week),
        '52week_low': str(low_52week),
        'stockdate': str(stockdate),
    }
    print(payload)
    put_response = kinesis.put_record(
        StreamName=my_stream_name,
        Data=json.dumps(payload),
        PartitionKey=str(stockid))


for stock in stocks:
    data = yf.download(stock, start=prevday, end=today, interval='1h')
    stockid = yf.Ticker(stock)
    high_52week = stockid.info['fiftyTwoWeekHigh']
    low_52week = stockid.info['fiftyTwoWeekLow']
    df = pd.DataFrame(data)
    d_list = df["Close"].to_dict()

    for key, val in d_list.items():
        timestamp = key
        value = val
        stockdate = key.strftime("%m/%d/%Y")
        put_to_stream(stock, timestamp, value, high_52week, low_52week, stockdate)

    # wait for milli-second
    time.sleep(0.25)
