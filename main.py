import io
import os
import ast
import json
import math
import time
import asyncio
import requests
import traceback
from urllib import request
from types import FunctionType
from dateutil.parser import parse
from datetime import datetime, timedelta
import aiohttp
import gspread
import anvil.server
import pandas as pd
from curio import Kernel
from bs4 import BeautifulSoup
from py5paisa import FivePaisaClient
from telethon.sessions import StringSession
from telethon.sync import TelegramClient, events
from flask import Flask, request as flask_request, render_template
from apscheduler.schedulers.background import BackgroundScheduler as bkgSch
from oauth2client.service_account import ServiceAccountCredentials

# https://myappcoordinator.chinmaysjoshi.repl.co
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()
app = Flask('app')
app.config['TIMEZONE'] = 'Asia/Kolkata'
all_info_dict = {}
work_info_dict = {}
func_dict = {}
gs_dict = {}
header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 "
                          "Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
dict_params, dict_params_2 = {'valueInputOption': 'RAW'}, {'valueInputOption': 'USER_ENTERED'}
sch_01, sch_02, sch_03, sch_04, sch_05 = bkgSch(), bkgSch(), bkgSch(), bkgSch(), bkgSch()
sch_01.start(), sch_02.start(), sch_03.start(), sch_04.start(), sch_05.start()


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/webhook', methods=['POST'])
def webhook():
    # Ref Axiom
    # Data received from Webhook is:
    # {'action': 'Open', 'event': {'ID': '7zEhpLOHHRYqM9Njsb', 'value': 1, 'source': 'monitors.FnXaqlly50Yq9pqgNW',
    #                              'body': 'Triggered with a value of 1', 'description': 'Memory usage 650 - 799 mb',
    #                              'IsGroupedQuery': False,
    #                              'title': 'Current value 1.000000 is Above Or Equal threshold value 0.5',
    #                              'timestamp': '2022-08-10T04:29:15.034461185Z', 'MonitorID': 'FnXaqlly50Yq9pqgNW',
    #                              'isOpened': True, 'externalIds': None}, 'sender': 'monitors'}
    # curl - X POST https://myappcoordinator.chinmaysjoshi.repl.co/webhook
    # -H "Content-Type: application/json"
    # -d "{\"action\": \"Open\", \"event\": {\"ID\": \"7zEhpLOHHRYqM9Njsb\", \"value\": 1, \"source\": " \
    #    "\"monitors.FnXaqlly50Yq9pqgNW\", \"body\": \"Triggered with a value of 1\", \"description\": " \
    #    "\"Memory usage 650 - 799 mb\", \"IsGroupedQuery\": false, \"title\": \"Current value 1.000000" \
    #    " is Above Or Equal threshold value 0.5\", \"timestamp\": \"2022-08-10T04:29:15.034461185Z\", " \
    #    "\"MonitorID\": \"FnXaqlly50Yq9pqgNW\", \"isOpened\": true, \"externalIds\": null}, \"sender\": \"monitors\"}"
    if flask_request.method == 'POST':
        req_json = flask_request.json
        if 'event' in req_json and 'title' in req_json['event']:
            if req_json['event']['description'].find('Memory') != -1:
                if req_json['event']['title'].find('Above') != -1:
                    work_info_dict['Heroku Secret Hamlet Memory Usage'] = req_json['event']['description']
                    anvil.server.call('aw_send_receive_info', req_json['event']['description'])
            elif req_json['event']['description'].find('Logs') != -1:
                if req_json['event']['title'].find('Above') != -1:
                    work_info_dict['Heroku Secret Hamlet Log Size'] = req_json['event']['description']
                else:
                    work_info_dict['Heroku Secret Hamlet Log Size'] = ''
        print("Data received from Webhook is: ", req_json)
        return "Webhook received!"


@app.route('/python_console', methods=['POST', 'GET'])
def python_console():
    if 'messages' not in work_info_dict:
        work_info_dict['messages'] = [{'command': 'NA', 'content': 'NA'}]

    if flask_request.method == 'POST':
        command = flask_request.form.to_dict()['command']
        output = misc_python_console(command)
        work_info_dict['messages'].append({'command': command, 'content': output})

    return render_template('python_console.html', messages=work_info_dict['messages'][::-1])


def init():
    global all_info_dict
    all_info_dict = ast.literal_eval(os.environ['all_info_dict'])
    # print(all_info_dict)
    anvil.server.connect(all_info_dict['anvil_server_uplink_url'])
    work_info_dict['misc_holiday_check'] = True
    work_info_dict['misc_holiday_check_2'] = True
    work_info_dict['a_loop'] = asyncio.new_event_loop()
    sch_02.add_job(gs_init, misfire_grace_time=120)
    sch_02.add_job(tt_init, misfire_grace_time=120)
    sch_05.add_job(heroku_keep_on, 'interval', minutes=10, misfire_grace_time=120)


def gs_init():
    gs_dict.update({'gs_cred_1': ServiceAccountCredentials.from_json_keyfile_dict(
        all_info_dict['gs_cred_1'], all_info_dict['gs_scope']),
        'gs_cred_2': ServiceAccountCredentials.from_json_keyfile_dict(
            all_info_dict['gs_cred_2'], all_info_dict['gs_scope'])})
    gs_dict.update({'gs_auth_cred_1': gspread.authorize(gs_dict['gs_cred_1']),
                    'gs_auth_cred_2': gspread.authorize(gs_dict['gs_cred_2']),
                    'base_spreadsheet': all_info_dict['base_spreadsheet'],
                    'rough_spreadsheet': all_info_dict['rough_spreadsheet']})
    sch_03.add_job(gs_handle_reads, 'interval', seconds=60, misfire_grace_time=40)
    sch_04.add_job(fp_init, 'interval', seconds=20, misfire_grace_time=120, id='fp_init')
    the_date_time = datetime.now().replace(minute=0, second=0)
    sch_05.add_job(misc_check_holiday, 'interval', minutes=10, misfire_grace_time=60, next_run_time=the_date_time)
    sch_05.add_job(misc_check_holiday_2, 'interval', minutes=10, misfire_grace_time=60, next_run_time=the_date_time)
    the_date_time = datetime.now().replace(hour=4, minute=0, second=0)
    sch_05.add_job(misc_generate_stocks_report_yf_2, 'interval', hours=12, misfire_grace_time=60,
                   next_run_time=the_date_time)


def gs_handle_reads():
    # Init
    debug_docs_range = all_info_dict['debug_docs_range']
    scripts_btst_holdings_range = all_info_dict['scripts_btst_holdings_range']
    scripts_btst_blacklist_range = all_info_dict['scripts_btst_blacklist_range']
    scripts_btst_blacklist_range_2 = all_info_dict['scripts_btst_blacklist_range_2']
    pfl_range = all_info_dict['pfl_range']
    scripts_info_df_range = all_info_dict['scripts_info_df_range']
    stocks_compare_tickers_range = all_info_dict['stocks_compare_tickers_range']
    stocks_interested_tickers_range = all_info_dict['stocks_interested_tickers_range']
    to_1cr_tickers_range = all_info_dict['to_1cr_tickers_range']
    trading_holidays_range = all_info_dict['trading_holidays_range']
    heroku_disable_enable_range = all_info_dict['heroku_disable_enable_range']

    # Batch read
    ranges = [scripts_btst_holdings_range, pfl_range, scripts_info_df_range, stocks_compare_tickers_range,
              stocks_interested_tickers_range, scripts_btst_blacklist_range, debug_docs_range,
              heroku_disable_enable_range]
    gs_data = gs_dict['gs_auth_cred_2'].open_by_key(gs_dict['base_spreadsheet']).values_batch_get(ranges)

    work_info_dict['dict_flags'] = {x[0]: x[1] for x in gs_data['valueRanges'][-2]['values']}
    work_info_dict['heroku_disable_enable'] = bool(int(gs_data['valueRanges'][-1]['values'][0][0]))


def gs_handle_write(sheet, range_str, values):
    gs_dict['gs_auth_cred_2'].open_by_key(gs_dict[sheet]).values_update(range_str, params=dict_params,
                                               body={'values': values})


def heroku_keep_on():
    dt_now = datetime.now()
    if not work_info_dict['heroku_disable_enable'] and dt_now.isoweekday() == 5 and 15 <= dt_now.hour < 23:
        requests.get(all_info_dict['heroku_url'])


def fp_init():
    if 'dict_flags' not in work_info_dict:
        return 1

    dict_flags = work_info_dict['dict_flags']
    fp_creds_1 = all_info_dict['fp_creds_1']
    fp_creds_2 = all_info_dict['fp_creds_2']
    work_info_dict.update({'fp_client_1': FivePaisaClient(
        fp_creds_1['username'], dict_flags['5p_pass'], fp_creds_1['pin'], fp_creds_1),
        'fp_client_2': FivePaisaClient(fp_creds_2['username'], dict_flags['5p_pass_2'], fp_creds_2['pin'], fp_creds_2)})
    client_1, client_2 = work_info_dict['fp_client_1'], work_info_dict['fp_client_2']
    client_1.login()
    client_2.login()

    if sch_04.get_job('fp_init'):
        sch_04.remove_job('fp_init')


@anvil.server.callable
def fp_validate_login(opt=1):
    return work_info_dict['fp_client_1'].jwt_validate() if opt == 1 else work_info_dict['fp_client_2'].jwt_validate()


def fp_get_ticker_codes():
    try:
        df_5p = pd.read_csv('fp_scrip_code.csv')
    except Exception:
        url_5p_codes = 'https://images.5paisa.com/website/scripmaster-csv-format.csv'
        df_5p = pd.DataFrame()
        for j in range(20):
            res = requests.get(url_5p_codes, headers=header)
            if res.status_code == 200:
                df_5p = pd.read_csv(io.BytesIO(res.content))
                break
    return df_5p


def fp_get_historical_data(tickers, interval, period):
    dt = datetime.now()
    inform(f'Downloading Historical Data from FivePaisa {interval} {period} {dt}')
    dt_start = (dt - timedelta(days=int(period[:-1]) * 7 / 5)).strftime('%Y-%m-%d')
    dt_stop = dt.strftime('%Y-%m-%d')
    client_1, client_2 = work_info_dict['fp_client_1'], work_info_dict['fp_client_2']
    failed_downloads_dict = {}
    # client = FivePaisaClient(email="shrinivas97@gmail.com", passwd="Purvi85+", dob="19560622")
    # client.login()
    # url_5p_codes = 'https://images.5paisa.com/website/scripmaster-csv-format.csv'
    # df_5p = pd.DataFrame()
    # for j in range(20):
    #     res = requests.get(url_5p_codes, headers=self.header)
    #     if res.status_code == 200:
    #         df_5p = pd.read_csv(io.BytesIO(res.content))
    #         break
    # self.tickers = [x.split('.')[0] for x in self.tickers]
    try:
        df_5p = pd.read_csv('fp_scrip_code.csv')
    except Exception:
        url_5p_codes = 'https://images.5paisa.com/website/scripmaster-csv-format.csv'
        df_5p = pd.DataFrame()
        for j in range(20):
            res = requests.get(url_5p_codes, headers=header)
            if res.status_code == 200:
                df_5p = pd.read_csv(io.BytesIO(res.content))
                break

    tickers = [x.split('.')[0] for x in tickers]
    for ticker in tickers:
        if ticker in failed_downloads_dict and failed_downloads_dict[ticker] > 2:
            tickers.remove(ticker)
    if len(tickers) < 1:
        return pd.DataFrame()
    for k, v in {'^NSEI': 'NIFTY', '^NSEBANK': 'BANKNIFTY'}.items():
        if k in tickers:
            tickers.remove(k)
            tickers.append(v)
    tickers_code = {v5p[3].strip(): v5p[2] for v5p in df_5p.values.tolist() if v5p[3].strip() in tickers and
                    v5p[0] == 'N' and v5p[1] == 'C'}
    data_list = []
    # p_bar = tqdm(total=len(tickers), desc='Downloader')
    failed_downloads = []

    def down(t, interval_1):
        nonlocal data_list
        for i in range(10):
            try:
                if t.strip() not in tickers_code:
                    failed_downloads.append(t)
                    break
                # df = util_download_5p_history(client, 'N', 'C', tickers_code[t], '1d', dt_start, dt_stop)
                df = client_1.historical_data('N', 'C', tickers_code[t.strip()], interval_1, dt_start, dt_stop)
                if df is None:
                    df = client_2.historical_data('N', 'C', tickers_code[t.strip()], interval_1, dt_start, dt_stop)
                if type(df) is str or df is None:
                    continue
                if interval_1 == '1d':
                    df.set_index('Datetime', inplace=True)
                    df['Close2'] = df['Close']
                    df.columns = pd.MultiIndex.from_tuples([(t, x) for x in ['Open', 'High', 'Low', 'Close',
                                                                             'Volume', 'Close2']])
                    df.index = [datetime.strptime(dti.split('T')[0], '%Y-%m-%d') for dti in df.index]
                elif interval_1[-1:] == 'm':
                    df['Close2'] = df['Close']
                    try:
                        df['pyDatetime'] = [datetime.strptime(dti.split('.')[0], '%Y-%m-%dT%H:%M:%S') for
                                            dti in df['Datetime']]
                    except:
                        print(df['Datetime'])
                        df['pyDatetime'] = [datetime.strptime(dti, '%Y-%m-%dT%H:%M:%S.%f') for dti in df['Datetime']]
                    # df['Datetime'] = [datetime.strptime(dti, '%Y-%m-%dT%H:%M:%S') for dti in df['Datetime']]
                    df = df.set_index('Datetime')
                    df.columns = pd.MultiIndex.from_tuples([(t, x) for x in ['Open', 'High', 'Low', 'Close',
                                                                             'Volume', 'Close2', 'pyDatetime']])
                data_list.append(df)
                # p_bar.update(1)
                break
            except Exception as exc:
                str_exc = f'fp_get_historical_data down {exc} trace {traceback.print_exc()}'
                if str_exc.find('Datetime') == -1 and str_exc.find('columns') == -1 and \
                        str_exc.find('KeyError') == -1 and str_exc.find(t) == -1:
                    print(str_exc)
                if i == 9:
                    failed_downloads.append(t)

    for ticker in tickers:
        sch_05.add_job(down, args=[ticker, interval], misfire_grace_time=1200, max_instances=40)
        time.sleep(0.08)

    p_bar_n_list = []
    while (data_list_len := len(data_list)) < len(tickers) - len(failed_downloads):
        time.sleep(0.5)
        p_bar_n_list.append(data_list_len)
        if len(p_bar_n_list) > 120 and p_bar_n_list[-120] == p_bar_n_list[-1]:
            break

    # p_bar.close()
    # for failed_download in failed_downloads:
    #     if failed_download in self.scripts_btst_dict['failed_downloads_dict'][interval]:
    #         self.scripts_btst_dict['failed_downloads_dict'][interval][failed_download] += 1
    #     else:
    #         self.scripts_btst_dict['failed_downloads_dict'][interval][failed_download] = 1
    if not data_list:
        print(failed_downloads)
        return pd.DataFrame()
    final_data = pd.concat(data_list, axis=1)
    print('\n', failed_downloads, '\n', len(final_data.columns) / 6)
    data_list = []
    # re_down = []

    # for ticker in tickers:
    #     if ticker in failed_downloads or len(final_data[ticker].dropna()) < len(final_data):
    #         re_down.append(ticker)
    # yf_data = yf.download('.NS '.join(re_down) + '.NS', period='320d', group_by='ticker')
    # yf_data.columns = pd.MultiIndex.from_tuples([(x[0].split('.')[0], x[1]) for x in yf_data.columns])
    # yf_data.to_pickle('1cr_TO_5p_data_3_1.pkl')
    # final_data.update(yf_data)
    for obj_to_del in [p_bar_n_list, df_5p, tickers_code]:
        del obj_to_del
    inform(f'Finished downloading Historical Data from FivePaisa {interval} {period} {dt} {len(final_data)}'
           f' {len(final_data.columns) / 6}')
    return final_data


def tt_init():
    a_loop = work_info_dict['a_loop']
    if a_loop.is_closed():
        work_info_dict['a_loop'] = a_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(a_loop)
    work_info_dict['tt_client'] = TelegramClient(StringSession(
        all_info_dict["telethon_creds_1"]['telethon_session_str']),
        all_info_dict["telethon_creds_1"]['telegram_api_id'],
        all_info_dict["telethon_creds_1"]['telegram_api_hash'],
        loop=a_loop)
    # work_info_dict['tt_client'] = TelegramClient('CJ',
    #     all_info_dict["telethon_creds_1"]['telegram_api_id'],
    #     all_info_dict["telethon_creds_1"]['telegram_api_hash'],
    #     loop=a_loop)
    work_info_dict['tt_client'].start()
    # send_to_slack('#imp_info', work_info_dict['tt_client'].get_dialogs())
    sch_03.add_job(tt_process_ids, misfire_grace_time=60)


def tt_process_ids():
    async def process_ids():
        a_loop = work_info_dict['a_loop']
        if a_loop.is_closed():
            work_info_dict['a_loop'] = a_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(a_loop)
        dialogs = {}
        try:
            dialogs = work_info_dict['tt_client'].get_dialogs()
        except Exception as e1:
            try:
                dialogs = await work_info_dict['tt_client'].get_dialogs()
            except Exception as e2:
                print(f'Get dialogs failed exceptions: \n{e1.with_traceback()} \n{e2.with_traceback()}')
        return dialogs

    with Kernel() as kernel:
        work_info_dict['tt_name_entity_dict'] = {x.name: x.entity.id for x in kernel.run(process_ids)}


def tt_run_function(attribute, *args, **kwargs):
    async def run_function():
        nonlocal attribute, args, kwargs
        a_loop = work_info_dict['a_loop']
        if a_loop.is_closed():
            work_info_dict['a_loop'] = a_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(a_loop)
        info = None
        function = getattr(work_info_dict['tt_client'], attribute)
        try:
            info = function(*args, **kwargs)
        except Exception as e1:
            try:
                info = await function(*args, **kwargs)
            except Exception as e2:
                print(f'Get dialogs failed exceptions: \n{e1.with_traceback(None)} \n{e2.with_traceback(None)}')
        return info

    with Kernel() as kernel:
        return kernel.run(run_function)


def tt_run_function_2(lambda_function):
    async def run_function():
        nonlocal lambda_function
        a_loop = work_info_dict['a_loop']
        if a_loop.is_closed():
            work_info_dict['a_loop'] = a_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(a_loop)
        info = None
        try:
            info = lambda_function()
        except Exception as e1:
            try:
                info = await lambda_function()
            except Exception as e2:
                print(f'Get dialogs failed exceptions: \n{e1.with_traceback(None)} \n{e2.with_traceback(None)}')
        return info

    with Kernel() as kernel:
        return kernel.run(run_function)


@app.route('/ttsm')
@anvil.server.callable
def tt_send_message(entity=1610358948, message='Howdy', reply_to_msg_id=None):
    async def send_message():
        nonlocal entity, message, reply_to_msg_id
        client = work_info_dict['tt_client']
        a_loop = work_info_dict['a_loop']
        if a_loop.is_closed():
            work_info_dict['a_loop'] = a_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(a_loop)
        message = "tt_sm5\n" + message
        reply_to_msg_id = (reply_to_msg_id + 1) if reply_to_msg_id else reply_to_msg_id
        entity = int(entity) if str(entity).isnumeric() else entity
        try:
            client.send_message(entity, message, reply_to=reply_to_msg_id)
        except Exception as e1:
            try:
                await client.send_message(entity, message + '\nawaited', reply_to=reply_to_msg_id)
            except Exception as e2:
                print(e1.with_traceback(None), e2.with_traceback(None))

    with Kernel() as kernel:
        kernel.run(send_message)


@anvil.server.callable
def tt_get_entity(entity):
    async def get_entity():
        nonlocal entity
        client = work_info_dict['tt_client']
        a_loop = work_info_dict['a_loop']
        if a_loop.is_closed():
            work_info_dict['a_loop'] = a_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(a_loop)
        info = 'None'
        try:
            info = client.get_entity(entity)
        except Exception as e1:
            try:
                info = await client.get_entity(entity)
            except Exception as e2:
                print(e1.with_traceback(None), e2.with_traceback(None))
        return info

    with Kernel() as kernel:
        return kernel.run(get_entity)


@anvil.server.callable
def send_to_slack(channel, message, host='REPLIT'):
    if 'slack_uri' not in all_info_dict:
        print(f'No slack uri hence cannot send message {channel} {message}')
        return 1
    slack_uri = all_info_dict['slack_uri']
    message = str(message).replace(' ', ' ')
    post = {"text": f"{message}", "channel": channel, "username": host,
            "title": "Yo"}
    try:
        json_data = json.dumps(post)
        req = request.Request(slack_uri, data=json_data.encode('ascii'),
                              headers={'Content-Type': 'application/json'})
        _ = request.urlopen(req)
    except Exception as em:
        prefix = f'\033[91m' + '{0:18}\033[00m : '.format('send_to_slack')
        dt = datetime.now()
        the_time = dt.strftime("%H:%M:%S.%f")
        if str(em).find('400') == -1:
            print(the_time + ' : ' + prefix + 'FAIL ' + str(em))
        return False
    return True


def misc_check_holiday():
    dt_now = datetime.now()
    if dt_now.replace(hour=8, minute=25) > dt_now >= dt_now.replace(hour=9, minute=0) and dt_now.isoweekday() < 6:
        work_info_dict['misc_holiday_check'] = False
        return 1
    elif work_info_dict['misc_holiday_check']:
        return 1

    holiday = False
    url = 'https://www.bankbazaar.com/indian-holiday/nse-holidays.html'
    for i in range(10):
        try:
            res = requests.get(url, headers=header)
            if res.status_code == 200:
                holiday_df = pd.read_html(res.text)[1]
                date_time = datetime.now()
                for dt in holiday_df['Day'].values.tolist():
                    dt_parsed = parse(dt)
                    if date_time.date() == dt_parsed.date():
                        holiday = True
                        break
        except:
            ...

    work_info_dict['misc_holiday_check'] = True
    if holiday:
        send_to_slack('#imp_info', 'Disabling Heroku as It is a holiday')
        sch_02.add_job(gs_handle_write, args=['base_spreadsheet', all_info_dict['heroku_disable_enable_range'], [[1]]],
                       misfire_grace_time=60)
        work_info_dict['holiday'] = dt_now


def misc_check_holiday_2():
    dt_now = datetime.now()
    if dt_now.replace(hour=8, minute=25) > dt_now >= dt_now.replace(hour=9, minute=0) and dt_now.isoweekday() < 6:
        work_info_dict['misc_holiday_check_2'] = False
        return 1
    elif work_info_dict['misc_holiday_check_2']:
        return 1

    holiday_trading, holiday_settlement = False, False
    info_key = ''
    if 'check_holiday_2_dict' not in work_info_dict:
        work_info_dict['check_holiday_2_dict'] = {}

    url = 'https://zerodha.com/marketintel/holiday-calendar/'
    for i in range(10):
        try:
            res = requests.get(url, headers=header)
            if res.status_code == 200:
                soup = BeautifulSoup(res.content, 'html.parser')
                uls = soup.find_all('ul', {'class': 'nostyle-list'})
                all_text = uls[0].text.replace('\t', '')
                extracted_text = '\n'.join([x for x in all_text.splitlines() if x != ''])
                for line in extracted_text.splitlines():
                    if line[:3] in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']:
                        info_key = line[5:]
                        date_time = datetime.strptime(' '.join(info_key.split()), '%d %b %Y')
                        work_info_dict['check_holiday_2_dict'][info_key] = {
                            'type': [], 'exchanges': [], 'date_time': date_time}
                    elif line in ['trading', 'settlement']:
                        work_info_dict['check_holiday_2_dict'][info_key]['type'].append(line)
                    elif line in ['nse', 'bse', 'mcx']:
                        work_info_dict['check_holiday_2_dict'][info_key]['exchanges'].append(line)
                    else:
                        work_info_dict['check_holiday_2_dict'][info_key]['info'] = line
                    if datetime.strptime(' '.join(info_key.split()), '%d %b %Y').date() == dt_now.date():
                        holiday_settlement = True
                        if 'trading' in work_info_dict['check_holiday_2_dict'][info_key]['type']:
                            holiday_trading = True
                send_to_slack('#imp_info', str(work_info_dict['check_holiday_2_dict']))
                break
        except:
            ...

    work_info_dict['misc_holiday_check_2'] = True
    if holiday_settlement:
        if holiday_trading:
            send_to_slack('#imp_info', 'Disabling Heroku as It is a holiday 2')
            sch_02.add_job(gs_handle_write, misfire_grace_time=60,
                           args=['base_spreadsheet', all_info_dict['heroku_disable_enable_range'], [[1]]])
            work_info_dict['holiday_trading'] = dt_now
            return 1
        send_to_slack('#imp_info', 'Today is a settlement holiday')


@anvil.server.callable
def misc_python_console(app_data_1=None):
    # x = self.gs_cred
    dt_now = datetime.now()
    run = True
    while run:
        msg_str = ''
        try:
            app_data = input('Please input the command to run : \n') if app_data_1 is None else app_data_1
            if app_data == 'exit':
                run = False
                continue
            var = None
            if app_data.find('=') != -1:
                indexes = [i for i, v in enumerate(app_data) if v == '=']
                if_idx = app_data.find('if')
                split = False if len(indexes) > 1 else True
                if not split:
                    for i in range(len(indexes) - 1):
                        if indexes[i] != indexes[i] + 1 or (indexes[i] > if_idx and i > 0):
                            split = True
                            break
                if split and len(indexes) > 1:
                    if app_data[indexes[1] - 1] not in [' ', '='] and app_data[indexes[1] + 1] not in [' ', '=']:
                        split = False
                if split:
                    var, app_data = app_data.split('=', 1)
                    var, app_data = var.strip(), app_data.strip()
                # print(var, type(var))
            _locals = locals()
            do_exec = False
            try:
                time_1 = time.time()
                eval_out = eval(app_data, globals(), _locals)
                if var is not None:
                    globals()[var] = eval_out
                    # print(globals()[var])
                    app_data = f'{var} = {app_data}'
                msg_str += f'{eval_out}\npyc_time : {time.time() - time_1:0.5f} secs\n'
            except:
                do_exec = True
            if do_exec:
                time_1 = time.time()
                exec_out = exec(app_data, globals(), _locals)
                if var is str:
                    globals()[var] = exec_out
                    app_data = f'{var} = {app_data}'
                msg_str += f'{exec_out}\npyc_time : {time.time() - time_1:0.5f} secs\n'
        except Exception as e:
            msg_str += f'{dt_now} : {e.with_traceback(None)}\n'
        if app_data_1:
            run = False
            return msg_str
        else:
            print(msg_str)


def misc_generate_stocks_report_yf_2():
    inform('Entered!')
    t1 = time.time()
    base_url = 'https://finance.yahoo.com/quote/'
    # https://in.finance.yahoo.com/quote/GPPL.NS/key-statistics?p=GPPL.NS
    # tickers = [x[0] for x in gs_cred.open_by_key(base_spreadsheet).values_batch_get('NSE!U4:U3000')['valueRanges']
    # [0]['values'][:200]]
    fp_df = fp_get_ticker_codes()
    tickers = [v5p[3].strip() for v5p in fp_df.values.tolist() if v5p[0] == 'N' and v5p[1] == 'C'
            and v5p[4] in ['BE', 'BZ', 'E1', 'EQ', 'IV', 'IT']]
    # self.gs_cred.open_by_key(self.base_spreadsheet).values_clear('NSE!AA2:CH3000')
    # cols = ['Script', 'Market cap (intra-day) 5', 'Enterprise value 3', 'Trailing P/E', 'Forward P/E 1',
    #         'PEG Ratio (5 yr expected) 1', 'Price/sales (ttm)', 'Price/book (mrq)', 'Enterprise value/revenue 3',
    #         'Enterprise value/EBITDA 6', 'Beta (5Y monthly)', '52-week change 3', 'S&P500 52-week change 3',
    #         '52-week high 3', '52-week low 3', '50-day moving average 3', '200-day moving average 3',
    #         'Avg vol (3-month) 3', 'Avg vol (10-day) 3', 'Shares outstanding 5', 'Float', '% held by insiders 1',
    #         '% held by institutions 1', 'Shares short 4', 'Short ratio 4', 'Short % of float 4',
    #         'Short % of shares outstanding 4', 'Shares short (prior month ) 4', 'Forward annual dividend rate 4',
    #         'Forward annual dividend yield 4', 'Trailing annual dividend rate 3',
    #         'Trailing annual dividend yield 3',
    #         '5-year average dividend yield 4', 'Payout ratio 4', 'Dividend date 3', 'Ex-dividend date 4',
    #         'Last split factor 2', 'Last split date 3', 'Fiscal year ends', 'Most-recent quarter (mrq)',
    #         'Profit margin', 'Operating margin (ttm)', 'Return on assets (ttm)', 'Return on equity (ttm)',
    #         'Revenue (ttm)', 'Revenue per share (ttm)', 'Quarterly revenue growth (yoy)', 'Gross profit (ttm)',
    #         'EBITDA', 'Net income avi to common (ttm)', 'Diluted EPS (ttm)', 'Quarterly earnings growth (yoy)',
    #         'Total cash (mrq)', 'Total cash per share (mrq)', 'Total debt (mrq)', 'Total debt/equity (mrq)',
    #         'Current ratio (mrq)', 'Book value per share (mrq)', 'Operating cash flow (ttm)',
    #         'Levered free cash flow (ttm)']
    # cols_1 = ['Script', 'Market Cap (intraday)', 'Enterprise Value', 'Trailing P/E',
    #    'Forward P/E', 'PEG Ratio (5 yr expected)', 'Price/Sales (ttm)',
    #    'Price/Book (mrq)', 'Enterprise Value/Revenue',
    #    'Enterprise Value/EBITDA', 'Beta (5Y Monthly)', '52-Week Change 3',
    #    'S&P500 52-Week Change 3', '52 Week High 3', '52 Week Low 3',
    #    '50-Day Moving Average 3', '200-Day Moving Average 3',
    #    'Avg Vol (3 month) 3', 'Avg Vol (10 day) 3', 'Shares Outstanding 5',
    #    'Implied Shares Outstanding 6', 'Float 8', '% Held by Insiders 1',
    #    '% Held by Institutions 1', 'Shares Short 4', 'Short Ratio 4',
    #    'Short % of Float 4', 'Short % of Shares Outstanding 4',
    #    'Shares Short (prior month ) 4', 'Forward Annual Dividend Rate 4',
    #    'Forward Annual Dividend Yield 4', 'Trailing Annual Dividend Rate 3',
    #    'Trailing Annual Dividend Yield 3', '5 Year Average Dividend Yield 4',
    #    'Payout Ratio 4', 'Dividend Date 3', 'Ex-Dividend Date 4',
    #    'Last Split Factor 2', 'Last Split Date 3', 'Fiscal Year Ends',
    #    'Most Recent Quarter (mrq)', 'Profit Margin', 'Operating Margin (ttm)',
    #    'Return on Assets (ttm)', 'Return on Equity (ttm)', 'Revenue (ttm)',
    #    'Revenue Per Share (ttm)', 'Quarterly Revenue Growth (yoy)',
    #    'Gross Profit (ttm)', 'EBITDA', 'Net Income Avi to Common (ttm)',
    #    'Diluted EPS (ttm)', 'Quarterly Earnings Growth (yoy)',
    #    'Total Cash (mrq)', 'Total Cash Per Share (mrq)', 'Total Debt (mrq)',
    #    'Total Debt/Equity (mrq)', 'Current Ratio (mrq)',
    #    'Book Value Per Share (mrq)', 'Operating Cash Flow (ttm)',
    #    'Levered Free Cash Flow (ttm)']
    final_df = pd.DataFrame(columns=['Script', '3M AVG', 'CtoC'])
    df_array = []
    scripts_info = []
    jobs_running = 0
    job_completed_count = 0
    row_write_no = 3

    def update_gsheet(opt=0, to_write='data'):
        nonlocal final_df, scripts_info, df_array, row_write_no
        grange = f'NSE YF Temp!C{row_write_no}' if opt == 1 else 'NSE YF!T4'
        if to_write == 'data':
            try:
                temp_df = pd.concat([pd.DataFrame(columns=['Script', '3M AVG', 'CtoC']), pd.concat(df_array)])
            except Exception as exc:
                inform(f'Exception in update gsheet : {exc.with_traceback(None)}')
                return 1
            temp_df.sort_values('Script', inplace=True)
            write_dict = dict(zip(['values'], [temp_df.fillna("-").values.tolist()]))
            gs_dict['gs_auth_cred_2'].open_by_key(gs_dict['rough_spreadsheet']).values_update(
                grange, params=dict_params, body=write_dict)
            write_dict_len = len(write_dict['values'])
            # final_df = final_df.drop(final_df.index[list(range(write_dict_len))])
            del df_array[:len(temp_df)]
            # final_df.to_pickle('scripts_statistics.pkl')
            # inform(f'Wrote to gsheet! : final_df {len(final_df)} : write_dict {write_dict_len} : '
            #        f'total scripts {len(scripts_info)}')
            inform(f'Wrote to gsheet! : df_array {len(df_array)} : write_dict {write_dict_len} : '
                   f'total scripts {len(scripts_info)}')
            row_write_no += write_dict_len
        elif to_write == 'column':
            write_dict = {'values': [final_df.columns.values.tolist()]}
            gs_dict['gs_auth_cred_2'].open_by_key(gs_dict['rough_spreadsheet']).values_update(
                'NSE YF Temp!C2', params=dict_params, body=write_dict)
            inform('Wrote to gsheet! : columns')

    async def get_data(work_queue):
        nonlocal final_df, jobs_running, scripts_info, df_array, job_completed_count
        async with aiohttp.ClientSession() as session:
            while not work_queue.empty():
                t = await work_queue.get()
                if t.find(' ') != -1:
                    return 1
                u = base_url + t + '.NS/key-statistics?'
                for tries in range(10):
                    async with session.get(u) as res:
                        try:
                            dfs = pd.read_html(await res.text())
                            temp_df = pd.DataFrame(dfs[0].iloc[:, :2].values, columns=range(2))
                            res_df = pd.concat([pd.DataFrame([['Script', t]]), temp_df, pd.concat(dfs[1:])])
                            # final_df = final_df.append(pd.DataFrame([res_df.iloc[:, 1].values.tolist()],
                            #                                         columns=res_df.iloc[:, 0].values.tolist()))
                            res_df_2 = pd.DataFrame([res_df.iloc[:, 1].values.tolist()],
                                                    columns=res_df.iloc[:, 0].values.tolist())
                            df_array.append(res_df_2)
                            scripts_info.append([t, res_df_2.iloc[-1]['Avg Vol (3 month) 3'], 0])
                            jobs_running -= 1
                            del dfs
                            del temp_df
                            del res_df
                            del res_df_2
                            job_completed_count += 1
                            if job_completed_count % 20 == 0:
                                sch_05.add_job(update_gsheet, misfire_grace_time=30)
                            if job_completed_count % 200 == 0:
                                sch_05.add_job(update_gsheet, args=[1, 'column'], misfire_grace_time=30)
                            break
                        except Exception as exc:
                            # print(t, u, exc.with_traceback(None))
                            if tries == 9:
                                print(f'Get data failed for {t} {u} {exc.with_traceback(None)}')
                                jobs_running -= 1
                            await asyncio.sleep(5)
                            continue
                        # the_cols = res_df[0].tolist()
                        # del res_df[0]
                        # res_df = res_df.transpose()
                        # res_df.columns = the_cols

    async def enqueue_work():
        work_queue = asyncio.Queue()
        for scrip in tickers:
            await work_queue.put(scrip)

        # with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(get_data(work_queue)),
            asyncio.create_task(get_data(work_queue)),
            asyncio.create_task(get_data(work_queue)),
            asyncio.create_task(get_data(work_queue)),
            asyncio.create_task(get_data(work_queue)),
        )

    sch_05.add_job(update_gsheet, 'interval', seconds=60, id='update_gsheet', misfire_grace_time=40)
    asyncio.run(enqueue_work())
    tickers_len = len(tickers)
    # for idx, ticker in enumerate(tickers):
    #     if (idx % 20 == 0 and idx > 0) or idx == tickers_len - 1:
    #         print(f'stocks_report_yf : get_data : {idx} : {tickers_len}')
    #         if sch02.get_job('report_yf'):
    #             sch02.remove_job('report_yf')
    #         if idx == 200:
    #             sch01.add_job(update_gsheet, args=[1, 'column'], misfire_grace_time=120)
    #     url = base_url + ticker + '.NS/key-statistics?'
    #     # for tries in range(10):
    #     #     res = requests.get(url, headers=header)
    #     #     if res.status_code == 200:
    #     #         try:
    #     #             dfs = pd.read_html(res.text)
    #     #             temp_df = pd.DataFrame(dfs[0].iloc[:, :2].values, columns=range(2))
    #     #             res_df = pd.concat([pd.DataFrame([['Script', ticker]]), temp_df, pd.concat(dfs[1:])])
    #     #             # final_df = final_df.append(pd.DataFrame([res_df.iloc[:, 1].values.tolist()],
    #     #             #                                         columns=res_df.iloc[:, 0].values.tolist()))
    #     #             res_df_2 = pd.DataFrame([res_df.iloc[:, 1].values.tolist()],
    #     #                                     columns=res_df.iloc[:, 0].values.tolist())
    #     #             df_array.append(res_df_2)
    #     #             scripts_info.append([ticker, res_df_2.iloc[-1]['Avg Vol (3 month) 3']])
    #     #             break
    #     #         except Exception as exc:
    #     #             # print(t, u, exc.with_traceback(None))
    #     #             if tries == 9:
    #     #                 print(f'Get data failed for {ticker} {url} {exc.with_traceback(None)}')
    #     sch03.add_job(get_data, args=[ticker, url], misfire_grace_time=60)
    #     jobs_running += 1
    #     while jobs_running >= 20:
    #         time.sleep(5)
    update_gsheet()
    time.sleep(10)

    while jobs_running > 0:
        time.sleep(1)

    t2 = time.time()
    inform(f'Part 1 Complete! {t2 - t1:0.2f} secs')
    # final_df.to_pickle('scripts_statistics.pkl')
    sch_05.add_job(update_gsheet, args=[1, 'column'], misfire_grace_time=120)
    # df = final_df[final_df['Script'] != '-']
    # col_name = 'Avg Vol (3 month) 3'
    # volumes = df[col_name].values
    # df = pd.DataFrame(list(scripts_info.keys()), columns=['Script'])
    # volumes = list(scripts_info.values())
    df = pd.DataFrame(scripts_info, columns=['Script', '3M AVG', 'CtoC'])
    df = df.set_index('Script')
    # volumes = [x[1] for x in scripts_info]
    # actual_volumes = []
    for idx, v in enumerate(scripts_info):
        if (idx % 50 == 0 and idx > 0) or idx == tickers_len - 1:
            inform(f'parse volumes : {idx} : {tickers_len}')
        if v[1] in ['-', '', ' '] or (type(v[1]) is float and math.isnan(v)):
            # actual_volumes.append(0)
            df.loc[v[0], '3M AVG'] = 0
            continue
        elif type(v[1]) is float or type(v[1]) is int:
            # actual_volumes.append(int(v))
            df.loc[v[0], '3M AVG'] = 0
            continue
        try:
            vol_temp = float(v[1][:-1] if v[1][-1] == 'M' or v[1][-1] == 'k' else v)
        except Exception as exc:
            inform(f'parse volumes : {idx} : {v} : {type(v[1])} {exc}')
            # actual_volumes.append(0)
            df.loc[v[0], '3M AVG'] = 0
            continue
        multiplier = 1000000 if v[1][-1] == 'M' else 1000 if v[1][-1] == 'k' else 1
        # actual_volumes.append(vol_temp * multiplier)
        df.loc[v[0], '3M AVG'] = vol_temp * multiplier
    # df.insert(1, '3M AVG', actual_volumes)
    t3 = time.time()
    inform(f'Part 2 Complete! {t3 - t2:0.2f} secs')
    # df.to_clipboard()
    # tickers = [x for x in df['Script'].values.tolist()]
    tickers = [x[0] for x in scripts_info]
    # circuit_to_circuit = []
    sch_05.remove_job('update_gsheet')
    # d1 = yf.download(' '.join(tickers), period='10d', interval='60m', group_by='ticker')
    # d1 = self.fp_get_historical_data(tickers, period='10d', interval='60m')
    counter_1 = 0
    for i in range(0, len(df), 10):
        if (i % 50 == 0 and i > 0) or i == tickers_len - 1:
            inform(f'circuit to circuit : {i} : {tickers_len}')
        try:
            d1 = fp_get_historical_data(tickers[i:i+10], period='10d', interval='60m')
        except:
            d1 = []
        for ticker in tickers[i:i+10]:
            try:
                data = d1[ticker]
                counter_1 += 1
            except:
                # circuit_to_circuit.append(0)
                # df.loc[v[0], 'CtoC'] = 0
                if ticker == tickers[i]:
                    print(tickers[i:i+10])
                    print(d1.columns)
                continue
            df.loc[ticker, 'CtoC'] = 1 if len(data[data['High'] == data['Low']]) >= 10 or len(data) == 0 else 0
        # print(circuit_to_circuit[-10:], counter_1)
    # df.insert(2, 'CtoC', circuit_to_circuit)
    # df.to_pickle('scripts_statistics.pkl')
    # final_df = df
    sch_05.add_job(update_gsheet, args=[1, 'column'], misfire_grace_time=120)
    # df.sort_values('Script', inplace=True)
    write_dict = dict(zip(['values'], [df.fillna("-").values.tolist()]))
    gs_dict['gs_auth_cred_2'].open_by_key(gs_dict['rough_spreadsheet']).values_update('NSE YF!U4', params=dict_params,
                                                                   body=write_dict)
    # update_gsheet()
    t4 = time.time()
    inform(f'Part 3 Complete! {t4 - t3:0.2f} secs')
    inform(f'Total time {t4 - t1:0.2f} secs')


def inform(msg):
    print(msg)


def misc_add_function(function_as_str, function_name):
    f_code = compile(function_as_str, function_name, 'exec')
    func_dict.update({function_name: FunctionType(f_code.co_consts[0], globals(), function_name)})


sch_01.add_job(init, misfire_grace_time=120)
app.run(host='0.0.0.0', port=8080)
