import os
import ast
import json
import time
import asyncio
import requests
from urllib import request
from datetime import datetime
from dateutil.parser import parse
import gspread
import anvil.server
import pandas as pd
from curio import Kernel
from py5paisa import FivePaisaClient
from telethon.sessions import StringSession
from telethon.sync import TelegramClient, events
from flask import Flask, request as flask_request
from apscheduler.schedulers.background import BackgroundScheduler as bkgSch
from oauth2client.service_account import ServiceAccountCredentials

# https://myappcoordinator.chinmaysjoshi.repl.co
app = Flask('app')
all_info_dict = {}
work_info_dict = {}
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
    #  {
    #     "action":"close",
    #     "sender":"monitors",
    #     "event": {
    #         "id":"f11f8121-c949-4b59-84ba-40ef868f4d54",
    #         "name":"Queue backlogging",
    #         "title":"Current value is above threshold value 2500",
    #         "body":"Triggered with a value of 2782",
    #         "value":"2782",
    #         "timestamp":"2021-02-23T14:43:45.34205696Z",
    #         "source":"monitors.qKKbK6n4xeokNBF9GC.COUNT",
    #         "priority":0,
    #         "snoozedUntil":"0001-01-01T00:00:00Z",
    #         "state":3
    #     },
    # }
    if flask_request.method == 'POST':
        req_json = flask_request.json
        if 'event' in req_json and 'name' in req_json['event']:
            if req_json['event']['name'][:6] == 'Memory':
                work_info_dict['Heroku Secret Hamlet Memory Usage'] = req_json['event']['name']
            elif req_json['event']['name'][:3] == 'Log':
                work_info_dict['Heroku Secret Hamlet Log Size'] = req_json['event']['name']
        print("Data received from Webhook is: ", req_json)
        return "Webhook received!"


def init():
    global all_info_dict
    all_info_dict = ast.literal_eval(os.environ['all_info_dict'])
    # print(all_info_dict)
    anvil.server.connect(all_info_dict['anvil_server_uplink_url'])
    work_info_dict['misc_holiday_check'] = True
    work_info_dict['a_loop'] = asyncio.new_event_loop()
    sch_02.add_job(gs_init, misfire_grace_time=120)
    sch_02.add_job(tt_init, misfire_grace_time=120)


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

    # Batch read
    ranges = [scripts_btst_holdings_range, pfl_range, scripts_info_df_range, stocks_compare_tickers_range,
              stocks_interested_tickers_range, scripts_btst_blacklist_range, debug_docs_range]
    gs_data = gs_dict['gs_auth_cred_2'].open_by_key(gs_dict['base_spreadsheet']).values_batch_get(ranges)

    work_info_dict['dict_flags'] = {x[0]: x[1] for x in gs_data['valueRanges'][-1]['values']}


def gs_handle_write(sheet, range_str, values):
    gs_dict['gs_auth_cred_2'].open_by_key(gs_dict[sheet]).values_update(range_str, params=dict_params,
                                               body={'values': values})


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
    send_to_slack('#imp_info', str(work_info_dict['tt_client'].get_dialogs()))


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
def send_to_slack(channel, message, host='REPLIT'):
    if 'slack_uri' not in all_info_dict:
        print(f'No slack uri hence cannot send message {channel} {message}')
        return 1
    slack_uri = all_info_dict['slack_uri']
    message = message.replace(' ', ' ')
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
        sch_02.add_job(gs_handle_write, args=['base_spreadsheet', all_info_dict['heroku_enable_disable_range'], [[1]]],
                       misfire_grace_time=60)


sch_01.add_job(init, misfire_grace_time=120)
app.run(host='0.0.0.0', port=8080)
