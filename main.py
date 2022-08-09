import os
import ast
import json
from urllib import request
from datetime import datetime
import gspread
import anvil.server
from flask import Flask, request as flask_request
from apscheduler.schedulers.background import BackgroundScheduler as bkgSch
from oauth2client.service_account import ServiceAccountCredentials

# https://myappcoordinator.chinmaysjoshi.repl.co
app = Flask('app')
all_info_dict = {}
work_info_dict = {}
gs_dict = {}
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
    print(all_info_dict)
    anvil.server.connect(all_info_dict['anvil_server_uplink_url'])
    gs_dict.update({'gs_cred_1': ServiceAccountCredentials.from_json_keyfile_dict(
        all_info_dict['gs_cred_1'], all_info_dict['gs_scope']),
        'gs_cred_2': ServiceAccountCredentials.from_json_keyfile_dict(
            all_info_dict['gs_cred_2'], all_info_dict['gs_scope'])})
    gs_dict.update({'gs_auth_cred_1': gspread.authorize(gs_dict['gs_cred_1']),
                    'gs_auth_cred_2': gspread.authorize(gs_dict['gs_cred_2'])})
    print(gs_dict)


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


sch_01.add_job(init, misfire_grace_time=120)
app.run(host='0.0.0.0', port=8080)
