import os
import ast
import json
from urllib import request
from datetime import datetime
import anvil.server
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler as bkgSch

app = Flask('app')
all_info_dict = {}
sch_01, sch_02, sch_03, sch_04, sch_05 = bkgSch(), bkgSch(), bkgSch(), bkgSch(), bkgSch()
sch_01.start(), sch_02.start(), sch_03.start(), sch_04.start(), sch_05.start()


@app.route('/')
def hello_world():
    return 'Hello, World!'


def init():
    global all_info_dict
    all_info_dict = ast.literal_eval(os.environ['all_info_dict'])
    print(all_info_dict)
    anvil.server.connect(all_info_dict['anvil_server_uplink_url'])


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
