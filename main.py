import os
import ast
from urllib import request
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


sch_01.add_job(init, misfire_grace_time=120)
app.run(host='0.0.0.0', port=8080)
