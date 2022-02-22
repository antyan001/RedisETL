#!/usr/bin/python3
import os
import sys
import json
import re
import ast
import time
import requests
import threading
import signal
import subprocess

import pandas as pd
import re
import numpy as np
# from collections import Counter
from datetime import datetime

import redis
import aioredis
import uvicorn
from fastapi import Depends, FastAPI, Request, Response
# from fastapi_limiter import FastAPILimiter
# from fastapi_limiter.depends import RateLimiter
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
# from flask import Flask, request, session, jsonify
from flask import jsonify
from subprocess import check_output, STDOUT
from typing import Any, Dict, AnyStr, List, Union
from src import PreprocPipe
from lib import SMTPMailSender, Authorization, class_method_logger

os.environ["PYTHONIOENCODING"]="utf8"

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
HOST            = os.environ.get("HOST") #"65.108.56.136"
APP_PORT        = os.environ.get("APP_PORT") #8002
RED_PORT        = os.environ.get("RED_PORT") #6379
PASS            = os.environ.get("PASS")
STREAM_DIR      = os.environ.get("STREAM_DIR")
REPLICA_REGISTRY__ = os.environ.get("REPLICA_REGISTRY")
SET_TIMEOUT     = 1000
REDIS_URL = "redis://:{0}@{1}:{2}/0?encoding=utf-8".format(PASS, HOST, RED_PORT)
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# app = Flask(__name__)
# app.secret_key = 'mykey'
# app.config['SESSION_TYPE'] = 'filesystem'

limiter = Limiter(key_func=get_remote_address, storage_uri=REDIS_URL)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

#*****************************************
run = True
isApprovedSession = False
#*****************************************

JSONObject = Dict[AnyStr, Any]
JSONArray = List[Any]
JSONStructure = Union[JSONArray, JSONObject]

def is_xlsx(filename): return os.path.splitext(filename)[1].lower() == '.xlsx'
def is_xlsb(filename): return os.path.splitext(filename)[1].lower() == '.xlsb'
def is_csv(filename): return os.path.splitext(filename)[1].lower() == '.csv'

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def send_mail(message, receiver_address):

    with open("mail_settings/mail_settings.txt", "r") as f:
        settings = json.load(f)

    auth = \
    Authorization(user = settings["user"],
                  mailbox = settings["mailbox"],
                  server = settings["server"],
                  domain = None)

    mail = \
    SMTPMailSender(auth_class = auth,
                   receiver_address= receiver_address,
                   message=message)

    mail.send_mail(mail.message)

def run_app():
    uvicorn.run("service:app", host="0.0.0.0", port=int(APP_PORT), reload=True, debug=True)
    # app.run(host="0.0.0.0", port=APP_PORT, debug=False, threaded=True, use_reloader=False)

@app.post('/check_fingerprint')
@limiter.limit("10/second")
async def check_fingerprint(request: Request, response: Response):
    '''
    curl -i -H "Content-Type: application/json" -X POST -d '{"md5_key":"0h******UCt******UzGoL/bEyU******T3kd7TL3Tk"}'
                              http://localhost:8003/check_fingerprint
    :param request: Dict[AnyStr, AnyStr] MD5 Fingerprint {"md5_key": xxx}'
    :param response: Dict[AnyStr, AnyStr]
    :return: Dict[AnyStr, AnyStr]
    '''

    global isApprovedSession

    getsha_ = re.compile(r"(?:SHA256\:)([a-zA-Z0-9+_:/!\-#]+)\sroot\@kcloud\-production\-user\-136\-vm\-179")
    res = check_output(["ssh-keygen", "-lf", "/ssh/id_rsa.pub"])

    if res is not None:
        sha_ = getsha_.findall(res.decode("utf-8"))
    else:
        sha_ = None
    # return {"body": await request.body(), "json": json.loads(await request.body())}

    # query  = json.loads(await request.body()) if issubclass(request.body().__class__, str) else request.json()
    # query = {"sha_": sha_[0], "payload": json.loads(await request.body())}
    query = json.loads(await request.body())
    if query["md5_key"] == sha_[0]:
        isApprovedSession = True
        return {"auth_status": "approved", "isApprovedSession": isApprovedSession}
    else:
        isApprovedSession = False
        return {"auth_status": "denied"}

@app.get('/register_replicas')
@limiter.limit("10/second")
async def register_replicas(request: Request, response: Response):
    '''
    curl -i http://locahost:8003/register_replicas
    :param request: None
    :param response: Dict[AnyStr, AnyStr]
    :return: Dict[AnyStr, AnyStr]
    '''

    if isApprovedSession:
        ticket_files = []
        dirs = os.listdir(STREAM_DIR)
        for dir_ in dirs:
            if dir_.endswith(".ticket"):
                ticket_files.append(dir_)

        for ticket_file in ticket_files:
            if ticket_file is not None:
                with open("{}/{}".format(STREAM_DIR, ticket_file), "r") as f:
                    jq = json.load(f)
                    if jq["status"] == "READY":
                        file_path = os.path.join(STREAM_DIR, jq["file_name"])
                        if os.path.isfile(file_path):
                            file_info = os.stat(file_path)
                            SIZE = convert_bytes(file_info.st_size)

                        REPLICA_NAME__ = jq["file_name"].split(".")[0]
                        METAREPLICA_NAME__ = REPLICA_NAME__ + ".META"

                        r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

                        ## remove from DB previously inserted records
                        r.lrem(REPLICA_REGISTRY__, 0, REPLICA_NAME__)

                        rkeys = r.hkeys(REPLICA_NAME__)
                        for key in rkeys:
                            r.hdel(REPLICA_NAME__, key.decode("utf-8"))

                        rkeys = r.hkeys(METAREPLICA_NAME__)
                        for key in rkeys:
                            r.hdel(METAREPLICA_NAME__, key.decode("utf-8"))

                        r.lpush(REPLICA_REGISTRY__, REPLICA_NAME__)
                        r.hmset(METAREPLICA_NAME__, {"DB_NAME": REPLICA_NAME__, "LOAD_DTTM": jq["load_dttm"]})
                        r.hmset(METAREPLICA_NAME__, {"FROMFPATH": file_path, "FILESIZE": SIZE})

                        jq["status"] == "REGISTERED"

                with open("{}/{}".format(STREAM_DIR, ticket_file), "w") as f:
                    json.dump(jq, f, indent=4)

        return {"registry": "success"}
    else:
        return {"registry": "blocked"}

@app.get('/unregister_replicas')
@limiter.limit("10/second")
async def unregister_replicas(request: Request, response: Response):
    '''
    curl -i http://locahost:8003/unregister_replicas
    :param request: None
    :param response: Dict[AnyStr, AnyStr]
    :return: Dict[AnyStr, AnyStr]
    '''

    global run
    run = False

    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

    ticket_files = []
    dirs = os.listdir(STREAM_DIR)
    for dir_ in dirs:
        if dir_.endswith(".ticket"):
            ticket_files.append(dir_)

    for ticket_file in ticket_files:
        if ticket_file is not None:
            with open("{}/{}".format(STREAM_DIR, ticket_file), "r") as f:
                jq = json.load(f)
                if jq["status"] == "PROCESSED":

                    REPLICA_NAME__ = jq["file_name"].split(".")[0]
                    METAREPLICA_NAME__ = REPLICA_NAME__ + ".META"

                    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=500)
                    r.lrem(REPLICA_REGISTRY__, 0, REPLICA_NAME__)

                    rkeys = r.hkeys(REPLICA_NAME__)
                    for key in rkeys:
                        r.hdel(REPLICA_NAME__, key.decode("utf-8"))

                    rkeys = r.hkeys(METAREPLICA_NAME__)
                    for key in rkeys:
                        r.hdel(METAREPLICA_NAME__, key.decode("utf-8"))

                    ## reinitialize ticket
                    jq["status"] = "READY"

            with open("{}/{}".format(STREAM_DIR, ticket_file), "w") as f:
                json.dump(jq, f, indent=4)

    t = threading.Thread(target=exit)
    t.start()

    return {"unregistry": "success"}

@app.get('/stop_calling_registry')
@limiter.limit("10/second")
async def stop_calling_registry(request: Request, response: Response):
    isApprovedSession = False


@app.post('/loadDf2redis')
@limiter.limit("10/second")
async def loadDf2redis(request: Request, response: Response):
    '''
    curl -i -X POST -d "ektov.a.va@sberbank.ru" http://localhost:8003/loadDf2redis
    :param request: string with user email
    :param response: Dict[AnyStr, AnyStr]
    :return: Dict[AnyStr, AnyStr]
    '''

    ## Call External Preprocessing Class
    prep = PreprocPipe()
    const_cols_dct = {}

    ticket_files = []
    dirs = os.listdir(STREAM_DIR)
    for dir_ in dirs:
        if dir_.endswith(".ticket"):
            ticket_files.append(dir_)

    for ticket_file in ticket_files:
        if ticket_file is not None:
            with open("{}/{}".format(STREAM_DIR, ticket_file), "r") as f:
                jq = json.load(f)
                if jq["status"] == "REGISTERED":
                    REPLICA_NAME__ = jq["file_name"].split(".")[0]
                    METAREPLICA_NAME__ = REPLICA_NAME__ + ".META"

                    if is_csv(jq["file_name"]):
                        df = pd.read_csv(os.path.join(STREAM_DIR, jq["file_name"]))
                    elif is_xlsx(jq["file_name"]):
                        df = pd.read_excel(os.path.join(STREAM_DIR, jq["file_name"]))

                    # reduce mem usage
                    df, NA_cols = prep.reduce_mem_usage(df)
                    all_cols_tr = df.columns.values.tolist()
                    # imputing strategy on numeric columns
                    imp_whole_df, miss_stat_df = prep.makeImputing(df, 'mean', all_cols=all_cols_tr)
                    # perform MinMax Scaling over numeric columns
                    fin_df = prep.makeScale(imp_whole_df, all_cols_tr, exclude_cols=None)
                    df_dct = fin_df.to_dict(orient='split')

                    const_cols_dct = prep.findConstCols(fin_df)
                    const_cols_info = {}
                    for col in const_cols_dct["zero_std_cols"]:
                        const_cols_info[col] = list(df[col].unique())

                    const_cols_dct_ = pd.DataFrame.from_dict(const_cols_info, orient='index').to_dict(orient='split')

                    miss_stat_dct_ = miss_stat_df.to_dict(orient='list')

                    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

                    r.hmset(METAREPLICA_NAME__, {"DF_SHAPE": json.dumps(df.shape),
                                                 "ZERO_STD_COLS": json.dumps(const_cols_dct_["index"]),
                                                 "ZERO_STD_COLS_VALUES": json.dumps(const_cols_dct_["data"]),
                                                 "DUPLICATED_COLS": json.dumps(const_cols_dct['duplic_cols']),
                                                 "MISSING_DATA_COL_NAMES": json.dumps(miss_stat_dct_['column_name']),
                                                 "MISSING_DATA_VALUES": json.dumps(miss_stat_dct_['missing_count']),
                                                 }
                            )

                    r.hmset(REPLICA_NAME__, {"index": "[{},{}]".format(np.array(df_dct["index"]).min(),
                                                                       np.array(df_dct["index"]).max())}
                            )
                    r.hmset(REPLICA_NAME__, {"columns": json.dumps(df_dct["columns"])})
                    r.hmset(REPLICA_NAME__, {"data": json.dumps(df_dct["data"])})

                    jq["status"] = "PROCESSED"
                    processed_dttm = datetime.strftime(datetime.now(), "%Y-%d-%d %H:%M:%S")

                    email_msg = \
                      "#"*100 + '\n' \
                    + "#"*35 + " TECH INFO ABOUT REDIS STAT AND TABLES METADATA " + "#"*35 + '\n' \
                    + "#" * 100 + '\n' \
                    + '''
                      Datamart {db} with SIZE {sz} and SHAPE {sh} has been stored in REDIS DB
                    
                      COLUMNS WITH ZERO STANDARD DEVIATION: {zero_std}
                    
                      DUPLICATED COLUMNS: {dupl_cols}
                      
                      MISSING_DATA_COL_NAMES: {miss_cols}
                      
                      MISSING_DATA_VALUES: {miss_val}
                    
                      LOAD TIME : {dt}
                      ''' \
                    + '\n' \
                    + "#" * 100

                    # rkeys = r.hkeys(REPLICA_NAME__ + ".META")

                    email_msg = email_msg.format(db=REPLICA_NAME__,
                                                 sh=r.hget(METAREPLICA_NAME__, "DF_SHAPE").decode("utf-8"),
                                                 sz=r.hget(METAREPLICA_NAME__, "FILESIZE").decode("utf-8"),
                                                 zero_std=r.hget(METAREPLICA_NAME__, "ZERO_STD_COLS").decode("utf-8"),
                                                 dupl_cols=r.hget(METAREPLICA_NAME__, "DUPLICATED_COLS").decode("utf-8"),
                                                 miss_cols=r.hget(METAREPLICA_NAME__, "MISSING_DATA_COL_NAMES").decode("utf-8"),
                                                 miss_val=r.hget(METAREPLICA_NAME__, "MISSING_DATA_VALUES").decode("utf-8"),
                                                 dt=processed_dttm
                                                 )

            with open("{}/{}".format(STREAM_DIR, ticket_file), "w") as f:
                json.dump(jq, f, indent=4)

        ## Send Notification Email Message about succesfull data storing in Redis DB

        req = await request.body()

        if req is not None:
            if "email=" in req.decode("utf-8"):
                email_recipient = req.decode("utf-8").split("email=")[-1].lower()
            else:
                email_recipient = req.decode("utf-8").lower()

                send_mail(email_msg, email_recipient)

    return jq

@app.get('/getRegisteredReplics')
@limiter.limit("10/second")
async def getRegisteredReplics(request: Request, response: Response):
    '''
    curl -i  http://localhost:8003/getRegisteredReplics
    :param request: None
    :param response: Dict[AnyStr, AnyStr]
    :return: Dict[AnyStr, AnyStr]
    '''

    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

    listvals = r.lrange(REPLICA_REGISTRY__,0,-1)
    curr_replicas = [ele.decode("utf-8") for ele in listvals]
    #rkeys = r.hkeys(REPLICA_NAME)

    if len(curr_replicas) > 0:
        resp = {"LIST_OF_REGISTERED_REPLICS": ", ".join(curr_replicas)}
    else:
        resp = {"LIST_OF_REGISTERED_REPLICS": "EMPTY"}

    return resp

# @app.post('/getTopNFromReplica')
# @limiter.limit("10/second")
# async def getTopNFromReplica(request: Request, response: Response):
#     r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)
#
#     listvals = r.lrange(REPLICA_REGISTRY__, 0, -1)
#     curr_replicas = [ele.decode("utf-8") for ele in listvals]
#     # rkeys = r.hkeys(REPLICA_NAME)
#
#     if len(curr_replicas) > 0:
#         resp = {"LIST_OF_REGISTERED_REPLICS": ", ".join(curr_replicas)}
#     else:
#         resp = {"LIST_OF_REGISTERED_REPLICS": "EMPTY"}
#
#     return resp


def exit():
    time.sleep(15)
    os.kill(os.getpid(), signal.SIGKILL)

if __name__ == '__main__':
    # Register the signal handlers
    signal.signal(signal.SIGTERM, unregister_replicas)
    signal.signal(signal.SIGINT, unregister_replicas)

    first_thread = threading.Thread(target=run_app)
    # second_thread = threading.Thread(target=get_key)
    first_thread.start()
    # second_thread.start()
    # second_thread.join()
    # register_replicas()

    # run_app()

    while run:
        pass

#     try:
#     	first_thread.start()
#     	second_thread.start()
#     	second_thread.join()	
#     except ServiceExit:
#     	first_thread.shutdown_flag.set()
#     	first_thread.join()
#     	unregister_replicas()
#     	if second_thread.is_alive():
#     		second_thread.shutdown_flag.set()
#     		second_thread.join()

#     while True:
#     	listvals = r.lrange("web_app",0,-1)
#     	curr_replicas = [ele.decode("utf-8") for ele in listvals]  
#     	rkeys = r.hkeys(REPLICA_NAME)  	
#     	if (REPLICA_NAME not in curr_replicas) or (rkeys == []):
#     		first_thread.shutdown_flag.set()
#     		first_thread.join()
#     		break









