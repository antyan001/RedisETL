#!/usr/bin/python3
import os
import sys

# sys.path.insert(0, '/usr/local/lib/python3.7/dist-packages')

import json
import re
import ast
import time
import requests
import asyncio
import logging
import threading
from multiprocessing import Process
import signal
import hashlib
import subprocess
import pandas as pd
import re
import numpy as np
# from collections import Counter
from datetime import datetime
from urllib.parse import urlparse, parse_qs

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
from typing import Any, Dict, AnyStr, List, Union, Tuple
from src import PreprocPipe
from lib import SMTPMailSender, Authorization, class_method_logger, log

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
app = FastAPI(title="REST API using FastAPI Redis Async EndPoints")
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

def convert_bytes(num: np.float) -> np.float:
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

class aobject(object):
    """Inheriting this class allows you to define an async __init__.

    So you can create objects by doing something like `await MyClass(params)`
    """
    async def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        await instance.__init__(*args, **kwargs)
        return instance

    async def __init__(self):
        pass

class ServiceLogger(aobject):

    async def __init__(self):
        super().__init__()
        self.local_path = "./logs/"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = 'RedisWriter'
        self.__dict__ = self.__dict__()
        self.init_logger()

    def __dict__(self):
        return dict()

    def init_logger(self):
        self.print_log = True

        try:
            os.makedirs(self.local_path + self.currdate)
        except Exception as ex:
            print("# MAKEDIRS ERROR: \n"+ str(ex), file=sys.stderr)
            p = subprocess.Popen(['mkdir', '-p', os.path.join(self.local_path, self.currdate)],
                                 stdout=subprocess.PIPE,
                                 stdin=subprocess.PIPE)
            res = p.communicate()[0]
            # print(res)

        logging.basicConfig(filename='logs/{}/{}.log'.format(self.currdate, self.script_name),
                            level=logging.INFO,
                            format='%(asctime)s %(message)s')
        self.logger = logging.getLogger(__name__)

        log("="*54 + " {} ".format(self.currdate) + "="*54, self.logger)


def send_mail(message: str, receiver_address: str):

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


#***************************************************
##******** Initialize Asynchronous Logging *********
#***************************************************
async def apilogger():
    return await ServiceLogger()
#***************************************************
#***************************************************
loop = asyncio.get_event_loop()
apilogger_cls = loop.run_until_complete(apilogger())
#***************************************************
#***************************************************


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#################################### FASTAPI REST ENDPOINTS INITIALIZATION BLOCK #######################################
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!



#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def run_app():
    uvicorn.run("service:app", host="0.0.0.0", port=int(APP_PORT), reload=True, debug=True)
    # app.run(host="0.0.0.0", port=APP_PORT, debug=False, threaded=True, use_reloader=False)
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

@app.on_event("startup")
async def startup():
    conn_tns = "redis://:{password}@{host}:{port}/0?encoding=utf-8".format(host=HOST, port=RED_PORT, db=0, password=PASS)
    redis = await aioredis.Redis.from_url(conn_tns, max_connections=10, decode_responses=True)

@app.post('/check_fingerprint')
@limiter.limit("10/second")
async def check_fingerprint(request: Request = None,
                            response: Response = None,
                            ) -> Dict[AnyStr, AnyStr]:
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
async def register_replicas(request: Request, response: Response) -> Dict[AnyStr, AnyStr]:
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
async def unregister_replicas(request: Request, response: Response) -> Dict[AnyStr, AnyStr]:
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
async def loadDf2redis(request: Request, response: Response) -> Dict[AnyStr, AnyStr]:
    '''
    curl -i -X POST -d "ektov.a.va@sberbank.ru" http://localhost:8003/loadDf2redis
    :param request: string with user email
    :param response: Dict[AnyStr, AnyStr]
    :return: Dict[AnyStr, AnyStr]
    '''

    ## Call External Preprocessing Class
    prep = PreprocPipe()
    const_cols_dct = {}

    req = await request.body()

    if req is not None:
        params_dct = parse_qs(urlparse(req.decode("utf-8")).query)
        email_recipient = params_dct.get("email", None)
        if email_recipient is not None:  email_recipient = email_recipient[0]
        reload = params_dct.get("force_reload", None)
        if isinstance(reload, list):
            force_reload = int(reload[0])
        else:
            force_reload = -1

    ticket_files = []
    dirs = os.listdir(STREAM_DIR)
    for dir_ in dirs:
        if dir_.endswith(".ticket"):
            ticket_files.append(dir_)

    for ticket_file in ticket_files:
        if ticket_file is not None:
            with open("{}/{}".format(STREAM_DIR, ticket_file), "r") as f:
                jq = json.load(f)
                if (jq["status"] == "REGISTERED") or (force_reload > 0):
                    REPLICA_NAME__ = jq["file_name"].split(".")[0]
                    METAREPLICA_NAME__ = REPLICA_NAME__ + ".META"
                    PRIMARY_INDX = jq["prim_index"]

                    if is_csv(jq["file_name"]):
                        df = pd.read_csv(os.path.join(STREAM_DIR, jq["file_name"]))
                    elif is_xlsx(jq["file_name"]):
                        df = pd.read_excel(os.path.join(STREAM_DIR, jq["file_name"]))

                    # reduce mem usage
                    df, NA_cols = prep.reduce_mem_usage(df)
                    all_cols_tr = df.columns.values.tolist()

                    # treatnig a misleading JSON strings
                    for col in df.columns:
                        try:
                            try_first_nonna = df[col][df[col].first_valid_index()]
                            if try_first_nonna is not None:
                                if prep.is_json(try_first_nonna):
                                    df[col] = df[col].fillna('')
                                    df[col] = df[col].apply(lambda x: prep.verify_json_str(x))
                        except:
                            pass
                    # imputing strategy on numeric columns
                    imp_whole_df, miss_stat_df = prep.makeImputing(df, 'mean', all_cols=all_cols_tr)
                    # perform MinMax Scaling over numeric columns
                    fin_df = prep.makeScale(imp_whole_df, all_cols_tr, exclude_cols=None)

                    ## Transform to dict form
                    if PRIMARY_INDX.lower() not in all_cols_tr:
                        fin_df["id"] = [hashlib.md5("{}".format(x).encode("utf-8")).hexdigest() for x in range(0, len(fin_df))]
                    fin_df.set_index(PRIMARY_INDX, inplace=True)
                    df_dct = fin_df.to_dict(orient='split')

                    const_cols_dct = prep.findConstCols(fin_df)
                    const_cols_info = {}
                    for col in const_cols_dct["zero_std_cols"]:
                        const_cols_info[col] = list(fin_df[col].unique().tolist())

                    const_cols_dct_ = pd.DataFrame.from_dict(const_cols_info, orient='index').to_dict(orient='split')

                    miss_stat_dct_ = miss_stat_df.to_dict(orient='list')

                    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

                    # r.hmset(METAREPLICA_NAME__, {"INDEX_LEN": "[{}, {}]".format(np.array(df_dct["index"]).min(),
                    #                                                         np.array(df_dct["index"]).max()
                    #                                                        )
                    #                             }
                    #         )

                    r.hmset(METAREPLICA_NAME__, {"COLUMNS": json.dumps(all_cols_tr)})

                    r.hmset(METAREPLICA_NAME__, {"DF_SHAPE":               json.dumps(df.shape),
                                                 "ZERO_STD_COLS":          json.dumps(const_cols_dct_["index"]),
                                                 "ZERO_STD_COLS_VALUES":   json.dumps(const_cols_info),
                                                 "DUPLICATED_COLS":        json.dumps(const_cols_dct['duplic_cols']),
                                                 "MISSING_DATA_COL_NAMES": json.dumps(miss_stat_dct_['column_name']),
                                                 "MISSING_DATA_COUNT":    json.dumps(miss_stat_dct_['missing_count']),
                                                 }
                            )

                    for indx, items in zip(df_dct["index"], df_dct["data"]):
                        r.hmset(REPLICA_NAME__, {indx: json.dumps(items)})

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
                      
                      MISSING_DATA_VALUES: {miss_cnt}
                    
                      LOAD TIME : {dt}
                      ''' \
                    + '\n' \
                    + "#" * 100

                    # rkeys = r.hkeys(REPLICA_NAME__ + ".META")

                    email_msg = email_msg.format(db        = REPLICA_NAME__,
                                                 sh        = r.hget(METAREPLICA_NAME__, "DF_SHAPE").decode("utf-8"),
                                                 sz        = r.hget(METAREPLICA_NAME__, "FILESIZE").decode("utf-8"),
                                                 zero_std  = r.hget(METAREPLICA_NAME__, "ZERO_STD_COLS").decode("utf-8"),
                                                 dupl_cols = r.hget(METAREPLICA_NAME__, "DUPLICATED_COLS").decode("utf-8"),
                                                 miss_cols = r.hget(METAREPLICA_NAME__, "MISSING_DATA_COL_NAMES").decode("utf-8"),
                                                 miss_cnt  = r.hget(METAREPLICA_NAME__, "MISSING_DATA_COUNT").decode("utf-8"),
                                                 dt        = processed_dttm
                                                 )

            with open("{}/{}".format(STREAM_DIR, ticket_file), "w") as f:
                json.dump(jq, f, indent=4)

        ## Send Notification Email Message about succesfull data storing in Redis DB
        if email_recipient is not None:
                send_mail(email_msg, email_recipient)

    return jq

@app.get('/getRegisteredReplics')
@limiter.limit("10/second")
async def getRegisteredReplics(request: Request, response: Response) -> Dict[AnyStr, AnyStr]:
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

@app.post('/getTopNFromReplica')
@limiter.limit("10/second")
async def getTopNFromReplica(request: Request, response: Response) -> str:

    req = await request.body()

    if req is not None:
        params_dct = parse_qs(urlparse(req.decode("utf-8")).query)
        replica__ = params_dct.get("replica", None)
        if replica__ is not None:  replica__ = replica__[0]
        topn = params_dct.get("topn", None)
        if isinstance(topn, list):
            topn = int(topn[0])
        else:
            topn = 10

    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

    listvals = r.lrange(REPLICA_REGISTRY__, 0, -1)
    curr_replicas = [ele.decode("utf-8") for ele in listvals]

    out_dct = {}
    if replica__ in curr_replicas:
        replica_keys = r.hkeys(replica__)[:topn]
        for key in replica_keys:
            val = r.hget(replica__, key).decode("utf-8")
            out_dct = dict(out_dct, **{key.decode("utf-8"): json.loads(val)})

    return out_dct

@app.post('/clearRedisCache')
@limiter.limit("10/second")
async def clearRedisCache(request: Request, response: Response) -> str:

    req = await request.body()

    if req is not None:
        params_dct = parse_qs(urlparse(req.decode("utf-8")).query)
        replica__ = params_dct.get("replica", None)
        if replica__ is not None:  replica__ = replica__[0]
        remove = params_dct.get("remove", None)
        if isinstance(remove, list):
            isremove = bool(remove[0])
        else:
            isremove = False

    r = redis.Redis(host=HOST, port=RED_PORT, db=0, password=PASS, socket_timeout=SET_TIMEOUT)

    listvals = r.lrange(REPLICA_REGISTRY__, 0, -1)
    curr_replicas = [ele.decode("utf-8") for ele in listvals]

    if (replica__ in curr_replicas) and isremove:
        replica_keys = r.hkeys(replica__)
        for key in replica_keys:
            r.hdel(replica__, key.decode("utf-8"))

    return {"clear_cache": "success"}

def exit():
    time.sleep(15)
    os.kill(os.getpid(), signal.SIGKILL)

if __name__ == '__main__':
    # Register the signal handlers
    signal.signal(signal.SIGTERM, unregister_replicas)
    signal.signal(signal.SIGINT, unregister_replicas)

    first_thread = Process(target=run_app)
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









