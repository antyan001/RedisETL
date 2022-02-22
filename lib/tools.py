################################## Import ##################################

import os
import sys
import warnings
import subprocess
import re
import abc
import argparse

warnings.filterwarnings('ignore')

curruser = os.environ.get('USER')

if curruser in os.listdir("/opt/workspace/"):
    sys.path.insert(0, '/opt/workspace/{user}/system/support_library/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/libs/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/system/labdata/lib/'.format(user=curruser))
else:
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))
    
import pandas as pd

pd.options.display.max_columns = 50
pd.options.display.max_rows = 5000
pd.options.display.max_colwidth = 1000

import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import inspect
from collections import Counter
import time
import re

from spark_connector import SparkConnector
from sparkdb_loader import spark
from connector import OracleDB
import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as stypes
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row
import loader
from py4j.protocol import Py4JJavaError

sys.path.append('../')
from lib.logger import *
from lib.settings import *

################################## Functions ##################################


def udf(return_type):
    return lambda func: f.udf(func, return_type)


def drop_col(df, cols: list):
    scol = sdf.columns
    final_cols = [i for i in scol if i not in cols]
    return df.select(*final_cols)


def show(self, n=10):
    return self.limit(n).toPandas()

pyspark.sql.dataframe.DataFrame.show = show


def exception_restart(num_of_attempts: int = 3, 
                      delay_time_sec: int = 10):
    
    def decorator(func):

        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(num_of_attempts):
                try:
                    func_return = func(*args, **kwargs)
                    return func_return
                except Exception as exc:
                    time.sleep(delay_time_sec)
                    last_exception = exc
                    continue    
            raise last_exception

        return wrapper

    return decorator


################################## Table functions ##################################


def load_table(schema, table, hive):
    return hive.table("{schema}.{table}".format(schema=schema, table=table))


def drop_table(schema, table, hive):
    hive.sql("drop table if exists {schema}.{table} purge" \
                .format(schema=schema, table=table))

    # subprocess.call(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', 
    #     "hdfs://clsklsbx/user/team/team_digitcamp/hive/{}".format(table.lower())])


def create_table_from_tmp(schema_out, table_out, table_tmp, hive):
    hive.sql("create table {schema_out}.{table_out} select * from {table_tmp}" \
            .format(schema_out=schema_out, table_out=table_out, table_tmp=table_tmp))

    # subprocess.call(['hdfs', 'dfs', '-chmod', '-R', '777', 
    #     "hdfs://clsklsbx/user/team/team_digitcamp/hive/{}".format(table_out.lower())])


def create_table_from_df(schema, table, df, hive):
    df.registerTempTable(table)
    create_table_from_tmp(schema, table, table, hive)


def insert_into_table_from_df(schema, table, df, hive):
    df.registerTempTable(table)
    hive.sql('insert into table {schema}.{table} select * from {table}' \
                .format(schema=SBX_TEAM_DIGITCAMP, table=table))


def rename_table(schema, old_name, new_name, hive):
    hive.sql("alter table {schema}.{old_name} rename to {schema}.{new_name}" \
                .format(schema=schema, old_name=old_name, new_name=new_name))
    
    
