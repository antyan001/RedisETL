import pandas as pd
import os
import re
import numpy as np
import json
from json.decoder import JSONDecodeError
from collections import Counter
from subprocess import check_output, STDOUT
from datetime import datetime
from geonamescache import GeonamesCache
from sklearn.preprocessing import StandardScaler, MaxAbsScaler, MinMaxScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, LabelEncoder

__all__ = ('PreprocPipe')

def brushing_json_str(x):
    # convert enclosing strings with double quotes and
    # treat the case if JSON holds escaped single-quotes (\')
    escaped_s_quotes = re.compile('(?<!\\\\)\'')
    none_sub = re.compile('None')
    inner_db_quotes = re.compile('(?<=[a-zA-z0-9])(\s*\"\s*)(?=[a-zA-z0-9])')
    json_str = escaped_s_quotes.sub('\"', x)
    json_str = inner_db_quotes.sub("\'", json_str)
    json_str = none_sub.sub('null', json_str)

    return json_str

class PreprocPipe():

    def __init__(self):
        # non_us_locations = []
        self.gc = GeonamesCache()
        # countries = gc.get_countries()
        # countries_by_names = gc.get_countries_by_names()
        self.us_states = self.gc.get_us_states()
        self.us_states_by_names = self.gc.get_us_states_by_names()

    def is_json(self, text: str) -> bool:
        if not isinstance(text, (str, bytes, bytearray)):
            return False
        if not text:
            return False
        text = text.strip()
        text = brushing_json_str(text)
        if text:
            if text[0] in {'{', '['} and text[-1] in {'}', ']'}:
                try:
                    json.loads(text)
                except (ValueError, TypeError, JSONDecodeError):
                    return False
                else:
                    return True
            else:
                return False
        return False

    def verify_json_str(self, x):
        json_str = brushing_json_str(x)
        try:
            json_ = json.loads(json_str)
        except Exception as ex:
            print(json_str)

        for k, v in json_.items():
            if v is not None:
                json_[k] = v.strip()

        find_city = self.gc.get_cities_by_name(json_['city'])
        if len(find_city) != 0:
            for city in find_city:
                for k, v in city.items():
                    res = v
                if res['countrycode'] == 'US':
                    try:
                        json_['state'] = self.us_states[res['admin1code']]['name']
                        json_['country'] = res['countrycode']
                    except Exception as ex:
                        print(json_str)
        else:
            if json_['state'] not in self.us_states_by_names:
                #             non_us_locations.append(json_['city'])
                json_['state'] = 'US-OUT'
                json_['country'] = 'US-OUT'

        return json.dumps(json_)


    def reduce_mem_usage(self, props):
        start_mem_usg = props.memory_usage().sum() / 1024 ** 2
        print("Memory usage: {:.1f} Mb".format(start_mem_usg))
        NAlist = []

        for col in (props.columns):
            if props[col].dtype != object:  # Exclude strings

                # Print current column type
                print("Column:", col)
                print("dtype before:", props[col].dtype)

                # make variables for Int, max and min
                IsInt = False
                mx = props[col].max()
                mn = props[col].min()

                # Integer does not support NA, therefore, NA needs to be filled
                if not np.isfinite(props[col]).all() and not col.startswith('embed_'):
                    NAlist.append(col)
                    props[col].fillna(mn - 1, inplace=True)
                elif col.startswith('embed_'):
                    props[col].fillna(0, inplace=True)

                # test if column can be converted to an integer
                asint = props[col].fillna(0).astype(np.int64)
                result = (props[col] - asint)
                result = result.sum()
                if result > -0.01 and result < 0.01:
                    IsInt = True

                # Make Integer/unsigned Integer datatypes
                if IsInt:
                    if mn >= 0:
                        if mx < 255:
                            props[col] = props[col].astype(np.uint8)
                        elif mx < 65535:
                            props[col] = props[col].astype(np.uint16)
                        elif mx < 4294967295:
                            props[col] = props[col].astype(np.uint32)
                        else:
                            props[col] = props[col].astype(np.uint64)
                    else:
                        if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                            props[col] = props[col].astype(np.int8)
                        elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                            props[col] = props[col].astype(np.int16)
                        elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                            props[col] = props[col].astype(np.int32)
                        elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                            props[col] = props[col].astype(np.int64)

                            # Make float datatypes 32 bit
                else:
                    props[col] = props[col].astype(np.float32)

                # Print new column type
                print("dtype after:", props[col].dtype)
                print()

        # Print final result
        print('-' * 50)
        mem_usg = props.memory_usage().sum() / 1024 ** 2
        print("Memory usage: {:.1f} Mb".format(mem_usg))
        print("This is {:.1f} % of the initial size".format(100 * mem_usg / start_mem_usg))
        return props, NAlist

    def makeImputing(self, df=None, strategy='mean', all_cols=None):
        fill_nan_cols = []
        fill_empty_cols = []
        im = SimpleImputer(strategy=strategy, fill_value=None, copy=False)

        df = df.replace([np.inf, -np.inf], np.nan, inplace=False)

        missing_df = df.isnull().sum(axis=0).reset_index()
        missing_df.columns = ['column_name', 'missing_count']
        missing_df = missing_df.loc[(missing_df['missing_count'] > 0) &
                                    (missing_df['missing_count'] < len(df)), :]
        missing_df = missing_df.sort_values(by='missing_count')

        for col, _type in df[missing_df.column_name.values.tolist()].items():
            # print(col, _type.dtype.kind)
            try:
                if (isinstance(df[col][df[col].first_valid_index()], np.float)) or \
                        (isinstance(df[col][df[col].first_valid_index()], int)):
                    fill_nan_cols.append(col)
                elif isinstance(df[col][df[col].first_valid_index()], str):
                    fill_empty_cols.append(col)
            except:
                pass
        if len(fill_nan_cols) > 0:
            res = im.fit_transform(df[fill_nan_cols])
            im_out_df = pd.DataFrame(res, columns=fill_nan_cols)

            part_cols = [col for col in all_cols if col not in (im_out_df.columns.values)]
            out = pd.concat([df[part_cols], im_out_df], axis=1)

        else:
            out = df

        ## Replace empty val with NULL character
        out[fill_empty_cols] = out[fill_empty_cols].fillna('')
        out= out.fillna(0.)

        ## FIll NaT
        u = out.select_dtypes(include=['datetime', 'timedelta'])
        if len(u.columns.tolist()) > 0:
            out[u.columns] = out.fillna(pd.to_datetime('today'))

        return out[all_cols], missing_df

    def makeScale(self, df, all_cols, exclude_cols):
        _df = df.copy()
        dtypes_dct = dict(df.dtypes.to_frame('dtypes').reset_index().values)
        if exclude_cols is not None:
            numerical_ix = _df.drop(exclude_cols, axis=1) \
                .select_dtypes(include=[int, np.float]).columns.values.tolist()
        else:
            numerical_ix = _df \
                .select_dtypes(include=[int, np.float]).columns.values.tolist()
        t = [('num', MinMaxScaler(), numerical_ix)]
        col_transform = ColumnTransformer(transformers=t, remainder='passthrough')
        encoder = col_transform.fit(_df)
        res = encoder.transform(_df)
        part_cols = [col for col in all_cols if col not in numerical_ix]
        reorder_df = pd.DataFrame(res, columns=numerical_ix + part_cols).astype(dtypes_dct)

        return reorder_df[all_cols]

    def findConstCols(self, df, isremove=False):
        # Columns to drop because there is no variation in training set
        #     numerical_ix  = df.select_dtypes(include=[int, np.float, bool]).columns.values.tolist()
        zero_std_cols = (df.std(axis=0) == 0.).index.tolist()
        if isremove:
            df.drop(columns=zero_std_cols, axis=1, inplace=True)
            print('Removed {} constant columns'.format(len(zero_std_cols)))

        # Removing duplicate columns
        colsToRemove = []
        colsScaned = []
        dupList = {}
        columns = df.columns
        for i in range(len(columns) - 1):
            v = df[columns[i]].values
            dupCols = []
            for j in range(i + 1, len(columns)):
                if np.array_equal(v, df[columns[j]].values):
                    colsToRemove.append(columns[j])
                    if columns[j] not in colsScaned:
                        dupCols.append(columns[j])
                        colsScaned.append(columns[j])
                        dupList[columns[i]] = dupCols
        colsToRemove = list(set(colsToRemove))
        if isremove:
            df.drop(colsToRemove, axis=1, inplace=True)
            print('Dropped {} duplicate columns'.format(len(colsToRemove)))

        return {"duplic_cols": dupList, "zero_std_cols": zero_std_cols}

