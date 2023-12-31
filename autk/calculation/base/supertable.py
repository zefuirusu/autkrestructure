#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Super Table
'''

from copy import deepcopy
from threading import Thread
from pandas import concat

from autk.gentk.funcs import f2dict,start_thread_list,save_df,get_time_str
from autk.calculation.base.table import ImmortalTable

class STB:
    def __init__(
        self,
        meta_json_path=None,
        nick_name='stb',
        great_key_name='great_key_name',
    ):
        '''
        meta_file is a json file like:
            {
                "name_1":mgl_args_tuple_1,
                "name_2":mgl_args_tuple_2,
                ...
            }
        '''
        pass
    def load_table(self):
        pass
    def clear_data(self):
        pass
    def reload(self):
        pass
    pass
