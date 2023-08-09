#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
from copy import deepcopy
from threading import Thread

from autk.mapper.map import ChartMap,ApArMap
from autk.reader.base.xlsht import XlSheet

class CalChart(XlSheet):
    '''
    CalChart must have a map to indicate its column-structure;
    If xlmap is passed None,then columns will be:
        ['entity','accid','accna','start_bal_type','start','dr_amount','cr_amount','end_bal_type','end','check_balance']
    '''
    def __init__(
        self,
        shmeta=[None,'sheet0',0],
        # structure of the table is less important than meta information.
        xlmap=None,
        #  key_index=['accid'], #['凭证日期','字','号'],
        #  key_name='accid',
        #  drcrdesc=['dr_amount','cr_amount'],#['借方发生','贷方发生'],
        #  accid_col='accid',#科目编号,
        #  accna_col='accna',#科目名称,
        #  date_col='date',#凭证日期',
        #  date_split_by=r'-',
        #  top_accid_len=4,
        #  accna_split_by=r'/',
        #  use_map=False,
        keep_meta_info=True,
    ):
        if xlmap is None:
            xlmap=ChartMap.from_list([
                'entity','accid','accna','start_bal_type','start','dr_amount','cr_amount','end_bal_type','end','check_balance'
            ])
        else:
            pass
        XlSheet.__init__(
            self,
            shmeta=shmeta,
            xlmap=xlmap,
            use_map=True,
            keep_meta_info=keep_meta_info,
            key_index=[xlmap.accid_col],
            key_name=xlmap.accid_col,
        )
        self.set_key_cols(
            drcrdesc=xlmap.drcrdesc,
            accid_col=xlmap.accid_col,
            accna_col=xlmap.accna_col
        )
        self.set_top_acct(
            xlmap.top_accid_len,
            xlmap.accna_split_by
        )
        pass
    def set_key_cols(
        self,
        drcrdesc=['借方发生金额','贷方发生金额'],
        accid_col='科目编号',
        accna_col='科目全路径'
    ):
        if self.use_map==True:
            setattr(self,'drcrdesc',self.xlmap.drcrdesc)
            setattr(self,'accid_col',self.xlmap.accid_col)
            setattr(self,'accna_col',self.xlmap.accna_col)
        else:
            setattr(self,'drcrdesc',drcrdesc)
            setattr(self,'accid_col',accid_col)
            setattr(self,'accna_col',accna_col)
        key_cols=[
            self.key_name,
            self.accid_col,
            self.drcrdesc[0],
            self.drcrdesc[1],
            self.accna_col
        ]
        setattr(self,'key_cols',key_cols)
        def cal_drcr_apply_func(row_series):
            dr_amount=row_series[self.drcrdesc[0]]
            cr_amount=row_series[self.drcrdesc[1]]
            if (
                isinstance(dr_amount,(int,float)) 
                and isinstance(cr_amount,(int,float))
            ):
                drcr=dr_amount-cr_amount
            else:
                drcr=0.0
            return drcr
        if self.shmeta[0] is not None:
            self.apply_df_func(cal_drcr_apply_func,1,'drcr')
    def __parse_acctmap(self):
        '''
        self.set_key_cols() must be called before this.
        '''
        self.acctmap={}
        if self.shmeta[0] is not None:
            map_df=self.data[[self.accid_col,self.accna_col]]
            map_dict=zip(
                list(map_df[self.accid_col]),
                list(map_df[self.accna_col])
            )
            self.acctmap.update(map_dict)
            self.acctmap_invert=dict(
               zip(
                   self.acctmap.values(),
                   self.acctmap.keys()
               )
            )
    def set_top_acct(
        self,
        top_accid_len=4,
        accna_split_by=r'/'
    ):
        setattr(self,'top_accid_len',top_accid_len)
        setattr(self,'accna_split_by',accna_split_by)
        if self.shmeta[0] is not None:
            self.change_float_to_str(self.accid_col)
            self.change_float_to_str(self.accna_col)
            def top_accid_apply_func(row_series):
                accid=row_series[self.accid_col]
                top_accid=accid[0:self.top_accid_len]
                return top_accid
            def top_accna_apply_func(row_series):
                accna=row_series[self.accna_col]
                top_accna=accna.split(self.accna_split_by)[0]
                top_accna=str(top_accna)
                return top_accna
            self.apply_df_func(top_accid_apply_func,1,'top_accid')
            self.apply_df_func(top_accna_apply_func,1,'top_accna')
        pass
    def calxl(self,df=None):
        pass
    def getjr(self,glid,type_xl=False):
        pass
    def __trans_accid_regex(self,accid,accurate=False):
        '''
        r'^'+accid+r'.*$' if not accurate, as default,
        or r'^\s*'+accid+r'\s*$' if accurate is True;
        Before/After the 'accid':
        If accurate, only space will be allowed;
        If not accurate, any str will be allowed;
        
        '''
        import re
        if accurate==False:
            accid_item=str(accid).join([r'^',r'.*$'])
        else:
            accid_item=str(accid).join([r'^\s*',r'\s*$',])
        # accid_item=re.sub(r'\.',r'\.',accid_item)
        return accid_item
    def __trans_accna_regex(self,accna,accurate=False):
        import re
        if accurate==False:
            accna_item=accna.join([r'^.*',r'.*$'])
        else:
            accna_item=accna.join([r'^\s',r'\s$'])
        return accna_item
    def whatna(self,accna_item):
        pass
    def whatid(self,accid_item):
        pass
    def scan_byna(self,accna_item):
        pass
    def scan_byid(self,accid_item,accid_label=None):
        pass
    def filterAcct(
        self,
        accid_item,
        side='all',
        pure=False,
        accurate=False,
        over_write=False,
        type_xl=False,
        accid_label=None
    ):
        pass
    def getAcct(
        self,
        accid_item,
        side='all',
        pure=False,
        accurate=False,
        type_xl=False,
        accid_label=None
    ):
        pass
    def getitem(
        self,
        regex_str,
        by='item_name',
        key_name=None,
        over_write=False,
        type_xl=False
    ):
        pass
    pass
