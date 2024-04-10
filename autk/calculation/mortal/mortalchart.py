#!/usr/bin/env python
# coding=utf-8

import datetime
from threading import Thread

from autk.gentk.funcs import start_thread_list
from autk.mapper.chmap import MchMap,ApArMap
from autk.meta.pmeta import JsonMeta
from autk.calculation.base.table import ImmortalTable
from autk.calculation.mortal.calca import CalChart

class MCH(ImmortalTable):
    '''
    Mortal Chart of Account.
        self.camap is passed as an argument only when instantiating an XlSheet object, in method of parsing xlmeta.
        self.xlmeta can be:
            1-dimension list, for example,[path,sheet_name,title];
            2-dimension list, [[path,sheet_name,title],[path,sheet_name,title],...]
            dict,for example,{path_1:[[sheet_1,0],[sheet_2,3]],path_2:[[sheet_3,2],[sheet_4,1]],....};
            directory,all xlsx files and all of their sheets will be loaded;
            json_file_path, same as dict;
            xlsx_file_path, all sheets of the xlsx file will be loaded.
    '''
    def __init__(
        self,
        xlmap:MchMap=None,
        xlmeta:JsonMeta=None,
    ):
        '''
        key_index, key_name, and chartmap, are useless, keep them as default.
        '''
        print('---Initializing MCH---')
        t_start=datetime.datetime.now()
        self.acctmap={}
        self.acctmap_invert={}
        super().__init__(
            xlmap,
            xlmeta,
        )
        t_end=datetime.datetime.now()
        t_interval=t_end-t_start
        print(
            '[{}] Initialize time spent:{}'.format(
                self.__class__.__name__,
                t_interval,
            ),
        )
        print('---MCH Initialized---')
        pass
    def collect_xl(self):
        '''
        collect `CalChart` into `self.xlset`.
        '''
        self.__clear_data()
        def __single_append(shmeta):
            xl=CalChart(
                xlmap=self.xlmap,
                shmeta=shmeta
            )
            if isinstance(self.xlmap,MchMap):
                pass
            else:
                self.xlmap=MchMap()
                self.xlmap.extend_col_list(
                    xl.xlmap.columns
                )
            self.xlset.append(xl)
            pass
        thli=[]
        for shmeta in self.xlmeta.split_to_shmeta():
            thli.append(
                Thread(
                    target=__single_append,
                    args=(shmeta,),
                    name='~'.join([
                        self.__class__.__name__,
                        'load',
                        'calchart'
                    ])
                )
            )
            continue
        start_thread_list(thli)
        self.__parse_acctmap()
        self.change_float_to_str(self.xlmap.accid_col)
        pass
    def append_xl_by_meta(self,shmeta):
        self.xlset.append(
            CalChart(
                xlmap=self.xlmap,
                shmeta=shmeta,
            )
        )
        pass
    def __parse_acctmap(self):
        self.acctmap={}
        self.acctmap_invert={}
        thli=[]
        for xl in self.xlset:
            thli.append(
                Thread(
                    target=self.acctmap.update,
                    args=(xl.acctmap,)
                )
            )
            continue
        start_thread_list(thli)
        self.acctmap_invert=dict(
            zip(
                self.acctmap.values(),
                self.acctmap.keys()
            )
        )
        pass
    def __clear_data(self):
        self.xlset=[]
        self.data=None
        self.load_count=0
        self.acctmap={}
        self.acctmap_invert={}
    def __trans_accid_regex(self,accid):
        accid_item=str(accid).join([r'^\s*',r'.*\s*$'])
        return accid_item
    def getid(self,accna):
        '''
        Match-mod regex is enabled.
        White space before and after accna does not matter.
        '''
        #  accna=self.__trans_accid_regex(accna)
        return self.vlookup(
            accna,
            self.xlmap.accna_col,
            self.xlmap.accid_col,
            if_regex=True,
            match_mode=True
        )
    def getna(self,accid):
        '''
        Match-mod regex is enabled.
        White space before and after accid does not matter.
        '''
        #  accid=self.__trans_accid_regex(accid)
        return self.vlookup(
            accid,
            self.xlmap.accid_col,
            self.xlmap.accna_col,
            if_regex=True,
            match_mode=True
        )
    def getAcct(self,accid='6001'):
        '''
        '''
        accid_item=self.__trans_accid_regex(accid)
        #  accid_item=str(accid)
        resu=self.filter(
            [[accid_item,self.xlmap.accid_col,True,True]],
            filter_type='str',
            type_xl=False,
        )
        return resu
        #  if resu.shape[0]==1:
            #  if isinstance(self.xlmap,MchMap):
                #  resu=resu.iloc[0,:]
                #  resu=list(resu)
                #  return resu
            #  else:
                #  print(
                    #  "[Warning][{}] You don't have an xlmap for MortalChartAccount!".format(
                        #  self.__class__.__name__,
                    #  )
                #  )
                #  return []
        #  else:
            #  print(
                #  '[{}|getAcct]shape of result DataFrame is:{}.',format(
                    #  self.__class__.__name__,
                    #  resu.shape
                #  )
            #  )
            #  return resu
    pass
class APAR(MCH):
    def __init__(
        self,
        xlmap,
        xlmeta,
    ):
        super().__init__(
            xlmap,
            xlmeta,
        )
    pass
if __name__=='__main__':
    pass
