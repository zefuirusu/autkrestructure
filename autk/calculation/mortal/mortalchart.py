#!/usr/bin/env python
# coding=utf-8
import datetime
from threading import Thread


#  from autk.parser.entry import Acct
from autk.gentk.funcs import transType,get_time_str
from autk.mapper.chmap import MchMap,ApArMap
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
        xlmeta=None,
        common_title=0,
        #  accid_col='科目编号',
        #  accna_col='科目名称',
        #  drcrdesc=['借方发生','贷方发生'],
        #  top_accid_len=4,
        xlmap=None,
        #  use_map=True,
        auto_load=False,
        key_cols=[],
        nick_name='mca'
    ):
        '''
        key_index, key_name, and chartmap, are useless, keep them as default.
        '''
        print('---Initializing MCA---')
        t_start=datetime.datetime.now()
        if nick_name=='mca':
            self.name=nick_name+'_'+get_time_str()
        else:
            self.name=nick_name
        if xlmap is None:
            xlmap=ChartMap.from_list([
                'entity','accid','accna','start_bal_type','start','dr_amount','cr_amount','end_bal_type','end','check_balance'
            ])
        else:
            pass
        self.xlmeta=xlmeta
        ImmortalTable.__init__(
            self,
            xlmeta,
            common_title=common_title,
            xlmap=xlmap,
            use_map=True,
            auto_load=auto_load,
            keep_meta_info=True,
            key_index=xlmap.key_index,
            key_name=xlmap.key_name,
        )
        #  self.xlmap=xlmap
        self.xlset=[]
        self.load_count=0
        self.parse_meta(self.xlmeta,common_title,auto_load=auto_load)
        #  self.keep_meta_info=False
        # self.key_list=None # all possible values of key 主键的所有可能取值
        #  self.top_accid_len=xlmap.top_accid_len
        #  self.__set_drcr_accid(left_len=xlmap.top_accid_len)
        #  self.__parse_key(xlmap.accid_col,xlmap.accna_col,xlmap.drcrdesc)
        #  self.parse_meta(self.accid_col, xlmeta, common_title)
        self.change_float_to_str(xlmap.accid_col)
        #  if auto_load==True:
            #  self.load_raw_data()
        t_end=datetime.datetime.now()
        t_interval=t_end-t_start
        print(
            '[{}] Initialize time spent:{}'.format(
                self.__class__.__name__,
                t_interval,
            ),
        )
        print('---MCA Initialized---')
        pass
    def append_xl_by_meta(self,shmeta):
        self.xlset.append(
            CalChart(
                shmeta,
                xlmap=self.xlmap,
                #  key_index=self.xlmap.key_index,
                #  key_name=self.xlmap.key_name,
                #  drcrdesc=self.xlmap.drcrdesc,
                #  accid_col=self.xlmap.accid_col,
                #  accna_col=self.xlmap.accna_col,
                #  date_col=self.xlmap.date_col,
                #  date_split_by=self.xlmap.date_split_by,
                #  top_accid_len=self.xlmap.top_accid_len,
                #  accna_split_by=self.xlmap.accna_split_by,
                #  use_map=True,
                keep_meta_info=True,
            )
        )
        pass
    def __parse_key(self,accid_col,accna_col,drcrdesc):
        self.key_index=[]
        self.key_name='key_id'
        if isinstance(self.xlmap, ChartMap):
            self.accid_col=self.xlmap.accid_col
            self.accna_col=self.xlmap.accna_col
            self.drcrdesc=self.xlmap.drcrdesc
            pass
        else:
            self.accid_col=accid_col
            self.accna_col=accna_col
            self.drcrdesc=drcrdesc
            pass
        pass
    def __clear_data(self):
        ImmortalTable.__clear_data(self)
    def __set_drcr_accid_single(self,xl,left_len=None):
        if left_len is not None:
            self.top_accid_len=left_len
        # def trans_accid_to_str(row_series):
        #     accid_str=transType(row_series[self.accid_col])
        #     return accid_str
        # xl.data[self.accid_col]=xl.data[self.accid_col].apply(trans_accid_to_str,axis=1)
        # xl.data[self.accid_col]=xl.data[self.accid_col].apply(lambda row_series:transType(row_series[self.accid_col]),axis=1)
        # xl.data[self.accid_col]=xl.data[self.accid_col].apply(str,axis=1)
        def top_accid_apply_func(row_series):
            accid=transType(row_series[self.accid_col])
            to_accid_len=self.top_accid_len
            top_accid=accid[0:to_accid_len]
            return top_accid
        # def top_accna_apply_func(row_series):
        #     accna=transType(row_series[self.accna_col])
        #     top_accna=accna.split('/')[0] # 余额表没有科目全路径，直接显示末级科目的名字
        #     top_accna=str(top_accna)
        #     return top_accna
        # def cal_drcr_apply_func(row_series): # 余额表计算这个没有意义
        #     drcr=row_series[self.drcrdesc[0]]-row_series[self.drcrdesc[1]]
        #     return drcr
        # if 'drcr' not in xl.data.columns:
        #     drcr_series=xl.data.apply(cal_drcr_apply_func,axis=1)
        #     xl.data.insert(1,'drcr',drcr_series,allow_duplicates=False)
        # else:
        #     xl.data['drcr']=xl.data.apply(cal_drcr_apply_func,axis=1)
        if 'top_accid' not in xl.data.columns:
            top_accid_series=xl.data.apply(top_accid_apply_func,axis=1)
            xl.data.insert(1,'top_accid',top_accid_series,allow_duplicates=False)
        else:
            xl.data['top_accid']=xl.data.apply(top_accid_apply_func,axis=1)
        # if 'top_accna' not in xl.data.columns:
        #     top_accna_series=xl.data.apply(top_accna_apply_func,axis=1)
        #     xl.data.insert(1,'top_accna',top_accna_series,allow_duplicates=False)
        # else:
        #     xl.data['top_accna']=xl.data.apply(top_accna_apply_func,axis=1)
        pass
    def __set_drcr_accid(self,left_len=None):
        thread_list=[]
        for xl in self.xlset:
            t=Thread(target=self.__set_drcr_accid_single,args=(xl,left_len))
            thread_list.append(t)
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()
        pass
    def __str__(self):
        if self.data is not None:
            str_info=''.join([
                '='*5,'Mortal Chart of Account','='*5,'\n',
                'mem_addr: ',str(id(self)),'\n',
                'shape:\n\t',str(self.data.shape),'\n',
                'columns:\n\t',str(list(self.data.columns)),'\n',
                'accid_col:\t',str(self.accid_col),'\n',
                'accna_col:\t',str(self.accna_col),'\n',
                'drcr_col:\t',str(self.drcrdesc),'\n',
                'data source:\n\t',str(self.xlmeta),'\n',
                '='*5,'Mortal Chart of Account','='*5
                ])
        else:
            str_info=''.join([
                '='*5,'Mortal Chart of Account','='*5,'\n',
                'mem_addr: ',str(id(self)),'\n',
                'data source:\n\t',str(self.xlmeta),'\n',
                'Raw data Not loaded!','\n'
                '='*5,'Mortal Chart of Account','='*5
                ])
        return str_info
    def __trans_accid_regex(self,accid):
        accid_item=''.join([r'^\s*',str(accid),r'\s*$'])
        return accid_item
    def getid(self,accna):
        '''
        Match-mod regex is enabled.
        White space before and after accna does not matter.
        '''
        accna=self.__trans_accid_regex(accna)
        return self.vlookup(accna, self.accna_col, self.accid_col, if_regex=True, match_mode=True)
    def getna(self,accid):
        '''
        Match-mod regex is enabled.
        White space before and after accid does not matter.
        '''
        accid=self.__trans_accid_regex(accid)
        return self.vlookup(accid, self.accid_col, self.accna_col, if_regex=True, match_mode=True)
    def getAcct(self,accid='6001'):
        '''
        此方法基本完成，还需要完善的是，没有xlmap的情况下如何结构化返回结果，需要手动指定self.key_cols.
        Acct类和ChartMap类必须联动。
        '''
        accid_item=self.__trans_accid_regex(accid)
        # accid_item=accid
        resu=self.filter([[accid_item,self.accid_col,True,True]], filter_type='str')
        if resu.shape[0]==1:
            if self.xlmap is not None:
                resu=resu[self.xlmap.key_cols]
                resu=resu.iloc[0,:]
                resu=list(resu)
                a1=Acct()
                a1.accept_key_chart_row(resu)
                print(a1.__dict__)
                return resu
            else:
                print("You don't have an xlmap for MortalChartAccount!")
                return []
        else:
            print('shape of result DataFrame is: ',resu.shape)
            return resu
    pass
class APAR(MCH):
    def __init__(
            self,
            xlmeta,
            common_title=3,
            accid_col='科目编号',
            accna_col='科目名称',
            drcrdesc=['借方发生','贷方发生'],
            top_accid_len=4,
            auto_load=False,
            xlmap=ApArMap()
    ):
        MCA.__init__(
            self,
            xlmeta,
            common_title=common_title,
            accid_col=accid_col,
            accna_col=accna_col,
            drcrdesc=drcrdesc,
            top_accid_len=top_accid_len,
            auto_load=auto_load,
            xlmap=xlmap
        )
    pass
if __name__=='__main__':
    pass
