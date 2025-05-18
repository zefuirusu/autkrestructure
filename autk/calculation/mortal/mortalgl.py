#!/usr/bin/env python
# encoding = 'utf-8'

import datetime
from copy import deepcopy
from threading import Thread
from pandas import DataFrame

from autk.gentk.funcs import start_thread_list,transType,save_df,regex_filter,get_time_str
from autk.mapper.glmap import MglMap
from autk.meta.pmeta import JsonMeta
from autk.calculation.base.table import ImmortalTable
from autk.calculation.mortal.calgl import CalSheet

class MGL(ImmortalTable):
    '''
    Mortal General Ledger.
        columns must be included:
            glid,date,mark,jrid,accid,accna,dr_amount,cr_amount,drcr,item_name,note;
        1. self.xlmap is passed as an argument only when instantiating an CalSheet object, in method of parsing xlmeta.
        2. key_index and key_name are needed only when you have to combine a few columns as the KEY column. Simply loading data of ImmortalTable does not needed key_index and key_name. 
        key_index and key_name are NOT necessary for ImmortalTable, yet FAIRLY necessary for MGL.
        3. self.xlmeta can be:
            1-dimension list, for example,[path,sheet_name,title];
            2-dimension list, [[path,sheet_name,title],[path,sheet_name,title],...]
            dict,for example,{path_1:[[sheet_1,0],[sheet_2,3]],path_2:[[sheet_3,2],[sheet_4,1]],....};
            directory,all xlsx files and all of their sheets will be loaded;
            json_file_path, same as dict;
            xlsx_file_path, all sheets of the xlsx file will be loaded.
    '''
    def __init__(
        self,
        xlmap:MglMap=None,#MglMap(),
        xlmeta:JsonMeta=None,#JsonMeta({
            #  'BLANK_PATH':[['sheet',0]]
        #  }),
        unit_type=CalSheet
        ):
        print('---Initializing MGL---')
        t_start=datetime.datetime.now()
        self.gl_matrix=None
        self.acctmap={}
        self.acctmap_invert={}
        super().__init__(
            xlmap,
            xlmeta,
            unit_type
        )
        self.__parse_acctmap()
        t_end=datetime.datetime.now()
        t_interval=t_end-t_start
        print(
            '[{}] Initialize time spent:{}'.format(
                self.__class__.__name__,
                t_interval
            ),
        )
        print('---MGL Initialized---')
        pass
    def __add__(self,other):
        resu=super().__add__(other)
        resu.acctmap={**deepcopy(self.acctmap),**deepcopy(other.acctmap)}
        resu.acctmap_invert={**deepcopy(self.acctmap_invert),**deepcopy(other.acctmap_invert)}
        return resu
    def append_xl_by_meta(self,shmeta):
        '''
        '''
        #TODO not perfect when self.xlmap=None
        self.xlset.append(
            self.unit_type(
                xlmap=self.xlmap,
                shmeta=shmeta,
            )
        )
        pass
    def __parse_acctmap(self):
        '''
        '''
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
    '''
    No need to call set_key_cols,set_top_acct, and set_date in self.__init__() now;
    '''
    def __clear_data(self):
        '''
        different from super().__clear_data()
        '''
        #  super().__clear_data()
        self.xlset=[]
        self.data=None
        self.load_count=0
        self.acctmap={}
        self.acctmap_invert={}
        self.gl_matrix=None
    def blank_sht(self):
        return self.unit_type(
            xlmap=self.xlmap,
            shmeta=None
        )
        pass
    def getjr(self,glid,type_xl=False):
        '''
        self.xlmap must not be None.
        parameters:
            glid: glid;
        return:
            class autk.parser.entry.JournalRecord;
        '''
        jr_data=self.filter(
            [[glid,self.xlmap.key_name,True,True]],
            'str',
            False,
            type_xl
        )
        return jr_data
    def __trans_accid_regex(self,accid,accurate=False):
        #  '''
        #  r'^'+accid+r'.*$' if not accurate, as default,
        #  or r'^\s*'+accid+r'\s*$' if accurate is True;
        #  '''
        if accurate==False:
            accid_item=str(accid).join([r'^',r'.*$'])
        else:
            accid_item=str(accid).join([r'^\s*',r'\s*$',])
        # accid_item=re.sub(r'\.',r'\.',accid_item)
        return accid_item
    def __trans_accna_regex(self,accna,accurate=False):
        from re import compile
        if accurate==False:
            accna_item=accna.join([r'^.*',r'.*$'])
        else:
            accna_item=accna.join([r'^\s',r'\s$'])
        return compile(accna_item)
    def whatna(self,accna_str):
        '''
        parameters:
            accna_item:str
                regular expression is supported.
        return:dict
            {accid:accna,accid:accna...}
        '''
        #  accna_item=self.__trans_accna_regex(accna_str,accurate=False)
        accna_item=str(accna_str)
        accna_list=regex_filter(
            accna_item,
            self.acctmap_invert.keys(),
            match_mode=True
        )
        acct={}
        for accna in accna_list:
            accid=self.acctmap_invert[accna]
            acct.update(
                {accid:accna}
            )
        print(
            '[{}] {} may be:{}'.format(
                self.__class__.__name__,
                accna_str,
                acct,
            ),
        )
        return acct
    def whatid(self,accid_str):
        '''
        parameters:
            accid_item:str
                regular expression is supported.
        return:dict
            {accid:accna,accid:accna...}
        '''
        #  accid_item=self.__trans_accid_regex(accid_str,accurate=False)
        accid_item=str(accid_str)
        acct={}
        accid_list=regex_filter(
            accid_item,
            self.acctmap.keys(),
            match_mode=True
        )
        for accid in accid_list:
            acct.update(
                {accid:self.acctmap[accid]}
            )
            continue
        print(
            '[{}] {} may be:{}'.format(
                self.__class__.__name__,
                accid_str,
                acct
            )
        )
        return acct
    def scan_byna(self,accna_item):
        acct_cols=[
            self.xlmap.accid_col,
            self.xlmap.drcrdesc[0],
            self.xlmap.drcrdesc[1],
            self.xlmap.accna_col
        ]
        acct_json=self.whatna(accna_item)
        accid_list=list(acct_json.keys())
        resu=[]
        if len(accid_list)==0:
            print('cannot find:',accna_item)
        elif len(accid_list) >0:
            def __accurate_scan(accurate_accid,acct_json):
                accna=acct_json[accurate_accid]
                acct_df=self.getAcct(
                    accurate_accid,
                    side='all',
                    pure=True,
                    accurate=True,
                    type_xl=False,
                    accid_label=None
                )
                acct_sum=acct_df[self.xlmap.drcrdesc].sum(axis=0)
                sub_resu=[
                    accurate_accid,
                    acct_sum[self.xlmap.drcrdesc[0]],
                    acct_sum[self.xlmap.drcrdesc[1]],
                    accna
                ]
                return sub_resu
            # thread_list=[]
            for accid in accid_list:
                resu.append(
                    __accurate_scan(accid,acct_json)
                )
                continue
        else:
            pass
        resu=DataFrame(
            resu,
            columns=acct_cols
        )
        return resu
    def scan_byid(self,accid_item,accid_label=None):
        '''
        parameters:
            accid_item: str
                regular expression is supported;
        returns:
            multi-list like object;
        '''
        acct_json=self.whatid(accid_item)
        accid_list=list(acct_json.keys())
        resu=[]
        def __accurate_scan(accurate_accid,acct_json):
            drcr_data=self.getAcct(
                accurate_accid,
                side='all',
                pure=True,
                accurate=True,
                type_xl=False,
                accid_label=accid_label
            )
            drcr_sum=drcr_data[self.xlmap.drcrdesc].sum(axis=0)
            drcr_sum=list(drcr_sum)
            sub_resu=[
                accurate_accid,
                drcr_sum[0],
                drcr_sum[1],
                acct_json[accurate_accid]
            ]
            return sub_resu
        if len(accid_list)==0:
            print('cannot find:',accid_item)
            pass
        elif len(accid_list) >0:
            for accid in accid_list:
                sub_resu=__accurate_scan(accid,acct_json)
                resu.append(sub_resu)
        else:
            pass
        # print('\nscan:\n',acct_json)
        resu=DataFrame(
            resu,
            columns=[
                self.xlmap.accid_col,
                self.xlmap.drcrdesc[0],
                self.xlmap.drcrdesc[1],
                self.xlmap.accna_col
            ]
        )
        return resu
    def filterAcct(
        self,
        *args,
        **kwargs,
    ):
        '''
        If over_write is True, result data will replace
        self.data;
        `side` can be 'all/dr/cr';
        parameters:
            accid_item,
            side='all',
            pure=False,
            accurate=False,
            over_write=False,
            type_xl=False,
            accid_label=None
        '''
        return self.apply_xl_collect_df(
            'filterAcct',
            *args,
            **kwargs,
        )
        ### the following can be replaced by `self.apply_xl_collect_df()`;
        #  accid_item=self.__trans_accid_regex(accid_item,accurate=accurate)
        #  if pure==True:
            #  if side=='all':
                #  resu=self.filter(
                    #  [[accid_item,self.xlmap.accid_col,True,True]],
                    #  filter_type='str',
                    #  over_write=over_write,
                    #  type_xl=type_xl
                #  )
                #  pass
            #  elif side=='dr':
                #  resu=self.filter(
                    #  {
                        #  'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        #  'number':[[self.xlmap.drcrdesc[0],'<>',0]]
                    #  },
                    #  filter_type='adv',
                    #  over_write=over_write,
                    #  type_xl=type_xl
                #  )
                #  pass
            #  elif side=='cr':
                #  resu=self.filter(
                    #  {
                        #  'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        #  'number':[[self.xlmap.drcrdesc[1],'<>',0]]
                    #  },
                    #  filter_type='adv',
                    #  over_write=over_write,
                    #  type_xl=type_xl
                #  )
                #  pass
            #  else:
                #  resu=None
                #  print('[Note:MGL] Invalid argument, filter None will not overwrite.')
                #  pass
            #  pass
        #  elif pure==False:
            #  if side=='all':
                #  resu=self.filter(
                    #  [[accid_item,self.xlmap.accid_col,True,True]],
                    #  filter_type='str',
                    #  over_write=False,
                    #  type_xl=False
                #  )
                #  pass
            #  elif side=='dr':
                #  resu=self.filter(
                    #  {
                        #  'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        #  'number':[[self.xlmap.drcrdesc[0],'<>',0]]
                    #  },
                    #  filter_type='adv',
                    #  over_write=False,
                    #  type_xl=False
                #  )
                #  pass
            #  elif side=='cr':
                #  resu=self.filter(
                    #  {
                        #  'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        #  'number':[[self.xlmap.drcrdesc[1],'<>',0]]
                    #  },
                    #  filter_type='adv',
                    #  over_write=False,
                    #  type_xl=False
                #  )
                #  pass
            #  else:
                #  resu=None
                #  print(
                    #  '[Note:MGL] Invalid argument, got None; not overwritten;'
                #  )
                #  pass
            #  if resu is not None:
                #  acct_glid_list=list(
                        #  set(resu[self.xlmap.key_name])
                #  )
            #  else:
                #  acct_glid_list=[]
            #  if len(acct_glid_list)==0:
                #  resu=None
            #  else:
                #  resu=self.filter_list(
                    #  acct_glid_list,
                    #  search_col=self.xlmap.key_name,
                    #  over_write=over_write,
                    #  type_xl=type_xl
                #  )
            #  pass
        #  if resu is None:
            #  resu=DataFrame([],columns=self.xlset[0].data.columns)
            #  return resu
        #  else:
            #  return resu
        ### the following can be replaced by `self.apply_xl_collect_df()`;
        pass
    def getAcct(
        self,
        *args,
        **kwargs,
    ):
        '''
        parameters:
            accid_item,
            side='all',
            pure=False,
            accurate=False,
            type_xl=False,
            accid_label=None
        '''
        return self.apply_xl_collect_df(
            'getAcct',
            *args,
            **kwargs,
        )
    def getitem(
        self,
        *args,
        **kwargs,
    ):
        '''
        Get full Journal Entries according to 'item_name' matched by regex_str by search-mode;
        parameters:
            regex_str,
            by='item_name',
            key_name=None,
            over_write=False,
            type_xl=False
        '''
        return self.apply_xl_collect_df(
            'getitem',
            *args,
            **kwargs,
        )
        #  if key_name is None:
            #  key_name=self.xlmap.key_name
        #  item_data=self.filter(
            #  [[regex_str,by,True,False]],
            #  'str',
            #  over_write=False,
            #  type_xl=False
        #  )
        ## item_data=self.xl2df(item_data)
        #  key_list=list(
            #  item_data[self.xlmap.key_name].drop_duplicates()
        #  )
        #  resu=self.filter_list(
            #  key_list,
            #  key_name,
            #  over_write=over_write,
            #  type_xl=type_xl
        #  )
        #  return resu
    def single_acct_analysis(
        self,
        accid,
        save=False,
        savepath='./',
        nick_name='single_acct',
        label=None
    ):
        print('='*5,accid,'='*12)
        acct_all=self.getAcct(
            accid,
            side='all',
            pure=False,
            accurate=False,
            type_xl=False,
            accid_label=label
        )
        print('acct\t%s:\n'%str(accid),acct_all)
        acct_dr=self.getAcct(
            accid,
            side='dr',
            pure=False,
            accurate=False,
            type_xl=False,
            accid_label=label
        )
        print('debit\t%s:\n'%str(accid),acct_dr)
        acct_cr=self.getAcct(
            accid,
            side='cr',
            pure=False,
            accurate=False,
            type_xl=False,
            accid_label=label,
        )
        print('credit\t%s:\n'%str(accid),acct_cr)
        print('-'*(len(accid)+17))
        # resu=[acct_all,acct_dr,acct_cr]
        if save==True:
            if acct_all.shape[0] !=0:
                save_df(
                    acct_all,
                    sheet_name='acct'+accid,
                    save_path=savepath,
                    file_nickname=nick_name
                )
            if acct_dr.shape[0] !=0:
                save_df(
                    acct_dr,
                    sheet_name='dr'+accid,
                    save_path=savepath,
                    file_nickname=nick_name
                )
            if acct_cr.shape[0] !=0:
                save_df(
                    acct_cr,
                    sheet_name='cr'+accid,
                    save_path=savepath,
                    file_nickname=nick_name
                )
            return [acct_all,acct_dr,acct_cr]
        else:
            return [acct_all,acct_dr,acct_cr]
    def multi_acct_analysis(
        self,
        accid_list,
        save=False,
        savepath='./',
        nick_name='multi_acct',
        label=None
    ):
        '''
        multi_thread is not supported currently and is being improved.
        accid_list:list
            A list to indicaate which accounts you are going to analysis;
        label:str
            self.xlmap.accid_col, indicating the location of accid;
        '''
        if save==True:
            for accid in accid_list:
                self.single_acct_analysis(
                    accid,
                    save=save,
                    savepath=savepath,
                    nick_name=nick_name,
                    label=label
                )
        elif save==False:
            for accid in accid_list:
                self.single_acct_analysis(
                    accid,
                    label=label,
                    save=save,
                    savepath=savepath,
                    nick_name=nick_name
                ) 
        else:
            pass
        pass
    def correspond(
        self,
        cr_accid,
        dr_accid,
        accurate=False,
        type_xl=False
    ):
        '''
        Get all journal entries from cr_accid to dr_accid;
        parameter:
            cr_accid:str
                credit account id;
            dr_accid:str
                debit account id;
            accurate:bool
        return:
            class pandas.core.frame.DataFrame.
            all journal entries from credit accid to debit accid;
        '''
        print(
            "-"*5,
            "credit:",
            cr_accid,
            "to ",
            "debit:",
            dr_accid,
            "-"*5
        )
        cr=self.getAcct(
            cr_accid,
            side='cr',
            pure=True,
            accurate=accurate,
            type_xl=False,
            accid_label=self.xlmap.accid_col
        )
        dr=self.getAcct(
            dr_accid,
            side='dr',
            pure=True,
            accurate=accurate,
            type_xl=False,
            accid_label=self.xlmap.accid_col
        )
        #  dr=self.xl2df(dr)
        #  cr=self.xl2df(cr)
        common_key_list=list(
            set(
                dr[self.xlmap.key_name].drop_duplicates()
            )&set(
                cr[self.xlmap.key_name].drop_duplicates()
            )
        )
        resu=self.filter_list(
            common_key_list,
            self.xlmap.key_name,
            over_write=False,
            type_xl=type_xl
        )
        return resu
    def cor_sum(self,cr_accid,dr_accid,accurate=False):
        '''
        Get all journal entries from cr_accid to dr_accid, and sum credit/debit amount;
        parameter:
            cr_accid:str
                credit account id;
            dr_accid:str
                debit account id;
            accurate:bool
        return:
           list:[cr_amount_sum,dr_amount_sum] 
        '''
        common_xl=self.correspond(
            cr_accid,
            dr_accid,
            accurate=accurate,
            type_xl=True
        )
        dr_sum=common_xl.sumifs(
            self.xlmap.drcrdesc[0],
            [[dr_accid,self.xlmap.accid_col,True,True]],
            filter_type='str'
        )
        cr_sum=common_xl.sumifs(
            self.xlmap.drcrdesc[1],
            [[cr_accid,self.xlmap.accid_col,True,True]],
            filter_type='str'
        )
        cr_dr_sum=[cr_sum,dr_sum]
        return cr_dr_sum
    def cor_glid(
        self,
        cr_accid,
        dr_accid,
        accurate=False,
        ):
        glid_list=set(
            self.correspond(
                cr_accid,
                dr_accid,
                accurate=accurate,
                type_xl=False
            )[self.xlmap.key_name]
        )
        glid_list=list(glid_list)
        return glid_list
    def find_opposite(
        self,
        target_accid_item,
        target_side,
        ref_accid_item,
        ref_side,
        info_col='item_name', # to find what data;
        col_index=1, # where to insert the result;
        col_name='opposite', # column name for the result;
        ):
        '''
        not perfect yet;
        '''
        import re
        target_cal=deepcopy(
            self.getAcct(
                target_accid_item,
                target_side,
                pure=False,
                accurate=False,
                type_xl=True,
                accid_label=self.xlmap.accid_col
            )
        )
        ref_cal=deepcopy(
            self.getAcct(
                ref_accid_item,
                ref_side,
                pure=True,
                accurate=False,
                type_xl=True,
                accid_label=self.xlmap.accid_col
            )
        )
        def drop_zero(resu_list):
            for test_value in resu_list:
                if test_value in [0,0.0,'0','0.0']:
                    resu_list.remove(test_value)
                continue
            while (0 in resu_list or 
                   0.0 in resu_list or 
                   '0' in resu_list or 
                   '0.0' in resu_list):
                drop_zero(resu_list)
            return resu_list
        def __df_find_opposite(row_series):
            current_accid=row_series[self.xlmap.accid_col]
            if re.search(target_accid_item,current_accid) is None:
                pass
            else:
                glid=row_series[self.xlmap.key_name]
                multi_values=ref_cal.vlookup(
                    glid,
                    self.xlmap.key_name,
                    info_col,
                    if_regex=True,
                    match_mode=True,
                    unique=True
                )
                multi_values=drop_zero(multi_values)
                if len(multi_values)==1:
                    multi_values=multi_values[0]
                else:
                    multi_values=';'.join(multi_values)
                return multi_values
        target_cal.apply_df_func(__df_find_opposite,col_index,col_name)
        return target_cal
    def save_cash_gl(self,savepath,cash_accid_item=r'100[12]'):
        cash_acct_dict=self.whatid(cash_accid_item)
        print(cash_acct_dict)
        cash_accid_list=list(cash_acct_dict.keys())
        self.multi_acct_analysis(cash_accid_list, save=True, savepath=savepath)
        dr_split=self.side_split(cash_accid_item,side='dr',show_col=self.xlmap.accna_col)
        cr_split=self.side_split(cash_accid_item, side='cr', show_col=self.xlmap.accna_col)
        save_df(dr_split,'dr_split',savepath)
        save_df(cr_split,'cr_split',savepath)
        pass
    def side_split(self,accid_item,side='cr',show_col='accna'):
        '''
        This method has not been able to return type `CalSheet`;
        '''
        return self.apply_xl_collect_df(
            'side_split',
            accid_item,
            side,
            show_col
        )
        #  thread_list=[]
        #  for xl in self.xlset:
            #  t=Thread(
                #  target=self.append_df_to_temp,
                #  args=(xl.side_split(accid_item,side=side,show_col=show_col),),
                #  name=''
            #  )
            #  thread_list.append(t)
            #  continue
        #  for t in thread_list:
            #  t.start()
        #  for t in thread_list:
            #  t.join()
        #  resu_df=self.get_df_temp_data(over_write=False, type_xl=False)
        #  return resu_df
    def side_analysis(self,accid,side='cr',top_mode=False):
        '''
        accid must NOT be regex!
        Credit amount is negative;
        Debit amount is positive;
        '''
        if top_mode==True:
            accid=str(accid)[0:self.xlmap.top_accid_len]
            pass
        accid_item=self.__trans_accid_regex(accid,accurate=True)
        acct_mgl=self.blank_copy()
        for xl in self.xlset:
            acct_mgl.append_xl_by_map(
                deepcopy(xl),
                xlmap=None
            )
        acct_mgl.filterAcct(
            accid_item,
            side=side,
            pure=False,
            accurate=True,
            over_write=True, 
            # if over_write==True, then acct_mgl.data will be loaded;
            type_xl=False,
            accid_label=None
        )
        if side=='cr':
            side_name=acct_mgl.xlmap.drcrdesc[1]
        elif side=='dr':
            side_name=acct_mgl.xlmap.drcrdesc[0]
        else:
            return [
                self.side_analysis(
                    accid,
                    side='dr',
                    top_mode=top_mode
                ),
                self.side_analysis(
                    accid,
                    side='cr',
                    top_mode=top_mode
                )
            ]
        def __acct_mark_side(row_series):
            current_accid=row_series[acct_mgl.xlmap.accid_col]
            side_amount=row_series[side_name]
            if current_accid==accid and side_amount !=0:
                return 'target_acct'
            else:
                return 'opposite_acct'
        acct_mgl.apply_df_func(
            __acct_mark_side,
            'mark_record'
        )
        resu_mgl=self.__class__(
            xlmap=self.xlmap.__class__.from_list([
                acct_mgl.xlmap.top_accid_col,
                acct_mgl.xlmap.accid_col,
                'drcr',
                acct_mgl.xlmap.accna_col,
                acct_mgl.xlmap.top_accna_col
            ]),
            xlmeta=None,
        )
        def __acct_xl_sum_opposite(
            xl,
            accid_list,
        ):
            
            resu_array=[]
            for acct in accid_list:
                acct_sum=xl.sumifs(
                    'drcr',
                    [
                        ['opposite_acct','mark_record',True,True],
                        [acct,acct_mgl.xlmap.accid_col,True,True]
                    ],
                    filter_type='str'
                )
                resu_array.append(
                    [acct,acct_sum,self.acctmap[acct]]
                )
                continue
            resu_array=DataFrame(
                resu_array,
                columns=[
                    acct_mgl.xlmap.accid_col,
                    'drcr',
                    acct_mgl.xlmap.accna_col,
                ]
            )
            resu_array.sort_values(
                'drcr',
                axis=0,
                ascending=False, # if True,1 to 9;
                inplace=True,
                kind='quicksort',
                na_position='last',
                ignore_index=True,
                key=None
            )
            resu_mgl.append_df_by_map(resu_array)
            return
        accid_list=list(
            acct_mgl.data[acct_mgl.xlmap.accid_col].drop_duplicates()
        )
        thread_list=[]
        for xl in acct_mgl.xlset:
            t=Thread(
                target=__acct_xl_sum_opposite,
                args=(
                    xl,
                    accid_list,
                ),
                name=''.join([
                    'apply->',
                    getattr(__acct_xl_sum_opposite,'__name__'),
                    r',for:',
                    xl.name
                ])
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        resu_mgl.apply_df_func(
            lambda row_series:str(row_series[self.xlmap.accid_col][0:self.xlmap.top_accid_len]),
            self.xlmap.top_accid_col
        )
        resu_mgl.apply_df_func(
            lambda row_series:str(row_series[self.xlmap.accna_col].split(self.xlmap.accna_split_by)[0]),
            self.xlmap.top_accna_col
        )
        resu_mgl.load_raw_data()
        print(resu_mgl.data)
        return resu_mgl
    def rand_sample(
        self,
        ss=None,
        percent=None,
        replace=False,
        weights=None,
        random_state=None,
        axis=None,
        over_write=False
    ):
        '''
        This method is not perfect yet!
        raw data must be loaded first!
        DataFrame.sample(n=None, frac=None, replace=False, weights=None, random_state=None, axis=None)
        parameters:
            ss:sample_size;
            percent:percentage of total to sample;
            axis: 0 for rows_sampling and 1 for colums_sampling;
            over_write: if True, self.sample_data will be over_write and therefore replaced. Currently, this parameter does nothing, once rand_sample(), self.sample_data will be over written.
        '''
        if self.data is None:
            self.load_raw_data()
        if over_write==True:
            self.sample_data=self.data.sample(
                n=ss,
                frac=percent,
                replace=replace,
                weights=weights,
                random_state=random_state,
                axis=axis
            )
            return self.sample_data
        else:
            return self.data.sample(
                n=ss,
                frac=percent,
                replace=replace,
                weights=weights,
                random_state=random_state,
                axis=axis
            )
    ### not perfet yet ???
    def get_gl_matrix(self,if_top_accid=False,over_write=False):
        '''
        Not perfect;
        returns:
            pandas.core.frame.DataFrame
        '''
        if self.data is None:
            self.load_raw_data()
        if if_top_accid==False:
            gl_matrix=self.data.pivot_table(values=['drcr'],index=[self.xlmap.key_name],columns=[self.xlmap.accid_col])
        elif if_top_accid==True:
            gl_matrix=self.data.pivot_table(values=['drcr'],index=['top_accid'],columns=[self.xlmap.accid_col])
        else:
            pass
        if over_write==True:
            self.gl_matrix=gl_matrix
        return gl_matrix
    ##### method get_gl_matrix is not perfect;#####
    # def get_gl_matrix(self,if_top_accid=False,over_write=False):
    #     '''
    #     self.data is not None on default;
    #     this method is not perfect yet;
    #     '''
    #     self.load_raw_data()
    #     from pandas import DataFrame
    #     glid_list=self.data[self.xlmap.key_name].drop_duplicates()
    #     if if_top_accid==False:
    #         accid_list=list(map(transType,self.data[self.xlmap.accid_col].drop_duplicates()))
    #     else:
    #         accid_list=list(map(transType,self.data['top_accid'].drop_duplicates()))
    #     def write_matrix_thread(df,i,j):
    #         if if_top_accid==False:
    #             sum_df=self.filter([[i,self.xlmap.key_name,True,True],[j,self.xlmap.accid_col,True,True]], filter_type='str')
    #         else:
    #             sum_df=self.filter([[i,self.xlmap.key_name,True,True],[j,'top_accid',True,True]], filter_type='str')
    #         df.iloc[i,j]=sum_df['drcr'].sum(axis=0)
    #         pass
    #     gl_matrix=DataFrame([],index=glid_list,columns=accid_list)
    #     thread_list=[]
    #     for i in range(len(glid_list)):
    #         for j in range(len(accid_list)):
    #             t=Thread(target=write_matrix_thread,args=(gl_matrix, i, j))
    #             thread_list.append(t)
    #     for t in thread_list:
    #         t.start()
    #     for t in thread_list:
    #         t.join()
    #     if over_write==True:
    #         self.gl_matrix=gl_matrix
    #     return gl_matrix
    pass
if __name__=='__main__':
    pass
