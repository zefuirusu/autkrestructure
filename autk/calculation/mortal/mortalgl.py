#!/usr/bin/env python
# encoding = 'utf-8'

import pysnooper

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
    #  @pysnooper.snoop(depth=2,prefix='[MGL]',relative_time=True)
    def __init__(
        self,
        xlmap:MglMap=None,#MglMap(),
        xlmeta:JsonMeta=None,#JsonMeta({
            #  'BLANK_PATH':[['sheet',0]]
        #  }),
        ):
        print('---Initializing MGL---')
        t_start=datetime.datetime.now()
        super().__init__(
            xlmap,
            xlmeta,
        )
        self.gl_matrix=None
        t_end=datetime.datetime.now()
        t_interval=t_end-t_start
        print('Initialize time spent:',t_interval)
        print('---MGL Initialized---')
        pass
    def collect_xl(self):
        '''
        collect XlSheet from `self.xlmeta` by
        `self.xlmap`, into `self.xlset`.
        '''
        print('start collecting xl to self.xlset.')
        self.__clear_data()
        def __single_append(shmeta):
            '''
            same as `self.append_xl_by_meta`
            '''
            xl=CalSheet(
                xlmap=self.xlmap,
                shmeta=shmeta
            )
            if isinstance(self.xlmap,MglMap):
                pass
            else:
                self.xlmap=MglMap()
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
                        'callsheet'
                    ])
                )
            )
            continue
        start_thread_list(thli)
        pass
    def append_xl_by_meta(self,shmeta):
        '''
        '''
        #TODO not perfect when self.xlmap=None
        self.xlset.append(
            CalSheet(
                xlmap=self.xlmap,
                shmeta=shmeta,
            )
        )
        pass
    def __parse_acctmap(self):
        '''
        self.set_key_cols() must be called before this.
        '''
        self.acctmap={}
        thread_list=[]
        for xl in self.xlset:
            thread_list.append(
                Thread(
                    target=self.acctmap.update,
                    args=(xl.acctmap,)
                )
            )
            continue
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()
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
    def getjr(self,glid,type_xl=False):
        '''
        self.xlmap must not be None.
        parameters:
            glid: glid;
        return:
            class autk.parser.entry.JournalRecord;
        '''
        if self.use_map==True:
            jr_data=self.filter(
                [glid,self.xlmap.key_name,True,True],
                filter_type='str',
                over_write=False,
                type_xl=type_xl
            )
            return jr_data
        else:
            jr_data=self.filter(
                [[glid,self.xlmap.key_name,True,True]],
                filter_type='str',
                over_write=False,
                type_xl=False
            )
            return jr_data
        pass
    def __trans_accid_regex(self,accid,accurate=False):
        '''
        r'^'+accid+r'.*$' if not accurate, as default,
        or r'^\s*'+accid+r'\s*$' if accurate is True;
        
        '''
        # import re
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
    def whatna(self,accna_item):
        '''
        parameters:
            accna_item:str
                regular expression is supported.
        return:dict
            {accid:accna,accid:accna...}
        '''
        acctna_str=accna_item
        accna_item=self.__trans_accna_regex(accna_item,accurate=False)
        accna_list=regex_filter(
            accna_item,
            self.acctmap_invert.keys(),
            match_mode=True
        )
        acct={}
        #  resu=[]
        for accna in accna_list:
            accid=self.acctmap_invert[accna]
            acct.update(
                {accid:accna}
            )
        print(acctna_str,':\t',acct)
        return acct
    def whatid(self,accid_item):
        '''
        parameters:
            accid_item:str
                regular expression is supported.
        return:dict
            {accid:accna,accid:accna...}
        '''
        accid_str=accid_item
        accid_item=accid_item.join([r'^.*',r'.*$'])
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
        print(accid_str,'\t',acct)
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
        #  accid_item,
        #  side='all',
        #  pure=False,
        #  accurate=False,
        #  over_write=False,
        #  type_xl=False,
        #  accid_label=None
        *args,
        **kwargs,
    ):
        '''
        If over_write is True, result data will replace
        self.data;
        `side` can be 'all/dr/cr';
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
        #  accid_item,
        #  side='all',
        #  pure=False,
        #  accurate=False,
        #  type_xl=False,
        #  accid_label=None
        *args,
        **kwargs,
    ):
        return self.apply_xl_collect_df(
            'getAcct',
            *args,
            **kwargs,
        )
    def getitem(
        self,
        #  regex_str,
        #  by='item_name',
        #  key_name=None,
        #  over_write=False,
        #  type_xl=False
        *args,
        **kwargs,
    ):
        '''
        Get full Journal Entries according to 'item_name' matched by regex_str by search-mode;
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
        #  common_key_list=list(
            #  set(
                #  self.vlookup(
                    #  dr_accid,
                    #  self.xlmap.accid_col,
                    #  resu_col=self.xlmap.key_name,
                    #  if_regex=True,
                    #  match_mode=True
                #  )
            #  )&set(
                #  self.vlookup(
                    #  cr_accid,
                    #  self.xlmap.accid_col,
                    #  resu_col=self.xlmap.key_name,
                    #  if_regex=True,
                    #  match_mode=True
                #  )
            #  )
        #  )
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
        #  print(target_cal.data)
        #  print(ref_cal.data)
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
                #  print(multi_values)
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
        #  self.clear_temp_df()
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=self.append_df_to_temp,
                args=(xl.side_split(accid_item,side=side,show_col=show_col),),
                name=''
            )
            thread_list.append(t)
            continue
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()
        resu_df=self.get_df_temp_data(over_write=False, type_xl=False)
        return resu_df
    def side_analysis(self,accid,side='cr',top_mode=False):
        '''
        accid must NOT be regex!
        Credit amount is negative;
        Debit amount is positive;
        '''
        if top_mode==True:
            accid=str(accid)[0:self.top_accid_len]
            pass
        accid_item=self.__trans_accid_regex(accid,accurate=True)
        acct_mgl=self.duplicate(use_meta=True)
        resu_mgl=self.duplicate(use_meta=False)
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
            side_name=acct_mgl.drcrdesc[1]
        elif side=='dr':
            side_name=acct_mgl.drcrdesc[0]
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
            current_accid=row_series[acct_mgl.accid_col]
            side_amount=row_series[side_name]
            if current_accid==accid and side_amount !=0:
                return 'target_acct'
            else:
                return 'opposite_acct'
        acct_mgl.apply_df_func(__acct_mark_side,1,'mark_record')
        def __acct_xl_sum_opposite(
            xl,
            accid_list,
            new_file_name,
            new_sheet_name
        ):
            
            resu_array=[]
            for acct in accid_list:
                acct_sum=xl.sumifs(
                    'drcr',
                    [
                        ['opposite_acct','mark_record',True,True],
                        [acct,acct_mgl.accid_col,True,True]
                    ],
                    filter_type='str'
                )
                resu_array.append(
                    [acct,acct_sum,self.acctmap[acct]]
                )
                continue
            resu_array=DataFrame(
                resu_array,
                columns=[acct_mgl.accid_col,'drcr',acct_mgl.accna_col]
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
            #  print('result_array:\n',resu_array)
            resu_xl=self.calxl(resu_array)
            setattr(resu_xl,'pure_file_name',new_file_name)
            setattr(resu_xl,'sheet_name',new_sheet_name)
            # setattr(resu_xl,'columns',list(resu_array.columns))
            resu_mgl.xlset.append(resu_xl)
            return resu_array
        acct_mgl.load_raw_data()
        accid_list=list(
            acct_mgl.data[acct_mgl.accid_col].drop_duplicates()
        )
        thread_list=[]
        for xl in acct_mgl.xlset:
            t=Thread(
                target=__acct_xl_sum_opposite,
                args=(xl,accid_list,xl.pure_file_name,xl.sheet_name),
                name=''.join([
                    'apply->',
                    getattr(__acct_xl_sum_opposite,'__name__'),
                    r',for:',
                    xl.pure_file_name,
                    xl.sheet_name
                ])
            )
            thread_list.append(t)
            continue
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()
        resu_mgl.set_top_acct(top_accid_len=4,accna_split_by=r'/')
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
    def append_df(self,in_df):
        '''
        columns of input DataFrame must correspond to that of self.xlmeta!
        Same as:
            xl=self.calxl()
            xl.accept_data(in_df)
            self.xlset.append(xl)
            # table=self.duplicate(use_meta=False)
            if self.data is not None:
                self.data=concat([self.data,in_df])
            else:
                self.data=in_df
        '''
        from pandas import concat
        xl=CalSheet(
            [None,
            '',
            self.common_title],
            xlmap=deepcopy(self.xlmap),
            #  use_map=self.use_map,
            keep_meta_info=self.keep_meta_info,
        )
        xl.accept_data(in_df)
        table=MGL(
            key_index=self.xlmap.key_index,
            key_name=self.xlmap.key_name,
            xlmap=self.xlmap
        )
        table.xlset.append(xl)
        table.data=in_df
        self.xlset.extend(table.xlset)
        if self.data is not None:
            self.data=concat([self.data,table.data],axis=0,join='outer')
        else:
            self.data=table.data
        pass
    def append_xl(self,file_path,sheet_name,title):
        from pandas import concat
        table=MGL(key_index=self.xlmap.key_index,key_name=self.xlmap.key_name,xlmap=self.xlmap)
        table.parse_meta([file_path,sheet_name,title], self.common_title, auto_load=True)
        self.xlset.extend(table.xlset)
        if self.data is not None:
            self.data=concat([self.data,table.data],axis=0,join='outer')
        else:
            self.data=table.data
        pass
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
