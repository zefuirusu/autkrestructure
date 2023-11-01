#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import deepcopy
from threading import Thread
from pandas import DataFrame

from autk.mapper.map import MglMap,get_glmap
from autk.gentk.funcs import transType,regex_filter,save_df
from autk.calculation.base.xlsht import XlSheet

class CalSheet(XlSheet):
    '''
    CalSheet must have a map to indicate its column-structure;
    If xlmap is passed None,then columns will be:
        ['glid','date','mark','jrid','accid','accna','dr_amount','cr_amount','drcr','item_name','note']
    '''
    def __init__(
        self,
        shmeta=[None,'sheet0',0],
        #  file_path=None,
        #  sheet_name='sheet0',
        #  title=0,
        # structure of the table is less important than meta information.
        #  key_index=['date','mark','jrid'], #['凭证日期','字','号'],
        #  key_name='glid',
        #  drcrdesc=['dr_amount','cr_amount'],#['借方发生金额','贷方发生金额'],
        #  accid_col='accid',#科目编号',
        #  accna_col='accna',#科目全路径',
        #  date_col='date',#凭证日期',
        #  date_split_by=r'-',
        #  top_accid_len=4,
        #  accna_split_by=r'/',
        xlmap=None,
        #  use_map=False,
        keep_meta_info=False,
    ):
        if xlmap is None:
            #  xlmap=get_glmap([
                #  'glid','date','mark','jrid','accid','accna','dr_amount','cr_amount','drcr','item_name','note'
            #  ])()
            xlmap=MglMap.from_list([
                'glid','date','mark','jrid','accid','accna','dr_amount','cr_amount','drcr','item_name','note'
            ])
        else:
            pass
        XlSheet.__init__(
            self,
            shmeta=shmeta,
            xlmap=xlmap,
            use_map=True,
            keep_meta_info=keep_meta_info,
            key_index=xlmap.key_index,
            key_name=xlmap.key_name
        )
        #  self.set_key_cols(
            #  drcrdesc=drcrdesc,
            #  accid_col=accid_col,
            #  accna_col=accna_col )
        #  self.set_top_acct(
            #  top_accid_len,
            #  accna_split_by
        #  )
        #  self.set_date(
            #  date_col=date_col,
            #  date_split_by=date_split_by
        #  )
        self.__parse_acctmap()
        self.fake=False
        pass
    def set_key_cols(
        self,
        #  drcrdesc=['借方发生金额','贷方发生金额'],
        #  accid_col='科目编号',
        #  accna_col='科目全路径'
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
        #  if self.use_map==False:
            #  accid_col='科目编号'
            #  accna_col='科目全路径'
        #  elif isinstance(self.xlmap,MglMap):
            #  accid_col='accid'
            #  accna_col='accna'
        #  else:
            #  pass
        print('see the cols:',self.xlmap.columns)
        if isinstance(self.data,DataFrame):
            map_df=self.data[[self.xlmap.accid_col,self.xlmap.accna_col]]
            map_dict=zip(
                list(map_df[self.xlmap.accid_col]),
                list(map_df[self.xlmap.accna_col])
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
                top_accna=accna.split(self.xlmap.accna_split_by)[0]
                top_accna=str(top_accna)
                return top_accna
            self.apply_df_func(top_accid_apply_func,1,'top_accid')
            self.apply_df_func(top_accna_apply_func,1,'top_accna')
        pass
    def set_date(self,date_col='date',date_split_by=r'-'):
        def __get_year(row_series):
            if self.use_map==True:
                date_str=row_series[self.xlmap.date_col]
            else:
                date_str=row_series[date_col]
            if isinstance(date_str,str) and date_str != '0.0':
                year=transType(date_str.split(date_split_by)[0])
            else:
                year='0'
            return year
        def __get_month(row_series):
            if self.use_map==True:
                date_str=row_series[self.xlmap.date_col]
            else:
                date_str=row_series[date_col]
            if isinstance(date_str,str) and (date_str != '0.0' or date_str != 0.0):
                month=transType(date_str.split(date_split_by)[1])
            else:
                month='0'
            return month
        def __get_day(row_series):
            if self.use_map==True:
                date_str=row_series[self.xlmap.date_col]
            else:
                date_str=row_series[date_col]
            if isinstance(date_str,str) and date_str != '0.0':
                day=transType(date_str.split(date_split_by)[2])
            else:
                day='0'
            return day
        if self.use_map==True:
            setattr(self,'date_col',self.xlmap.date_col)
            setattr(self,'date_split_by',self.xlmap.date_split_by)
        else:
            setattr(self,'date_col',date_col)
            setattr(self,'date_split_by',date_split_by)
        if self.shmeta[0] is not None and date_col in self.columns:
            date_col_index=list(self.columns).index(date_col)
            self.change_dtype(date_col,target_type=str)
            self.apply_df_func(__get_year,date_col_index,'year')
            self.apply_df_func(__get_month,date_col_index+1,'month')
            self.apply_df_func(__get_day,date_col_index+2,'day')
        pass
    #  def duplicate(self,use_meta=False):
        #  from copy import deepcopy
        #  calculator=deepcopy(self)
        #  if use_meta==False:
            #  calculator.clear()
        #  return calculator
    def accept_df(self,in_df):
        '''
        if you code 'self.data=in_df',
        index/columns of them may differs,
        yet inner data will be the same;
        '''
        super().accept_df(in_df)
        self.__parse_acctmap()
    def calxl(self,df=None):
        cal=CalSheet(
            shmeta=[None,self.sheet_name,self.title],
            #  file_path=None,
            #  sheet_name='sheet0',
            #  title=0,
            #  key_index=deepcopy(self.key_index),
            #  key_name=deepcopy(self.key_name),
            #  drcrdesc=deepcopy(self.drcrdesc),
            #  accid_col=deepcopy(self.accid_col),
            #  accna_col=deepcopy(self.accna_col),
            #  date_col=deepcopy(self.date_col),
            #  date_split_by=self.date_split_by,
            xlmap=deepcopy(self.xlmap),
            use_map=self.use_map,
            keep_meta_info=self.keep_meta_info,
        )
        cal.accept_df(df)
        return cal
    #  def xl2df(self,xl):
        #  '''
        #  Transfer XlSheet into it's data;
        #  '''
        #  df=deepcopy(xl.data)
        #  return df
    #  def df2xl(self,df):
        #  xl=self.calxl()
        #  xl.accept_df(deepcopy(df))
        #  return xl
    def getjr(self,glid,type_xl=False):
        '''
        'glid' can be a regular expression string, in match_mode.
        '''
        if self.use_map==True:
            jr_data=self.filter(
                [glid,self.key_name,True,True],
                filter_type='str',
                over_write=False,
                type_xl=type_xl
            )
            return jr_data
        else:
            jr_data=self.filter(
                [[glid,self.key_name,True,True]],
                filter_type='str',
                over_write=False,
                type_xl=False
            )
            return jr_data
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
        '''
        parameters:
            accna_item:str
                regular expression is supported.
        return:dict
            print DataFrame but return dict;
            {accid:accna,accid:accna...}
        '''
        acctna_str=accna_item
        accna_item=self.__trans_accna_regex(accna_item,accurate=False)
        #  acct_cols=[
            #  self.accid_col,
            #  self.drcrdesc[0],
            #  self.drcrdesc[1],
            #  self.accna_col
        #  ]
        accna_list=regex_filter(
            accna_item,
            self.acctmap_invert.keys(),
            match_mode=True
        )
        acct={}
        #  resu=[]
        for accna in accna_list:
            accid=self.acctmap_invert[accna]
            #  acct_sum=self.getAcct(
                #  accid,side='all',
                #  pure=True,
                #  accurate=True,
                #  type_xl=False,
                #  accid_label=None
            #  )[self.drcrdesc].sum(axis=0)
            acct.update(
                {accid:accna}
            )
            #  resu.append(
                #  [accid,
                 #  acct_sum[self.drcrdesc[0]],
                 #  acct_sum[self.drcrdesc[1]],
                 #  accna]
            #  )
        #  resu=DataFrame(
            #  resu,
            #  columns=acct_cols
        #  )
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
        print(accid_str,':\t',acct)
        return acct
    def scan_byna(self,accna_item):
        acct_cols=[
            self.accid_col,
            self.drcrdesc[0],
            self.drcrdesc[1],
            self.accna_col
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
                acct_sum=acct_df[self.drcrdesc].sum(axis=0)
                sub_resu=[
                    accurate_accid,
                    acct_sum[self.drcrdesc[0]],
                    acct_sum[self.drcrdesc[1]],
                    accna
                ]
                return sub_resu
            thread_list=[]
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
            drcr_sum=drcr_data[self.drcrdesc].sum(axis=0)
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
                self.accid_col,
                self.drcrdesc[0],
                self.drcrdesc[1],
                self.accna_col
            ]
        )
        return resu
    #  def scanAcct(self,accid_item,accid_label=None):
        #  '''
        #  parameters:
            #  accid_item: str
                #  regular expression is supported;
        #  returns:
            #  multi-list like object;
        #  '''
        #  def __accurate_scan(accurate_accid,acct_json):
            #  drcr_data=self.getAcct(
                #  accurate_accid,
                #  side='all',
                #  pure=True,
                #  accurate=True,
                #  type_xl=False,
                #  accid_label=accid_label
            #  )
            #  drcr_sum=drcr_data[self.drcrdesc].sum(axis=0)
            #  drcr_sum=list(drcr_sum)
            #  sub_resu=[
                #  accurate_accid,
                #  drcr_sum[0],
                #  drcr_sum[1],
                #  acct_json[accurate_accid]
            #  ]
            #  return sub_resu
        #  acct_json=self.whatid(accid_item)
        #  accid_list=list(acct_json.keys())
        #  resu=[]
        #  if len(accid_list)==0:
            #  print('cannot find:',accid_item)
            #  pass
        #  elif len(accid_list) >0:
            #  for accid in accid_list:
                #  sub_resu=__accurate_scan(accid,acct_json)
                #  resu.append(sub_resu)
        #  else:
            #  pass
        #  print('\nscan:\n',acct_json)
        #  resu=DataFrame(
            #  resu,
            #  columns=[
                #  self.accid_col,
                #  self.drcrdesc[0],
                #  self.drcrdesc[1],
                #  self.accna_col
            #  ]
        #  )
        #  return resu
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
        if accid_label is None:
            accid_label=self.accid_col
        accid_item=self.__trans_accid_regex(accid_item,accurate=accurate)
        if pure==True:
            if side=='all':
                resu=self.filter(
                    [[accid_item,self.accid_col,True,True]],
                    filter_type='str',
                    over_write=over_write,
                    type_xl=type_xl
                )
                pass
            elif side=='dr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.accid_col,True,True]],
                        'number':[[self.drcrdesc[0],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=over_write,
                    type_xl=type_xl
                )
                pass
            elif side=='cr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.accid_col,True,True]],
                        'number':[[self.drcrdesc[1],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=over_write,
                    type_xl=type_xl
                )
                pass
            else:
                resu=None
                print('[Note:MGL] Invalid argument, filter None will not overwrite.')
                pass
            pass
        elif pure==False:
            if side=='all':
                resu=self.filter(
                    [[accid_item,self.accid_col,True,True]],
                    filter_type='str',
                    over_write=False,
                    type_xl=False
                )
                pass
            elif side=='dr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.accid_col,True,True]],
                        'number':[[self.drcrdesc[0],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=False,
                    type_xl=False
                )
                pass
            elif side=='cr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.accid_col,True,True]],
                        'number':[[self.drcrdesc[1],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=False,
                    type_xl=False
                )
                pass
            else:
                resu=None
                print(
                    '[Note:MGL] Invalid argument, got None; not overwritten;'
                )
                pass
            acct_glid_list=list(
                    resu[self.key_name].drop_duplicates()
            )
            if len(acct_glid_list)==0:
                resu=None
            else:
                resu=self.filter_list(
                    acct_glid_list,
                    search_col=self.key_name,
                    over_write=over_write,
                    type_xl=type_xl
                )
            pass
        if resu is None:
            resu=DataFrame([],columns=self.data.columns)
            return resu
        else:
            return resu
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
        return self.filterAcct(
            accid_item,
            side=side,
            pure=pure,
            accurate=accurate,
            over_write=False,
            type_xl=type_xl,
            accid_label=accid_label
        )
    def getitem(
        self,
        regex_str,
        by='item_name',
        key_name=None,
        over_write=False,
        type_xl=False
    ):
        '''
        Get full Journal Entries according to 'item_name' matched by regex_str;
        '''
        if key_name is None:
            key_name=self.key_name
        item_data=self.filter(
            [[regex_str,by,True,False]],
            'str',
            over_write=False,
            type_xl=False
        )
        #  item_data=self.xl2df(item_data)
        key_list=list(
            item_data[self.key_name].drop_duplicates()
        )
        resu=self.filter_list(
            key_list,
            key_name,
            over_write=over_write,
            type_xl=type_xl
        )
        return resu
    def single_acct_analysis(
        self,
        accid,
        accurate=False,
        save=False,
        savepath='./',
        nick_name='single_acct',
        label=None
    ):
        '''
        This method does not return anything.
        '''
        acct_all=self.getAcct(
            accid,
            side='all',
            pure=False,
            accurate=accurate,
            type_xl=False,
            accid_label=label
        )
        acct_dr=self.getAcct(
            accid,
            side='dr',
            pure=False,
            accurate=accurate,
            type_xl=False,
            accid_label=label
        )
        acct_cr=self.getAcct(
            accid,
            side='cr',
            pure=False,
            accurate=accurate,
            type_xl=False,
            accid_label=label,
        )
        def save_acct(df):
            if df.shape[0]>0:
                save_df(
                    df,
                    sheet_name='',
                    save_path=savepath,
                    file_nickname=nick_name
                )
            else:
                print(df.shape,'\n',df)
        if save==True:
            save_acct(acct_all)
            save_acct(acct_dr)
            save_acct(acct_cr)
        else:
            print('='*5,accid,'='*12)
            print('acct\t%s:\n'%str(accid),acct_all)
            print('debit\t%s:\n'%str(accid),acct_dr)
            print('credit\t%s:\n'%str(accid),acct_cr)
            print('-'*(len(accid)+17))
        pass
    def multi_acct_analysis(
        self,
        accid_list,
        accurate=False,
        save=False,
        savepath='./',
        nick_name='multi_acct',
        label=None
    ):
        for accid in accid_list:
            self.single_acct_analysis(
                accid,
                accurate=accurate,
                save=save,
                savepath=savepath,
                nick_name=nick_name,
                label=label
            )
            continue
        pass
    def correspond(
        self,
        cr_accid,
        dr_accid,
        accurate=False,
        over_write=False,
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
        #  print(
            #  "-"*5,
            #  "credit:",
            #  cr_accid,
            #  "to ",
            #  "debit:",
            #  dr_accid,
            #  "-"*5
        #  )
        cr=self.getAcct(
            cr_accid,
            side='cr',
            pure=True,
            accurate=accurate,
            type_xl=False,
            accid_label=self.accid_col
        )
        dr=self.getAcct(
            dr_accid,
            side='dr',
            pure=True,
            accurate=accurate,
            type_xl=False,
            accid_label=self.accid_col
        )
        common_key_list=list(
            set(
                dr[self.key_name]
            )&set(
                cr[self.key_name]
            )
        )
        resu=self.filter_list(
            common_key_list,
            self.key_name,
            over_write=over_write,
            type_xl=type_xl
        )
        return resu
    def cor_sum(self,cr_accid,dr_accid,accurate=False):
        common_xl=self.correspond(
            cr_accid,
            dr_accid,
            accurate=accurate,
            over_write=False,
            type_xl=True
        )
        dr_sum=common_xl.sumifs(
            self.drcrdesc[0],
            [[dr_accid,self.accid_col,True,True]],
            filter_type='str'
        )
        cr_sum=common_xl.sumifs(
            self.drcrdesc[1],
            [[cr_accid,self.accid_col,True,True]],
            filter_type='str'
        )
        cr2dr_sum=DataFrame(
            [[dr_sum,0],[0,cr_sum]],
            index=[dr_accid,cr_accid],
            columns=self.drcrdesc
        )
        return cr2dr_sum
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
                over_write=False,
                type_xl=False
            )[self.key_name]
        )
        glid_list=list(glid_list)
        return glid_list
    def find_opposite(self,col_index,col_name='opposite_accid'):
        pass
    def acct_side_split(self,accid,side='cr'):
        '''
        accid is just accid, not regex;
        '''
        resu_unit=self.duplicate(use_meta=False)
        analysis_unit=self.duplicate(use_meta=True)
        analysis_unit.filterAcct(
            accid,
            side=side,
            pure=False,
            accurate=True,
            over_write=True,
            type_xl=False,
            accid_label=self.accid_col
        )
        if side=='cr':
            side_name=analysis_unit.drcrdesc[1]
        elif side=='dr':
            side_name=analysis_unit.drcrdesc[0]
        else:
            # 'cr' side is for default;
            side_name=analysis_unit.drcrdesc[1]
        def __acct_mark_side(row_series):
            current_accid=row_series[analysis_unit.accid_col]
            side_amount=row_series[side_name]
            if current_accid==accid and side_amount !=0:
                return 'target_acct'
            else:
                return 'opposite_acct'
        analysis_unit.apply_df_func(__acct_mark_side,1,'mark_record')
        # opposite account id:
        oppo_accid_list=list(
            set(
                analysis_unit.data[analysis_unit.accid_col]
            )
        )
        oppo_glid_list=list(
            set(
                analysis_unit.data[analysis_unit.key_name]
            )
        )
        resu_array=[]
        for oppo_accid in oppo_accid_list:
            # opposite account amount sum;
            oppo_amt_sum=analysis_unit.sumifs(
                'drcr',[
                    ['opposite_acct','mark_record',True,True],
                    [oppo_accid,analysis_unit.accid_col,True,True]
                ],
                filter_type='str'
            )
            resu_array.append(
                [oppo_amt_sum,oppo_accid,self.acctmap[oppo_accid]]
            )
            continue
        resu_df=DataFrame(
            resu_array,
            columns=[
                str(accid),
                analysis_unit.accid_col,
                analysis_unit.accna_col
            ]
        )
        resu_df.sort_values(
            str(accid),
            axis=0,
            ascending=False, # if True,1 to 9;
            inplace=True,
            kind='quicksort',
            na_position='last',
            ignore_index=True,
            key=None
        )
        resu_unit.accept_df(resu_df)
        resu_unit.set_top_acct(top_accid_len=4,accna_split_by=r'/')
        # pvt_df=resu_unit.data.pivot_table(
        #     values=['drcr'],
        #     index=['accna']
        # )
        # print('resu:',resu_df.shape,resu_df['drcr'].sum(),'\n',resu_df)
        # print('pvt_resu:',pvt_df.shape,pvt_df['drcr'].sum(),'\n',pvt_df)
        # print(resu_unit.data)
        resu_cols=[
            'top_accid','top_accna',
            analysis_unit.accid_col,
            analysis_unit.accna_col,
            str(accid)
        ]
        # resu_unit has columns:
        # ['accid',accid_col,accna_col],
        # of which data is:
        # [oppo_amt_sum,oppo_accid,oppo_accna]
        # resu_df(last)/resu_array, data of resu_unit, has columns:
        # ['top_accid','top_accna',accid_col,accna_col,this_accid],
        # of which data is:
        # [oppo_amt_sum,oppo_accid,oppo_accna]
        # column data is named from target_accid;
        resu_df=deepcopy(resu_unit.data[resu_cols])
        return resu_df
    def side_split(self,accid_item,side='cr',show_col='accna'):
        '''
        show_col must be one of :['top_accna','top_accid',self.accid_col,self.accna_col];
        '''
        tar_accid_list=list(self.whatid(accid_item).keys())
        resu_dfli=[]
        for tar_accid in tar_accid_list:
            split_df=self.acct_side_split(tar_accid,side=side)
            split_df.fillna(0.0,inplace=True)
            resu_dfli.append(split_df)
            continue
        from pandas import concat
        resu_df=concat(resu_dfli,axis=0,join='outer')
        resu_df.fillna(0.0,inplace=True)
        # print('temp:\n',resu_df)
        resu_cols=[show_col]
        resu_cols.extend(tar_accid_list)
        resu_df=resu_df[resu_cols]
        # resu_df has columns:
        # [show_col+tar_accid_list],
        return resu_df
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
    ##### the following will be abandoned; ####
    def fake_side_analysis(self,accid,side='cr',pvt_mode=False,type_xl=False):
        '''
        accid must NOT be regex!
        Credit number is negative;
        Debit number is positive;
        '''
        accid_item=self.__trans_accid_regex(accid,accurate=True)
        accid_list=self.scan_byid(accid_item, accid_label=self.accid_col)
        acct_mgl=self.duplicate(use_meta=True)
        resu_mgl=self.duplicate(use_meta=False)
        acct_mgl.filterAcct(
            accid_item,
            side=side,
            pure=False,
            accurate=True,
            over_write=True,
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
                    side='dr'
                ),
                self.side_analysis(
                    accid,
                    side='cr'
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
        accid_list=list(
            set(
                acct_mgl.data[acct_mgl.accid_col]
            )
        )
        resu_array=[]
        for accid in accid_list:
            acct_sum=acct_mgl.sumifs(
                'drcr',[
                    ['opposite_acct','mark_record',True,True],
                    [accid,acct_mgl.accid_col,True,True]
                ],
                filter_type='str'
            )
            resu_array.append(
                [accid,acct_sum,self.acctmap[accid]]
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
        resu_mgl.accept_df(resu_array)
        resu_mgl.set_top_acct(top_accid_len=4,accna_split_by=r'/')
        if pvt_mode==True:
            pvt_data=resu_mgl.data.pivot_table(
                values=['drcr'],
                index=['top_accna']
            )
            resu_mgl.accept_df(pvt_data)
            pass
        if type_xl==False:
            return resu_mgl.data
        else:
            return resu_mgl
    pass
