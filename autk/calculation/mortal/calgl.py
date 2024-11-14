#!/usr/bin/env python
# -*- coding: utf-8 -*-


from copy import deepcopy
from threading import Thread
from pandas import DataFrame,concat

from autk.gentk.funcs import transType,regex_filter,save_df,start_thread_list
from autk.mapper.glmap import MglMap
from autk.meta.pmeta import JsonMeta,PathMeta
from autk.calculation.base.xlsht import XlSheet

class CalSheet(XlSheet):
    '''
    If xlmap is passed None,then columns will be:
        ['glid','date','mark','jrid','accid','accna','dr_amount','cr_amount','drcr','item_name','note']
    '''
    def __init__(
        self,
        xlmap:MglMap=None,#MglMap(),
        shmeta:PathMeta=None,#JsonMeta({'BLANK_PATH':[['sheet',0]]})
    ):
        self.acctmap={}
        self.acctmap_invert={}
        super().__init__(
            xlmap=xlmap,
            shmeta=shmeta,
        )
        pass
    def __add__(self,other):
        resu=super().__add__(other)
        resu.acctmap=self.acctmap+other.acctmap
        resu.acctmap_invert=other.acctmap_invert
        return resu
    def load_raw_data(self):
        super().load_raw_data()
        if (
            isinstance(self.xlmap,MglMap)
            and isinstance(self.shmeta,JsonMeta)
        ):
            print(
                '[{}] setting acct map.'.format(
                    self.__class__.__name__,
                )
            )
            self.__set_drcr()
            self.__set_acctmap()
            ## set_top and set_date are optional.
            self.__set_top()
            self.__set_date()
    def __set_drcr(self):
        if self.xlmap.has_cols(self.xlmap.drcrdesc):
            self.change_dtype(
                self.xlmap.drcrdesc[0],
                target_type=float
            )
            self.change_dtype(
                self.xlmap.drcrdesc[1],
                target_type=float
            )
        else:
            print(
                '[Warning][{}|{}] check `dr/cr`:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.columns
                ),
            )
        def __cal_drcr(row_series):
            dr_col=self.xlmap.drcrdesc[0]
            cr_col=self.xlmap.drcrdesc[1]
            dr_amt=row_series[dr_col]
            cr_amt=row_series[cr_col]
            if (
                isinstance(dr_amt,float) 
                and 
                isinstance(cr_amt,float)
            ):
                return dr_amt-cr_amt
            else:
                return 0.0
        if self.xlmap.has_cols(['drcr']):
            pass
        else:
            print(
                '[{}|{}] check `drcr`:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.has_cols(['drcr']),
                ),
            )
        self.apply_df_func(
            __cal_drcr,
            'drcr',
        )
        pass
    def __set_acctmap(self):
        '''
        '''
        if self.xlmap.has_cols([
            self.xlmap.accid_col,
            self.xlmap.accna_col,
        ]) and isinstance(self.data,DataFrame):
            self.change_dtype(self.xlmap.accid_col,int)
            self.change_dtype(self.xlmap.accid_col,str)
            self.change_dtype(self.xlmap.accna_col,str)
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
            pass
        else:
            print(
                '[Warning][{}|{}] check accid/accna or data:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.has_cols([
                        self.xlmap.accid_col,
                        self.xlmap.accna_col,
                    ]),
                ),
            )
        pass
    def __set_top(self):
        if (
                hasattr(self.xlmap,'top_accid_len')
            and self.xlmap.has_cols([
                getattr(self.xlmap,'accid_col'),
                getattr(self.xlmap,'top_accid_col'),
            ])
        ):
            print(
                '[{}|{}] check accid:ok'.format(
                    self.__class__.__name__,
                    self.name,
                )
            )
            self.change_dtype(
                self.xlmap.accid_col,
                str
            )
            def __set_top_accid(row_series):
                accid=row_series[self.xlmap.accid_col]
                top_accid=accid[0:self.xlmap.top_accid_len]
                return top_accid
            self.apply_df_func(
                __set_top_accid,
                self.xlmap.top_accid_col
            )
        else:
            print(
                '[Warning][{}|{}] check your xlmap:{}.'.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.show
                )
            )
        if (
                hasattr(self.xlmap,'accna_split_by')
            and self.xlmap.has_cols([
                getattr(self.xlmap,'accna_col'),
                getattr(self.xlmap,'top_accna_col')
            ])
        ):
            print(
                  '[{}|{}] check accna:ok'.format(
                      self.__class__.__name__,
                      self.name
                  )
                  )
            def __set_top_accna(row_series):
                accna=str(
                    row_series[self.xlmap.accna_col])
                top_accna=accna.split(
                    self.xlmap.accna_split_by
                )[0]
                return top_accna
            self.apply_df_func(
                __set_top_accna,
                self.xlmap.top_accna_col
            )
        else:
            print(
                '[Warning][{}|{}] check your xlmap:{}.'.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.show
                )
            )
        pass
    def __set_date(self):
        if (hasattr(self.xlmap,'date_split_by')
        and self.xlmap.has_cols([
            getattr(self.xlmap,'date_col'),
        ])):
            print(
                '[{}|{}] check date column:ok'.format(
                    self.__class__.__name__,
                    self.name,
                )
            )
        pass
    #  def set_top_acct(
        #  self,
        #  top_accid_len=4,
        #  accna_split_by=r'/'
    #  ):
        #  setattr(self,'top_accid_len',top_accid_len)
        #  setattr(self,'accna_split_by',accna_split_by)
        #  if self.shmeta[0] is not None:
            #  self.change_float_to_str(self.xlmap.accid_col)
            #  self.change_float_to_str(self.xlmap.accna_col)
            #  def top_accid_apply_func(row_series):
                #  accid=row_series[self.xlmap.accid_col]
                #  top_accid=accid[0:self.xlmap.top_accid_len]
                #  return top_accid
            #  def top_accna_apply_func(row_series):
                #  accna=row_series[self.xlmap.accna_col]
                #  top_accna=accna.split(self.xlmap.accna_split_by)[0]
                #  top_accna=str(top_accna)
                #  return top_accna
            #  self.apply_df_func(top_accid_apply_func,self.xlmap.top_accid)
            #  self.apply_df_func(top_accna_apply_func,self.xlmap.top_accna)
        #  pass
    #  def set_date(self,date_col='date',date_split_by=r'-'):
        #  def __get_year(row_series):
            #  if self.use_map==True:
                #  date_str=row_series[self.xlmap.date_col]
            #  else:
                #  date_str=row_series[date_col]
            #  if isinstance(date_str,str) and date_str != '0.0':
                #  year=transType(date_str.split(date_split_by)[0])
            #  else:
                #  year='0'
            #  return year
        #  def __get_month(row_series):
            #  if self.use_map==True:
                #  date_str=row_series[self.xlmap.date_col]
            #  else:
                #  date_str=row_series[date_col]
            #  if isinstance(date_str,str) and (date_str != '0.0' or date_str != 0.0):
                #  month=transType(date_str.split(date_split_by)[1])
            #  else:
                #  month='0'
            #  return month
        #  def __get_day(row_series):
            #  if self.use_map==True:
                #  date_str=row_series[self.xlmap.date_col]
            #  else:
                #  date_str=row_series[date_col]
            #  if isinstance(date_str,str) and date_str != '0.0':
                #  day=transType(date_str.split(date_split_by)[2])
            #  else:
                #  day='0'
            #  return day
        #  if self.use_map==True:
            #  setattr(self,'date_col',self.xlmap.date_col)
            #  setattr(self,'date_split_by',self.xlmap.date_split_by)
        #  else:
            #  setattr(self,'date_col',date_col)
            #  setattr(self,'date_split_by',date_split_by)
        #  if self.shmeta[0] is not None and date_col in self.columns:
            #  date_col_index=list(self.columns).index(date_col)
            #  self.change_dtype(date_col,target_type=str)
            #  self.apply_df_func(__get_year,'year',date_col_index)
            #  self.apply_df_func(__get_month,'month',date_col_index+1)
            #  self.apply_df_func(__get_day,'day',date_col_index+2)
        #  pass
    #  def duplicate(self,use_meta=False):
        #  from copy import deepcopy
        #  calculator=deepcopy(self)
        #  if use_meta==False:
            #  calculator.clear()
        #  return calculator
    #  def accept_df(self,in_df):
        #  '''
        #  if you code 'self.data=in_df',
        #  index/columns of them may differs,
        #  yet inner data will be the same;
        #  '''
        #  super().accept_df(in_df)
        #  self.__parse_acctmap()
    #  def calxl(self,df=None):
        #  cal=CalSheet(
            #  shmeta=[None,self.sheet_name,self.title],
            #  file_path=None,
            #  sheet_name='sheet0',
            #  title=0,
            #  key_index=deepcopy(self.xlmap.key_index),
            #  key_name=deepcopy(self.xlmap.key_name),
            #  drcrdesc=deepcopy(self.xlmap.drcrdesc),
            #  accid_col=deepcopy(self.xlmap.accid_col),
            #  accna_col=deepcopy(self.xlmap.accna_col),
            #  date_col=deepcopy(),
            #  date_split_by=self.date_split_by,
            #  xlmap=deepcopy(self.xlmap),
            #  use_map=self.use_map,
            #  keep_meta_info=self.keep_meta_info,
        #  )
        #  cal.accept_df(df)
        #  return cal
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
            print(
                '[{}|{}] cannot find:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    accna_item
                ),
            )
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
            print(
                '[{}|{}] cannot find:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    accid_item
                ),
            )
            pass
        elif len(accid_list) >0:
            for accid in accid_list:
                sub_resu=__accurate_scan(accid,acct_json)
                resu.append(sub_resu)
        else:
            pass
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
            #  drcr_sum=drcr_data[self.xlmap.drcrdesc].sum(axis=0)
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
                #  self.xlmap.accid_col,
                #  self.xlmap.drcrdesc[0],
                #  self.xlmap.drcrdesc[1],
                #  self.xlmap.accna_col
            #  ]
        #  )
        #  return resu
    def filterAcct(
        self,
        accid_item,
        side:str='all',
        pure:bool=False,
        accurate:bool=False,
        over_write:bool=False,
        type_xl:bool=False,
        accid_label:str=None
    ):
        accid_item=self.trans_accid_regex(accid_item,accurate=accurate)
        if isinstance(self.xlmap,MglMap) and accid_label is None:
            accid_label=self.xlmap.accid_col
        if pure==True:
            if side=='all':
                resu=self.filter(
                    [[accid_item,self.xlmap.accid_col,True,True]],
                    filter_type='str',
                    over_write=over_write,
                    type_xl=type_xl
                )
                pass
            elif side=='dr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        'number':[[self.xlmap.drcrdesc[0],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=over_write,
                    type_xl=type_xl
                )
                pass
            elif side=='cr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        'number':[[self.xlmap.drcrdesc[1],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=over_write,
                    type_xl=type_xl
                )
                pass
            else:
                resu=None
                print(
                    '[Error][{}|{}] Invalid argument, filter None will not overwrite.'.format(
                        self.__class__.__name__,
                        self.name,
                    )
                )
                pass
            pass
        elif pure==False:
            if side=='all':
                resu=self.filter(
                    [[accid_item,self.xlmap.accid_col,True,True]],
                    filter_type='str',
                    over_write=False,
                    type_xl=False
                )
                pass
            elif side=='dr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        'number':[[self.xlmap.drcrdesc[0],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=False,
                    type_xl=False
                )
                pass
            elif side=='cr':
                resu=self.filter(
                    {
                        'string':[[accid_item,self.xlmap.accid_col,True,True]],
                        'number':[[self.xlmap.drcrdesc[1],'<>',0]]
                    },
                    filter_type='adv',
                    over_write=False,
                    type_xl=False
                )
                pass
            else:
                resu=None
                print(
                    '[Error][{}|{}] Invalid argument, got None; data not overwritten;'.format(
                        self.__class__.__name__,
                        self.name,
                    )
                )
                pass
            acct_glid_list=list(
                    resu[self.xlmap.key_name].drop_duplicates()
            )
            if len(acct_glid_list)==0:
                resu=None
            else:
                resu=self.filter_list(
                    acct_glid_list,
                    search_col=self.xlmap.key_name,
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
            key_name=self.xlmap.key_name
        item_data=self.filter(
            [[regex_str,by,True,False]],
            'str',
            over_write=False,
            type_xl=False
        )
        #  item_data=self.xl2df(item_data)
        key_list=list(
            item_data[self.xlmap.key_name].drop_duplicates()
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
                print(
                    '[{}|{}] single_acct_analysis: got blank DataFrame:\n{}'.format(
                        self.__class__.__name__,
                        self.name,
                        df
                    )
                )
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
        common_key_list=list(
            set(
                dr[self.xlmap.key_name]
            )&set(
                cr[self.xlmap.key_name]
            )
        )
        resu=self.filter_list(
            common_key_list,
            self.xlmap.key_name,
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
            self.xlmap.drcrdesc[0],
            [[dr_accid,self.xlmap.accid_col,True,True]],
            filter_type='str'
        )
        cr_sum=common_xl.sumifs(
            self.xlmap.drcrdesc[1],
            [[cr_accid,self.xlmap.accid_col,True,True]],
            filter_type='str'
        )
        cr2dr_sum=DataFrame(
            [[dr_sum,0],[0,cr_sum]],
            index=[dr_accid,cr_accid],
            columns=self.xlmap.drcrdesc
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
            )[self.xlmap.key_name]
        )
        glid_list=list(glid_list)
        return glid_list
    def find_opposite(self,col_index,col_name='opposite_accid'):
        #TODO
        pass
    def acct_side_split(self,accid,side='cr'):
        '''
        accid is just accid, not regex;
        '''
        #  analysis_unit=self.duplicate(use_meta=True)
        analysis_unit=self.blank_copy()
        analysis_unit.load_df_by_map(self.data)
        # start to split-analysis:
        analysis_unit.filterAcct(
            accid,
            side=side,
            pure=False,
            accurate=True,
            over_write=True,
            type_xl=False,
            accid_label=self.xlmap.accid_col
        )
        def __acct_mark_side(row_series):
            if side=='cr':
                side_name=analysis_unit.xlmap.drcrdesc[1]
            elif side=='dr':
                side_name=analysis_unit.xlmap.drcrdesc[0]
            else:
                # 'cr' side is on default;
                side_name=analysis_unit.xlmap.drcrdesc[1]
            side_amount=row_series[side_name]
            current_accid=row_series[analysis_unit.xlmap.accid_col]
            if current_accid==accid and side_amount !=0:
                return 'target_acct'
            else:
                return 'opposite_acct'
        analysis_unit.apply_df_func(__acct_mark_side,'mark_record',1)
        resu_cols=[
            # used to print results,
            # not for `resu_unit`;
            #  'top_accid','top_accna',
            analysis_unit.xlmap.top_accid_col,
            analysis_unit.xlmap.top_accna_col,
            analysis_unit.xlmap.accid_col,
            analysis_unit.xlmap.accna_col,
            str(accid)
        ]
        # opposite account id:
        oppo_accid_list=list(
            set(
                analysis_unit.data[analysis_unit.xlmap.accid_col]
            )
        )
        oppo_glid_list=list(
            set(
                analysis_unit.data[analysis_unit.xlmap.key_name]
            )
        )
        resu_array=[]
        def __oppo_sum_collect(oppo_accid):
            # opposite account amount sum;
            oppo_amt_sum=analysis_unit.sumifs(
                'drcr',[
                    ['opposite_acct','mark_record',True,True],
                    [oppo_accid,analysis_unit.xlmap.accid_col,True,True]
                ],
                filter_type='str'
            )
            resu_array.append(
                # drcr-sum of oppo_accid of `accid`,oppo_accid,oppo_accna
                [oppo_amt_sum,oppo_accid,self.acctmap[oppo_accid]]
            )
            pass
        thli=[]
        for oppo_accid in oppo_accid_list:
            thli.append(
                Thread(
                    target=__oppo_sum_collect,
                    args=(oppo_accid,)
                )
            )
            continue
        start_thread_list(thli)
        resu_df=DataFrame(
            resu_array,
            columns=[
                str(accid),
                analysis_unit.xlmap.accid_col,
                analysis_unit.xlmap.accna_col
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
        # resu_df has 3 columns;
        resu_unit=self.blank_copy()
        resu_unit.xlmap.change_cols(resu_cols)
        resu_unit.load_df_by_map(resu_df)
        '''
        resu_unit has columns:
        ['accid',accid_col,accna_col],
        of which data is:
        [oppo_amt_sum,oppo_accid,oppo_accna]
        resu_df(last)/resu_array, data of resu_unit, has columns:
        ['top_accid','top_accna',accid_col,accna_col,this_accid],
        of which data is:
        [oppo_amt_sum,oppo_accid,oppo_accna]
        column data is named from target_accid;
        '''
        #  print('check resu cols:',resu_unit.xlmap.has_cols(resu_cols),resu_unit.xlmap.columns)
        resu_df=deepcopy(resu_unit.data[resu_cols])
        return resu_df
    def side_split(self,accid_item,side='cr',show_col='accna'):
        '''
        show_col must be one of :['top_accna','top_accid',self.xlmap.accid_col,self.xlmap.accna_col];
        '''
        tar_accid_list=list(self.whatid(accid_item).keys())
        if len(tar_accid_list)==0:
            resu_df=DataFrame([],)
        else:
            resu_dfli=[]
            for tar_accid in tar_accid_list:
                split_df=self.acct_side_split(tar_accid,side=side)
                split_df.fillna(0.0,inplace=True)
                resu_dfli.append(split_df)
                continue
            resu_df=concat(resu_dfli,axis=0,join='outer')
            resu_df.fillna(0.0,inplace=True)
            resu_cols=[show_col]
            resu_cols.extend(tar_accid_list)
            resu_df=resu_df[resu_cols]
            '''
            resu_df has columns:
            [show_col+tar_accid_list],
            '''
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

    #  def fake_side_analysis(self,accid,side='cr',pvt_mode=False,type_xl=False):
        #  '''
        #  accid must NOT be regex!
        #  Credit number is negative;
        #  Debit number is positive;
        #  '''
        #  accid_item=self.__trans_accid_regex(accid,accurate=True)
        #  accid_list=self.scan_byid(accid_item, accid_label=self.xlmap.accid_col)
        #  acct_mgl=self.duplicate(use_meta=True)
        #  resu_mgl=self.duplicate(use_meta=False)
        #  acct_mgl.filterAcct(
            #  accid_item,
            #  side=side,
            #  pure=False,
            #  accurate=True,
            #  over_write=True,
            #  type_xl=False,
            #  accid_label=None
        #  )
        #  if side=='cr':
            #  side_name=acct_mgl.xlmap.drcrdesc[1]
        #  elif side=='dr':
            #  side_name=acct_mgl.xlmap.drcrdesc[0]
        #  else:
            #  return [
                #  self.side_analysis(
                    #  accid,
                    #  side='dr'
                #  ),
                #  self.side_analysis(
                    #  accid,
                    #  side='cr'
                #  )
            #  ]
        #  def __acct_mark_side(row_series):
            #  current_accid=row_series[acct_mgl.xlmap.accid_col]
            #  side_amount=row_series[side_name]
            #  if current_accid==accid and side_amount !=0:
                #  return 'target_acct'
            #  else:
                #  return 'opposite_acct'
        #  acct_mgl.apply_df_func(__acct_mark_side,'mark_record',1)
        #  accid_list=list(
            #  set(
                #  acct_mgl.data[acct_mgl.xlmap.accid_col]
            #  )
        #  )
        #  resu_array=[]
        #  for accid in accid_list:
            #  acct_sum=acct_mgl.sumifs(
                #  'drcr',[
                    #  ['opposite_acct','mark_record',True,True],
                    #  [accid,acct_mgl.xlmap.accid_col,True,True]
                #  ],
                #  filter_type='str'
            #  )
            #  resu_array.append(
                #  [accid,acct_sum,self.acctmap[accid]]
            #  )
            #  continue
        #  resu_array=DataFrame(
            #  resu_array,
            #  columns=[acct_mgl.xlmap.accid_col,'drcr',acct_mgl.accna_col]
        #  )
        #  resu_array.sort_values(
            #  'drcr',
            #  axis=0,
            #  ascending=False, # if True,1 to 9;
            #  inplace=True,
            #  kind='quicksort',
            #  na_position='last',
            #  ignore_index=True,
            #  key=None
        #  )
        #  resu_mgl.accept_df(resu_array)
        #  resu_mgl.set_top_acct(top_accid_len=4,accna_split_by=r'/')
        #  if pvt_mode==True:
            #  pvt_data=resu_mgl.data.pivot_table(
                #  values=['drcr'],
                #  index=['top_accna']
            #  )
            #  resu_mgl.accept_df(pvt_data)
            #  pass
        #  if type_xl==False:
            #  return resu_mgl.data
        #  else:
            #  return resu_mgl
    pass
