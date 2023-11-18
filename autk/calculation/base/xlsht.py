#!/usr/bin/env python
# encoding = 'utf-8'
'''
Excel Sheet
'''
import pysnooper

import re
import os
from copy import deepcopy
from os.path import isfile,isdir
from xlrd import open_workbook
from openpyxl import load_workbook
from pandas import concat,DataFrame,read_excel

from autk.gentk.funcs import transType,get_time_str
from autk.mapper.base import XlMap
from autk.meta.pmeta import PathMeta,JsonMeta
from autk.brother.xlbk import XlBook

class XlSheet:
    '''
    self.data is determined by self.xlmap and
    self.shmeta;
        arguments  |shmeta=JsonMeta  |shmeta=None
        -----------|-----------------|-----------
        xlmap=XlMap|load normally    |load blank DataFrame
        xlmap=None |get xlmap from df|self.data=None
    '''
    def __init__(
        self,
        xlmap:XlMap=None,#XlMap(),
        shmeta:PathMeta=None#JsonMeta({'BLANK_PATH':[['sheet',0]]}),
    ):
        '''
        XlSheet fills all blanks with float zero.
        '''
        self.__row_temp=[]
        self.data=None
        self.shmeta=shmeta
        self.xlmap=xlmap
        self.load_raw_data()
        pass
    def __str__(self):
        return str(self.show)
    def __len__(self):
        return 1
    def __add__(
        self,
        other
    ):
        resu=self.blank_copy()
        resu.load_df_by_map(
            concat(
                [deepcopy(self.data),deepcopy(other.data)],
                axis=0,
                join='outer'
            ),
            xlmap=deepcopy(other.xlmap)
        )
        return resu
    @property
    def name(self):
        name_str='None#None#0' if self.shmeta is None else '#'.join([
            str(list(self.shmeta.data.keys())[0]).split(os.sep)[-1],
            self.shmeta.data[list(self.shmeta.data.keys())[0]][0][0],
            str(self.shmeta.data[list(self.shmeta.data.keys())[0]][0][1]),
        ])
        return name_str
    @property
    def show(self):
        show={
            "calculator":{
                self.__class__.__name__:
                self.name
            },
            "data_size":(
                self.data.shape 
                if isinstance(self.data,DataFrame) else None
            ),
            "xlmap":{
                #  self.xlmap.__class__.__name__:
                str(type(self.xlmap)):
                None if self.xlmap is None else self.xlmap.show
            },
            "shmeta":{
                self.shmeta.__class__.__name__:
                #  str(type(self.shmeta)):
                None if self.shmeta is None else self.shmeta.data
            }
        }
        return show
    def __set_key_from_map(self):
        if self.xlmap.has_cols(
                [self.xlmap.key_name]+self.xlmap.key_index
        ):
            print(
                '[{}|{}] key_name `{}` is set from key_index:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.key_name,
                    self.xlmap.key_index
                )
            )
            def __join_key(row_series):
                return '-'.join(
                    str(row_series[sub_index]) 
                    for sub_index in getattr(self.xlmap,'key_index')
                )
            self.apply_df_func(
                __join_key,
                getattr(self.xlmap,'key_name'),
                col_index=None,
            )
        else:
            print(
                '[Warning][{}|{}] check `key_index`:'.format(
                    self.__class__.__name__,
                    self.name
                ),
                'key in xlmap:{}/{}'.format(
                    self.xlmap.key_name,
                    self.xlmap.key_index
                ),
                'columns in xlmap:{}'.format(self.xlmap.columns)
            )
        pass
    def __clear_row_temp(self):
        self.__row_temp=[]
        pass
    def clear(self):
        self.__clear_row_temp()
        self.data=None
        pass
    @property
    def rawdf(self):
        '''
        returns:
        arguments  |shmeta=JsonMeta  |shmeta=None
        -----------|-----------------|-----------
        xlmap=XlMap|Raw DataFrame    |blank-DataFrame
        xlmap=None |Raw DataFrame    |None
        '''
        if isinstance(self.shmeta,JsonMeta):
            path=str(list(self.shmeta.data.keys())[0])
            d=XlBook(path).get_df(
                self.shmeta.data[path][0][0],
                title=self.shmeta.data[path][0][1]
            )
            d.fillna(0.0,inplace=True)
            return d
        else:
            if isinstance(self.xlmap,XlMap):
                d=DataFrame(
                    data=[],
                    columns=self.xlmap.columns,
                )
                d.fillna(0.0,inplace=True)
                return d
            else:
                return None
    def load_raw_data(self):
        '''
        self.data may be:
        arguments  |shmeta=JsonMeta  |shmeta=None
        -----------|-----------------|-----------
        xlmap=XlMap|load normally    |load blank DataFrame
        xlmap=None |get xlmap from df|self.data=None
        '''
        if isinstance(self.shmeta,JsonMeta):# and path != 'BLANK_PATH':
            path=str(list(self.shmeta.data.keys())[0])
            if isinstance(self.xlmap,XlMap):
                print(
                    '[{}|{}] data loaded by map `{}`.'.format(
                        self.__class__.__name__,
                        self.name,
                        self.xlmap.__class__.__name__
                    )
                )
                self.data=XlBook(path).get_mapdf(
                    self.shmeta.data[path][0][0],
                    self.xlmap,
                    title=self.shmeta.data[path][0][1]
                )
                self.__set_key_from_map()
            else:
                print(
                    '[{}|{}] data loaded without XlMap.'.format(
                        self.__class__.__name__,
                        self.name
                    )
                )
                self.data=XlBook(path).get_df(
                    self.shmeta.data[path][0][0],
                    title=self.shmeta.data[path][0][1]
                )
                self.xlmap=XlMap.from_list(list(self.data.columns))
        else:
            if isinstance(self.xlmap,XlMap):
                print(
                    '[{}|{}] blank sheet created by map `{}`.'.format(
                        self.__class__.__name__,
                        self.name,
                        self.xlmap.__class__.__name__
                    )
                )
                self.data=DataFrame(
                    data=[],
                    columns=self.xlmap.columns,
                )
                self.data.fillna(0.0,inplace=True)
                pass
            else:
                print('[Warning][{}|{}] `None` data loaded.'.format(
                    self.__class__.__name__,
                    self.name
                ))
                self.data=None
        if self.data is not None:
            self.data.fillna(0.0,inplace=True)
        pass
    def blank_copy(self):
        '''
        `self.xlmap` will be deepcopy.
        self.data is blank-DataFrame if 
        self.xlmap is not None;
        else self.data=None;
        '''
        return self.__class__(
            xlmap=deepcopy(self.xlmap),
            shmeta=None
        )
    def df_copy(self,df:DataFrame):
        xl=self.__class__(
            xlmap=self.xlmap.__class__.from_list(
                list(df.columns)
            ),
            shmeta=None
        )
        xl.load_df_by_map(
            df,
            xlmap=None
        )
        return xl
    def transform_df(self,df):
        '''
        Transform the input df so that 
        it fits with self.xlmap;
        Used when isinstance(self.xlmap,XlMap);
        self.xlmap must be an instance of XlMap;
        This method does not change self.xlmap.
        '''
        source_cols=list(df.columns.to_numpy())
        if self.xlmap.has_cols(source_cols):
            print(
                '[{}|{}] columns fits with xlmap.'.format(
                    self.__class__.__name__,
                    self.name,
                )
            )
        else:
            print(
                '[{}|{}] gets `DataFrame` with cols:{}'.format(
                    self.__class__.__name__,
                    self.name,
                    source_cols,
                ),
            )
        if isinstance(self.xlmap,XlMap):
            data=DataFrame(
                [],
                columns=self.xlmap.columns
            )
            for col in self.xlmap.columns:
                if col in source_cols:
                    data[col]=deepcopy(
                        df[col]
                    )
                continue
            return data
        else:
            print(
                '[Warning][{}|{}] transform failed.check `self.xlmap`:{} '.format(
                    self.__class__.__name__,
                    self.name,
                    self.xlmap.show
                ),
            )
            return None
    def load_df_by_map(self,df,xlmap:XlMap=None):
        '''
        This function replaces self.data with `df` passed as argument.
        # FutureWarning: Support for multi-dimensional indexing (e.g. `obj[:, None]`) 
        # is deprecated and will be removed in a future version.  
        # Convert to a numpy array before indexing instead:
        '''
        if isinstance(self.xlmap,XlMap):
            if isinstance(xlmap,XlMap):
                if xlmap.columns==self.xlmap.columns:
                    #self.transform_df(df)
                    pass
                else:
                    self.xlmap.accept_json(
                        xlmap.show,
                        over_write=False
                    )
            else:
                self.xlmap.extend_col_list(
                    list(df.columns)
                )
            self.data=self.transform_df(df)
        else: # self.xlmap=None
            if isinstance(xlmap,XlMap):
                self.xlmap=xlmap
                self.data=self.transform_df(df)
            else:
                self.xlmap=XlMap.from_list(list(
                    df.columns
                ))
                self.data=df
        self.data.fillna(0.0,inplace=True)
    def get_row_temp_data(
        self,
        over_write=False,
        type_xl=False
    ):
        '''
        Get current concatenated data of self.__row_temp, then clear it;
        '''
        if len(self.__row_temp)==0:
            temp_data=DataFrame([],columns=self.xlmap.columns)
        else:
            temp_data=concat(
                self.__row_temp,
                axis=1
            ).T
        if over_write==True:
            self.data=temp_data
        if type_xl==True:
            temp_xl=self.blank_copy()
            temp_xl.load_df_by_map(temp_data)
            resu=temp_xl
        else:
            resu=temp_data
        self.__clear_row_temp()
        return resu
    def check_cols(self):
        '''
        Check if `self.xlmap.columns` corresponds
        with `self.data.columns`;
        '''
        if (
            isinstance(self.data,DataFrame) 
            and isinstance(self.xlmap,XlMap)
        ):
            return self.xlmap.has_cols(
                list(self.data.columns)
            )
        else:
            print(
                '[Warning][{}|{}] columns-check failed, check data or map.'.format(
                    self.__class__.__name__,
                    self.name,
                )
            )
            return False
    def append_col_name(self,col_name):
        self.xlmap.append_col_name(col_name)
        if self.xlmap.has_cols([col_name]):
            pass
        else:
            self.data.insert(
                self.xlmap.get_index(col_name),
                col_name,
                0
            )
        print(
            '[{}|{}] check cols after column `{}` appended:{}.'.format(
                self.__class__.__name__,
                self.name,
                col_name,
                self.check_cols(),
            ),
        )
        pass
    def extend_col_list(self,col_list):
        for col_name in col_list:
            self.append_col_name(col_name)
        pass
    def apply_df_func(
        self,
        df_apply_func,
        col_name:str,
        col_index:int=None, # this parameter is useless.
    ):
        '''
        If self.xlmap.show[col_name] is not None,
        new data will overwrite this column.
        Whatever you passed to `col_index` it is ignored currently.
        `col_index` is useless.
        '''
        if self.xlmap.has_cols([col_name]):
            self.data[col_name]=deepcopy(
                self.data.apply(
                    df_apply_func,
                    axis=1,
                    raw=False,
                    result_type=None
                    # result_type can be ['reduce','expand',None];
            ))
        else:
            # `col_name` not in self.xlmap.columns;
            # `col_index` cannot be assigned.
            # maybe self.xlmap.insert_col_name(col_name,col_index)
            # later.
            self.append_col_name(col_name)
            col_index=self.xlmap.get_index(col_name)
            self.data.insert(
                col_index,
                col_name,
                self.data.apply(
                    df_apply_func,
                    axis=1,
                    raw=False,
                    result_type='reduce'
                ),
                allow_duplicates=False
            )
        pass
    def change_dtype(self,col_name,target_type=str):
        self.data[col_name]=self.data[col_name].astype(target_type)
        return 0
    def change_float_to_str(self,col_name):
        def trans_row_dtype(row_series):
            return transType(row_series[col_name])
        self.data[col_name]=self.data.apply(trans_row_dtype, axis=1)
        return 0
    def __parse_str_match(self,condition_row,row_series):
        '''
        Return:bool
        '''
        regitem=condition_row[0]
        compare_col=condition_row[1]
        if_regex=condition_row[2]
        match_mode=condition_row[3]
        if compare_col not in row_series.index:
            return False
        else:
            compare_item=str(row_series[compare_col])
            if if_regex==False:
                return regitem == compare_item
            else: # if_regex == True
                regitem=transType(regitem)
                regitem=re.compile(regitem)
                if match_mode==True: # match mode;
                    b=re.match(regitem, compare_item)
                else: # match_mode == False, search mode;
                    b=re.search(regitem, compare_item)
                if b is not None:
                    return True
                else:
                    return False
    def __parse_num_compare(self,condition_row,row_series):
        '''
        Return:bool
        '''
        # print('num:',condition_row)
        num_col=str(condition_row[0])
        compare_type=condition_row[1]
        criteria=condition_row[2]
        num=row_series[num_col]
        if num_col not in row_series.index:
            return False
        elif compare_type=='>':
            return num > criteria
        elif compare_type=='<':
            return num < criteria
        elif compare_type=='=':
            return num == criteria
        elif compare_type=='>=':
            return num >= criteria
        elif compare_type=='<=':
            return num <= criteria
        elif compare_type=='<>' or '!=':
            return num != criteria
        else:
            return False
    def __parse_adv_match(self,row_series,str_con_row,num_con_row):
        '''
        Comprehensively match with string condition matrix and number condition matrix.
        Return:bool;
        '''
        if (
            self.__parse_str_match(
                str_con_row,
                row_series
            ) == True 
            and self.__parse_num_compare(
                num_con_row,
                row_series
            ) == True
        ):
            return True
        else:
            return False
    def filter_num(
        self,
        condition_matrix,
        over_write=False,
        type_xl=False
    ):
        '''
        for condition_row in condition_matrix:
            num_col=str(condition_row[0])
            compare_type=condition_row[1]
            criteria=condition_row[2]
        '''
        self.__clear_row_temp()
        if isinstance(condition_matrix, list) and len(condition_matrix)==3:
            condition_matrix=[condition_matrix]
        else:
            pass
        for i in self.data.iterrows():
            row_data=i[1]
            condition_set=[]
            for condition_row in condition_matrix:
                condition_set.append(
                    self.__parse_num_compare(condition_row,row_data)
                )
                continue
            if condition_set==[True]*len(condition_set):
                self.__row_temp.append(row_data)
            else:
                pass
            continue
        resu=self.get_row_temp_data(
            over_write=over_write,
            type_xl=type_xl
        )
        self.__clear_row_temp()
        return resu
    def filter_str(
        self,
        condition_matrix,
        over_write=False,
        type_xl=False
    ):
        '''
        for condition_row in condition_matrix:
            [find_str, in_which_col,if_regex,match_mode]
            regitem=condition_row[0]
            compare_col=condition_row[1]
            if_regex=condition_row[2]
            match_mode=condition_row[3]
        '''
        self.__clear_row_temp()
        if (
            isinstance(condition_matrix, list) 
            and len(condition_matrix)==4
        ):
            condition_matrix=[condition_matrix]
        else:
            pass
        for i in self.data.iterrows():
            row_data=i[1]
            condition_set=[]
            for condition_row in condition_matrix:
                condition_set.append(
                    self.__parse_str_match(condition_row,row_data)
                )
                continue
            if condition_set==[True]*len(condition_set):
            # if condition_set==[True]*len(condition_matrix):
                self.__row_temp.append(row_data)
            else:
                pass
            continue
        resu=self.get_row_temp_data(over_write=over_write,type_xl=type_xl)
        return resu
    def filter_adv(
        self,
        condition_dict,
        over_write=False,
        type_xl=False
    ):
        self.__clear_row_temp()
        str_condition_matrix=condition_dict['string']
        if isinstance(str_condition_matrix, list) and len(str_condition_matrix)==4:
            str_condition_matrix=[str_condition_matrix]
        num_condition_matrix=condition_dict['number']
        if isinstance(num_condition_matrix, list) and len(num_condition_matrix)==3:
            num_condition_matrix=[num_condition_matrix]
        for i in self.data.iterrows():
            row_data=i[1]
            condition_set=[]
            for str_con in str_condition_matrix:
                for num_con in num_condition_matrix:
                    condition_set.append(
                        self.__parse_adv_match(row_data,str_con,num_con)
                    )
                    continue
            if condition_set==[True]*(len(condition_set)):
            # if condition_set==[True]*(len(str_condition_matrix)*len(num_condition_matrix)):
                self.__row_temp.append(row_data)
            else:
                pass
            continue
        resu=self.get_row_temp_data(
            over_write=over_write,
            type_xl=type_xl
        )
        return resu
    def filter(
        self,
        condition,
        filter_type='adv',
        over_write=False,
        type_xl=False
    ):
        '''
        parameter:
            filter_type: type = 'str','num', or 'adv','adv' in default.
            condition: for type 'adv', condition is like:
                {
                    'string':[[]],
                    'number':[[]]
                }
            over_write:bool, default to False.
        return:
            pandas.core.frame.DataFrame.
        '''
        if filter_type=='str':
            return self.filter_str(
                condition,
                over_write=over_write,
                type_xl=type_xl
            )
        elif filter_type=='num':
            return self.filter_num(
                condition,
                over_write=over_write,
                type_xl=type_xl
            )
        elif filter_type=='adv':
            return self.filter_adv(
                condition,
                over_write=over_write,
                type_xl=type_xl
            )
        else:
            pass
    def filter_list(
        self,
        target_list,
        search_col,
        over_write=False,
        type_xl=False
    ):
        '''
        Get all records whose value of `search_col` in
        `target_list`.
        '''
        self.__clear_row_temp()
        for row in self.data.iterrows():
            row_data=row[1]
            if row_data[search_col] in target_list:
                # need deep copy of row_data?
                self.__row_temp.append(row_data)
            continue
        resu=self.get_row_temp_data(
            over_write=over_write,
            type_xl=type_xl
        )
        return resu
    def filter_regex_list(
        self,
        regex_list,
        search_col,
        match_mode=False,
        over_write=False,
        type_xl=False
    ):
        self.__clear_row_temp()
        def multi_regex_match(the_str,regex_list):
            '''
            Any of regex_list matches 'the_str', returns True;
            '''
            logic_resu=[]
            for regex_str in regex_list:
                regex_str=re.compile(regex_str)
                if match_mode==False:
                    logic_resu.append(
                        re.search(regex_str,the_str) is not None
                    )
                else:
                    logic_resu.append(
                        re.match(regex_str,the_str) is not None
                    )
                continue
            if True in logic_resu:
                return True
            else:
                return False
        for row in self.data.iterrows():
            row_data=row[1]
            the_str=str(row_data[search_col])
            if multi_regex_match(the_str,regex_list)==True:
                self.__row_temp.append(row_data)
        resu=self.get_row_temp_data(over_write=over_write,type_xl=type_xl)
        return resu
    def vlookup(
        self,
        find_str,
        key_col,
        resu_col=None,
        if_regex=True,
        match_mode=True,
        unique=False
    ):
        if resu_col is None:
            resu_col=key_col
        d=self.filter(
            [[find_str,key_col,if_regex,match_mode]],
            filter_type='str',
            over_write=False,
            type_xl=False
        )
        resu=d[resu_col]
        if unique==True:
            resu=list(
                set(resu)
            )
        return resu
    def vlookups(
        self,
        resu_col:str,
        condition_matrix,
        filter_type:str='adv',
        unique:bool=False
    )->list:
        '''
        parameters:
            resu_col,
            condition_matrix,
            filter_type='adv',
            unique=False
        return:
            list
        '''
        if resu_col is None:
            resu_col=self.key_name
        d=self.filter(
            condition_matrix,
            filter_type=filter_type,
            over_write=False,
            type_xl=False
        )
        resu=d[resu_col]
        if unique==True:
            resu=set(resu)
        resu=list(resu)
        return resu
    def sumifs(
        self,
        sum_col,
        condition,
        filter_type='adv'
    ):
        '''
        condition can be matrix or dict;
        '''
        values_to_sum=self.filter(
            condition,
            filter_type=filter_type,
            over_write=False,
            type_xl=False
        )[sum_col]
        return sum(values_to_sum)
    def split(self,by:str)->list:
        resu=[]
        if self.xlmap.has_cols([by]):
            from threading import Thread
            from autk.gentk.funcs import start_thread_list
            value_list=self.vlookups(
                by,
                [['.*',by,True,False]],
                filter_type='str',
                unique=True,
            )
            def __sub_filter(sub_v):
                sub_xl=self.blank_copy()
                sub_xl.load_df_by_map(
                    deepcopy(self.data),
                    xlmap=self.xlmap
                )
                sub_xl.filter(
                    [[sub_v,by,True,True]],
                    filter_type='str',
                    over_write=True,
                    type_xl=False
                )
                resu.append(
                    sub_xl
                )
                pass
            thli=[]
            for sub_v in value_list:
                thli.append(
                    Thread(
                        target=__sub_filter,
                        args=(sub_v,)
                    )
                )
                continue
            start_thread_list(thli)
            pass
        else:
            print(
                '[Warning][{}|{}] check if `{}` is in columns:{}.'.format(
                    self.__class__.__name__,
                    self.name,
                    by,
                    self.xlmap.columns
                )
            )
        return resu
    def to_table(self,by:str):
        from autk.calculation.base.table import ImmortalTable
        table=ImmortalTable(
            xlmap=self.xlmap,
            xlmeta=None
        )
        table.xlset=self.split(by)
        return table
    ### the following can be derived by both `CalSheet` and `CalChart`;
    def trans_accid_regex(self,accid,accurate=False):
        '''
        r'^'+accid+r'.*$' if not accurate, as default,
        or r'^\s*'+accid+r'\s*$' if accurate is True;
        Before/After the 'accid':
        If accurate, only space will be allowed;
        If not accurate, any str will be allowed;
        
        '''
        import re
        if accurate==False:
            accid_item=str(accid).join([r'^.*',r'.*$'])
        else:
            accid_item=str(accid).join([r'^\s*',r'\s*$',])
        # accid_item=re.sub(r'\.',r'\.',accid_item)
        return accid_item
    def trans_accna_regex(self,accna,accurate=False):
        import re
        if accurate==False:
            accna_item=accna.join([r'^.*',r'.*$'])
        else:
            accna_item=accna.join([r'^\s*',r'\s*$'])
        return accna_item
    def whatna(self,accna_str):
        '''
        parameters:
            accna_item:str
                regular expression is supported.
        return:dict
            print DataFrame but return dict;
            {accid:accna,accid:accna...}
        '''
        if not hasattr(self,'acctmap'):
            print(
                '[{}|{}}] attribute `{}` does not exist.'.format(
                    self.__class__.__name__,
                    self.name,
                    'acctmap'
                )
            )
            return {}
        else:
            from autk.gentk.funcs import regex_filter
            accna_item=self.trans_accna_regex(accna_str,accurate=False)
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
                '[{}|{}]`{}` may be:\n\t{}'.format(
                    self.__class__.__name__,
                    self.name,
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
        if not hasattr(self,'acctmap'):
            print(
                '[{}|{}] attribute `{}` does not exist.'.format(
                    self.__class__.__name__,
                    self.name,
                    'acctmap'
                )
            )
            return {}
        else:
            from autk.gentk.funcs import regex_filter
            accid_item=self.trans_accid_regex(accid_str,accurate=False)
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
                '[{}|{}]`{}` may be:\n\t{}'.format(
                    self.__class__.__name__,
                    self.name,
                    accid_str,
                    acct
                )
            )
            return acct
    ### the above can be derived by both `CalSheet` and `CalChart`;
    ### below are not perfect ???
    def percentage(self,percent_col_name,target_col_name):
        '''
        to calculate percentage for each item in a column;
        '''
        target_sum=self.data[target_col_name].sum()
        def __percent_cal(row_series):
            return row_series[target_col_name]/target_sum
        self.apply_df_func(
            __percent_cal,
            percent_col_name,
        )
        pass
    ### above are not perfect.
    pass
if __name__=='__main__':
    pass
