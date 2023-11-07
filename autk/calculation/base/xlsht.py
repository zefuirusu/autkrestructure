#!/usr/bin/env python
# encoding = 'utf-8'
'''
Excel Sheet
'''

import re
import os
from copy import deepcopy
from os.path import isfile,isdir
from xlrd import open_workbook
from openpyxl import load_workbook
from pandas import concat,DataFrame,read_excel

from autk.gentk.funcs import transType,get_time_str
from autk.mapper.map import XlMap
from autk.meta.pmeta import PathMeta,JsonMeta
from autk.brother.xlbk import XlBook

class XlSheet:
    '''
    Map is not so important to XlSheet.
    '''
    def __init__(
        self,
        xlmap:XlMap=XlMap(),
        shmeta:PathMeta=JsonMeta({'BLANK_PATH':[['sheet',0]]}),
    ):
        '''
        XlSheet fills all blanks with float zero.
        '''
        self.__row_temp=[]
        self.shmeta=shmeta
        self.xlmap=xlmap
        self.load_raw_data()
        pass
    @property
    def show(self):
        show={}
        show.update({
            "ojb":self.__class__.__name__,
            "xlmap":{
                #  self.xlmap.__class__.__name__:
                str(type(self.xlmap)):
                self.xlmap.show
            },
            "shmeta":{
                self.shmeta.__class__.__name__:
                #  str(type(self.shmeta)):
                self.shmeta.data
            }
        })
        return show
    def __str__(self):
        return str(self.show)
    def __set_key_from_map(self):
        if self.xlmap.check_cols(
                [self.xlmap.key_name]+self.xlmap.key_index
        ):
            print(
                '[Note]:setting `key_name` ',
                getattr(self.xlmap,'key_name'),
                'from `key_index` ',
                getattr(self.xlmap,'key_index')
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
            print('[Warning]:key_index is not set properly:')
            print(
                "xlmap's key:",
                {self.xlmap.key_name:
                self.xlmap.key_index},
                'xlmap.columns:',
                self.xlmap.columns,
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
        if isinstance(self.shmeta,JsonMeta):
            path=str(list(self.shmeta.data.keys())[0])
            if isinstance(self.xlmap,XlMap):
                print(
                    '[Note]:{} loads data from {}.'.format(
                        self.__class__.__name__,
                        self.xlmap.__class__.__name__
                    )
                )
                self.data=XlBook(path).get_mapdf(
                    self.shmeta.data[path][0][0],
                    self.xlmap,
                    title=self.shmeta.data[path][0][1]
                )
            else:
                print(
                    '[Note]:{} created without XlMap.'.format(
                        self.__class__.__name__
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
                    '[Note]:{} created blank sheet.'.format(
                        self.__class__.__name__)
                )
                self.data=DataFrame(
                    data=[],
                    columns=self.xlmap.columns,
                )
                self.data.fillna(0.0,inplace=True)
                pass
            else:
                print('[Warning]:{} loaded no data.'.format(
                    self.__class__.__name__
                ))
                self.data=None
        self.__set_key_from_map()
        if self.data is not None:
            self.data.fillna(0.0,inplace=True)
        else:
            pass
        pass
    def transform_df(self,df):
        '''
        used when isinstance(self.xlmap,XlMap);
        self.xlmap must be an instance of XlMap;
        this function is quite simillar to XlBook.get_mapdf();
        '''
        from copy import deepcopy
        if isinstance(self.xlmap,XlMap):
            data=DataFrame(
                [],
                columns=self.xlmap.columns
            )
            for col in self.xlmap.columns:
                col_index=xlmap.show[col]
                col_from_source=df.columns.to_numpy()[col_index]
                if isinstance(col_index,int):
                    data[col]=deepcopy(
                        df[col_from_source]
                    )
                elif isinstance(col_index,list):
                    data[col]=0
                    for sub_col_index in col_index:
                        sub_col_from_source=df.columns[sub_col_index]
                        data[col]=deepcopy(
                            data[col]+df[sub_col_from_source]
                        )
                        continue
                else:
                    pass
                continue
            return data
        else:
            print('[Warning]:check self.xlmap, ',self.xlmap)
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
                self.xlmap.accept_json(
                    xlmap.show,
                    over_write=False
                )
            else:
                self.xlmap.extend_col_list(list(
                    df.columns
                ))
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
    def get_row_temp_data(self,over_write=False,type_xl=False):
        '''
        Get current concatenated data of self.__row_temp, then clear it;
        '''
        if len(self.__row_temp)==0:
            temp_data=DataFrame([],columns=self.data.columns)
        else:
            temp_data=concat(
                self.__row_temp,
                axis=1
            ).T
        if over_write==True:
            self.accept_df(temp_data)
        if type_xl==True:
            cal=self.duplicate(use_meta=False)
            cal.accept_df(temp_data)
            resu=cal
        else:
            resu=temp_data
        self.__clear_row_temp()
        return resu
    def apply_df_func(
        self,
        df_apply_func,
        col_name:str,
        col_index:int=None,
    ):
        '''
        If self.xlmap.show[col_name] is not None,
        new data will overwrite this column.
        Whatever you passed to `col_index` it is ignored currently.
        '''
        if self.xlmap.check_cols([col_name]):
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
            self.xlmap.append_col_name(col_name)
            col_index=self.xlmap.show[col_name]
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
        pass
    def change_float_to_str(self,col_name):
        def trans_row_dtype(row_series):
            return transType(row_series[col_name])
        self.data[col_name]=self.data.apply(trans_row_dtype, axis=1)
        pass
    def __parse_str_match(self,condition_row,row_series):
        '''
        Return:bool
        '''
        # print('str',condition_row)
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
            self.__parse_str_match(str_con_row, row_series) == True 
            and self.__parse_num_compare(num_con_row, row_series) == True
        ):
            return True
        else:
            return False
    def filter_list(
        self,
        target_list,
        search_col,
        over_write=False,
        type_xl=False
    ):
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
        #  if self.data is None:
            #  self.set_key()
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
            # if condition_set==[True]*len(condition_matrix):
                self.__row_temp.append(row_data)
            else:
                pass
            continue
        resu=self.get_row_temp_data(over_write=over_write,type_xl=type_xl)
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
        #  if self.data is None:
            #  self.set_key()
        if isinstance(condition_matrix, list) and len(condition_matrix)==4:
            condition_matrix=[condition_matrix]
        else:
            pass
        for i in self.data.iterrows():
            row_data=i[1]
            condition_set=[]
            for condition_row in condition_matrix:
                condition_set.append(self.__parse_str_match(condition_row,row_data))
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
        #  if self.data is None:
            #  self.set_key()
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
        resu=self.get_row_temp_data(over_write=over_write,type_xl=type_xl)
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
        resu_col,
        condition_matrix,
        filter_type='adv',
        unique=False
    ):
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
    def filter_key_record(self,condition_matrix):
        '''
        Expected to be perfect.
        Seems useless.
        Return all keys (data in column 'key_name') according to the passed argument condition_matrix.
        '''
        resu_keys=list(self.filter_str(condition_matrix)[self.key_name].drop_duplicates())
        return self.filter_list(resu_keys, self.key_name)
    ### above are not perfect.
    pass
if __name__=='__main__':
    pass
