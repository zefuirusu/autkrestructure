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
        xlmap:XlMap=None,
        shmeta:PathMeta=None,
        # structure of the table is less important than meta information, yet simply make keep_meta_info default to True.
        # key_index is not so important for class XlSheet, so simply make key_index and key_name as defalt.
    ):
        '''
        XlSheet fills all blanks with float zero.
        file_path,sheet_name and title:string
            Indicating the location of the workbook,sheet name and title row.
        key_index:list,default []
            Indicating which columns are joined to generate a key to identify every rows of the data.
        key_name:string,default 'key_id'
            Name of the new key.
        xlmap: class XlMap,default None
            If a map is needed, pass an instance of class XlMap to indicate location of each column.
            If not necessary, passing None is fine.
        keep_meta_info: bool,default True
            If False, columns of 'from_book' and 'from_sheet' will be dropped; else will be kept.
        Basic Structure of XlSheet on default:
        '''
        self.__row_temp=[]
        self.shmeta=shmeta # [None,'sheet0',0] as default
        self.xlmap=xlmap
        self.load_raw_data()
        # key index not set up till now.
        if self.data is not None:
            self.data.fillna(0.0,inplace=True)
        else:
            pass
        pass
    def __str__(self):
        return ''
    #  def __parse_meta(self,shmeta):
        #  '''
        #  doing nothing except setting attributes;
        #  '''
        #  if isinstance(shmeta,list):
            #  self.file_path=shmeta[0]
            #  self.sheet_name=shmeta[1]
            #  self.title=shmeta[2]
            #  self.__parse_file_name(self.file_path)
        #  elif isinstance(shmeta,DataFrame):
            #  self.file_path='no_file'
            #  self.sheet_name='df'
            #  self.title=0
            #  self.__parse_file_name(shmeta)
            #  pass
        #  else:
            #  self.file_path=None
            #  self.sheet_name=None
            #  self.title=0
            #  self.__parse_file_name(self.file_path)
            #  pass
        #  pass
    #  def __parse_file_name(self,file_path):
        #  '''
        #  must be called after self.__parse_meta;
        #  '''
        #  if file_path is None:
            #  self.file_name='None'
            #  self.pure_file_name=self.file_name
            #  self.sheet_name='sheet0'
            #  self.title=0
            #  self.suffix='None'
            #  pass
        #  elif isfile(file_path):
            #  self.file_name=str(file_path.split(os.sep)[-1])
            #  self.pure_file_name=re.sub(
                #  re.compile(r'\.xls[xm]?$'),
                #  '',
                #  self.file_name
            #  )
            #  self.suffix=re.sub(
                #  self.pure_file_name+r'.',
                #  '',
                #  self.file_name
            #  )
        #  elif isinstance(file_path,DataFrame):
            #  self.file_name='DataFrame'
            #  self.pure_file_name=self.file_name
            #  self.sheet_name='sheet0'
            #  self.title=0
            #  self.suffix='df'
            #  pass
        #  else:
            #  print(
                #  '[Warning:XlSheet] unknown file_path:\n',
                #  file_path
            #  )
            #  self.file_name='woc'
            #  self.pure_file_name='woc'
            #  self.sheet_name='woc'
            #  self.title=0
            #  self.suffix='woc'
            #  pass
        #  self.name=''.join(
            #  [self.pure_file_name,
            #  '-',
            #  self.sheet_name,
            #  '-',
            #  str(self.title)]
        #  )
        #  pass
    #  def read(self,shmeta):
        #  def create_blank():
            #  print('[Note:] Blank sheet created.')
            #  self.accept_df(None)
        #  if isinstance(shmeta,DataFrame):
            #  self.accept_df(shmeta[0])
        #  elif shmeta[0] is None:
            #  print(shmeta)
            #  create_blank()
        #  elif isfile(shmeta[0]):
            #  if self.suffix == r'xls':
                #  data_fake=read_excel(
                    #  shmeta[0],
                    #  sheet_name=shmeta[1],
                    #  header=shmeta[2],
                    #  engine='xlrd'
                #  )
                #  pass
            #  elif self.suffix == r'xlsx':
                #  data_fake=read_excel(
                    #  shmeta[0],
                    #  sheet_name=shmeta[1],
                    #  header=shmeta[2],
                    #  engine='openpyxl'
                #  )
                #  pass
            #  elif self.suffix == r'xlsm':
                #  data_fake=read_excel(
                    #  shmeta[0],
                    #  sheet_name=shmeta[1],
                    #  header=shmeta[2],
                    #  engine='openpyxl'
                #  )
                #  pass
            #  else:
                #  pass
            #  data_fake.fillna(0.0,inplace=True)
            #  if self.use_map==True:
                #  self.load_df_by_map(data_fake,self.xlmap)
            #  else:
                #  self.accept_df(data_fake)
            #  pass
        #  else:
            #  print(shmeta)
            #  create_blank()
        #  if (
            #  self.data is not None
            #  and self.keep_meta_info==True
        #  ):
            #  def __set_file_name(row_series):
                #  return self.pure_file_name
            #  def __set_sheet_name(row_series):
                #  return self.sheet_name
            #  self.apply_df_func(__set_sheet_name,0,'from_sheet')
            #  self.apply_df_func(__set_file_name,0,'from_book')
        #  else:
            #  print('[Waining] meta_info not kept in self.data.')
            #  print(self.xlmap)
        #  pass
    #  @property
    #  def columns(self):
        #  '''
        #  self.xlmap must not be None.
        #  this function cannot display the instant columns of self.data;
        #  '''
        #  if self.xlmap is not None:
            #  columns=deepcopy(
                #  list(
                    #  self.xlmap.columns
                #  )
            #  )
        #  else:
            #  columns=[]
        #  return columns
    def fresh_columns(self):
        '''
        self.data must not be None.
        '''
        self.columns=deepcopy(
            list(
                self.data.columns
            )
        )
        pass
    def update_columns(self,new_cols):
        '''
        self.data must not be None.
        '''
        self.columns=deepcopy(
            list(
                new_cols
            )
        )
    def load_raw_data(self):
        '''
        arguments  |shmeta=JsonMeta  |shmeta=None
        -----------|-----------------|-----------
        xlmap=XlMap|load normally    |load blank DataFrame
        xlmap=None |get xlmap from df|self.data=None
        '''
        if isinstance(self.shmeta,JsonMeta):
            path=str(list(self.shmeta.data.keys())[0])
            if isinstance(self.xlmap,XlMap):
                ## load normally
                print('[Note]:XlSheet loads data from XlMap.')
                self.data=XlBook(path).get_mapdf(
                    self.shmeta.data[path][0][0],
                    self.xlmap,
                    title=self.shmeta.data[path][0][1]
                )
            else:
                print('[Note]:XlSheet created without XlMap.')
                self.data=XlBook(path).get_df(
                    self.shmeta.data[path][0][0],
                    None,
                    title=self.shmeta.data[path][0][1]
                )
                self.xlmap=XlMap.from_list(list(self.data.columns))
        else:
            if isinstance(self.xlmap,XlMap):
                print('[Note]:Blank sheet created.')
                self.data=DataFrame(
                    data=[],
                    columns=self.xlmap.columns,
                )
                self.data.fillna(0.0,inplace=True)
                pass
            else:
                print('[Warning]:No data loaded.')
                self.data=None
        pass
    def load_df_by_map(self,df,xlmap=None):
        '''
        If xlmap is None, load by self.xlmap.
        # FutureWarning: Support for multi-dimensional indexing (e.g. `obj[:, None]`) is deprecated and will be removed in a future version.  Convert to a numpy array before indexing instead:
        '''
        print('XlSheet loads data ','with shape',df.shape,'by map:',xlmap)
        col_names_in_xlmap=list(self.xlmap.show.keys())
        self.data=DataFrame([],columns=col_names_in_xlmap)
        self.colmap_info=[]
        for col_name in col_names_in_xlmap:
            col_index=self.xlmap.show[col_name]
            if col_index==len(col_names_in_xlmap):
                break
            else:
                pass
            col_name_in_file=df.columns.to_numpy()[col_index]
            self.colmap_info.append({
                'col_name(xlmap)':col_name,
                'col_index(xlmap)':col_index,
                'col_name(file)':col_name_in_file
            })
            if isinstance(col_index,int):
                self.data[col_name]=deepcopy(df[col_name_in_file])
            elif isinstance(col_index,list):
                ## col_index is list, indicating that this column in
                ## map equals sum of a series of columns from df.
                self.data[col_name]=0
                for sub_col_index in col_index:
                    sub_col_name_in_file=df.columns[sub_col_index]
                    self.data[col_name]=deepcopy(
                        self.data[col_name]+df[sub_col_name_in_file]
                    )
                    continue
            else:
                pass
            continue
        print('load by colmap:',self.colmap_info)
        self.colmap_info=DataFrame(self.colmap_info)
        self.fresh_columns()
        pass
    def accept_df(self,in_df):
        '''
        if you code 'self.data=in_df',
        index/columns of them may differs,
        yet inner data will be the same;
        '''
        from copy import deepcopy
        self.data=deepcopy(in_df)
        if self.data is not None:
            self.data.fillna(0.0,inplace=True)
            self.fresh_columns()
        else:
            self.columns=[]
            pass
    def set_key_index(self,key_index,key_name):
        '''
        # always process with argument passed whether using xlmap or not;
        # so you must know which columns in xlmap,
        # to be set as key_index 
        # before you decide to use xlmap;
        '''
        self.key_index=key_index
        self.key_name=key_name
        if isinstance(self.data,DataFrame):
            if self.key_index != []:
                self.accept_key_index(self.key_index,self.key_name)
                self.data.fillna(0.0,inplace=True)
            else:
                print('invalid key_index:',self.key_index)
                print('key_name:',self.key_name)
                print('[Warning:XlSheet] key is not set.')
                pass
        else:
            print('[Warning:XlSheet] Data is None so key cannot be set.')
            pass
    def accept_key_index(self,key_index,key_name):
        def __join_key_id(row_series):
            resu=[]
            for k in key_index:
                if self.use_map==False:
                    resu.append(transType(row_series[k]))
                else:
                    if self.xlmap.show[k] is not None:
                        resu.append(transType(row_series[k]))
                    else:
                        pass
                continue
            return '-'.join(resu)
        if isinstance(self.data,DataFrame):
            self.apply_df_func(__join_key_id,0,key_name)
        else:
            print('[Warning:XlSheet] Data is None!')
            pass
    @property
    def rawdf(self):
        '''
        It seems useless, because self.data may be updated when using XlSheet.
        Thus the original data loaded from self.shmeta may not be what you need.
        '''
        if self.shmeta is None:
            df=deepcopy(self.data)
        elif isfile(self.shmeta[0]):
            if self.suffix=='xls':
                df=read_excel(
                    self.shmeta[0],
                    sheet_name=self.shmeta[1],
                    header=self.shmeta[2],
                    engine='xlrd'
                )
            elif self.suffix=='xlsx' or self.suffix=='xlsm':
                df=read_excel(
                    self.shmeta[0],
                    sheet_name=self.shmeta[1],
                    header=self.shmeta[2],
                    engine='openpyxl'
                )
            else:
                print('[Warning] Not an Excel file!')
                df=None
        else:
            df=None
        return df
    @property
    def shtli(self):
        if self.suffix=='xls':
            shtli=list(open_workbook(self.file_path).sheet_names())
        elif self.suffix=='xlsx':
            shtli=list(load_workbook(self.file_path).sheetnames)
        else:
            shtli=[]
        return shtli
    def __clear_row_temp(self):
        self.__row_temp=[]
        pass
    def clear(self):
        self.__clear_row_temp()
        self.data=None
        pass
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
    def duplicate(self,use_meta=False):
        cal=deepcopy(self)
        if use_meta==False:
            cal.clear()
        return cal
    def calxl(self):
        '''
        This method will generate a blank XlSheet.
        Do not call method load_raw_data();
        '''
        cal=XlSheet(
            shmeta=[None,'sheet0',0],
            #  file_path=None,
            #  sheet_name='sheet0',
            #  title=0,
            keep_meta_info=self.keep_meta_info,
            xlmap=deepcopy(self.xlmap),
            use_map=self.use_map,
            key_index=deepcopy(self.key_index),
            key_name=deepcopy(self.key_name)
        )
        return cal
    def xl2df(self,xl):
        '''
        Transfer XlSheet into it's data;
        '''
        df=deepcopy(xl.data)
        return df
    def df2xl(self,df):
        '''
        This method calls self.calxl();
        '''
        xl=self.calxl()
        xl.accept_df(deepcopy(df))
        return xl
    def apply_df_func(
        self,
        df_apply_func,
        col_index,
        col_name
    ):
        if col_name in self.data.columns:
            self.data[col_name]=deepcopy(
                self.data.apply(
                    df_apply_func,
                    axis=1,
                    raw=False,
                    result_type=None
                    # result_type can be ['reduce','expand',None];
            ))
        else:
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
        self.fresh_columns()
        # print(self.data[col_name].drop_duplicates())
        pass
    def get_data(self):
        return self.data
    def change_dtype(self,col_name,target_type=str):
        self.data[col_name]=self.data[col_name].astype(target_type)
        pass
    def change_float_to_str(self,col_name):
        # print('changing dtype, from float to str:',col_name)
        # self.change_dtype(col_name, int)
        # self.change_dtype(col_name, str)
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
        resu=self.get_row_temp_data(over_write=over_write,type_xl=type_xl)
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
    def select_matrix(
        self,
        from_cell_index,
        to_cell_index,
        type_df=False,
        has_title=False
    ):
        '''
        from_cell_index and to_cell_index are tuples like 'R1C1' ref-style in Excel: (row,column);
        '''
        return self.get_matrix(
            from_cell_index,
            to_cell_index[0]-from_cell_index[0]+1,
            to_cell_index[1]-from_cell_index[1]+1,
            type_df=type_df,
            has_title=has_title
        )
    def get_matrix(
        self,
        start_cell_index,
        n_rows_range,
        n_cols_range,
        type_df=False,
        has_title=False
    ):
        '''
        start_cell_index is a tuple like 'R1C1' ref-style in Excel: (row,column);
        For xlrd.open_workbook().sheet_by_name(), index starts from 0;
        While for openpyxl.load_workbook().get_sheet_by_name(), index starts from 1;
        That's all right, just start from 1 when passing argument 'start_cell_index' as tuple like (n,m).
        For numbers, different file type results in different data type:
            xls:str(float)
            xlsx/xlsm:str(int)
        '''
        from numpy import array
        if self.suffix=='xls':
            sht=open_workbook(self.file_path).sheet_by_name(self.sheet_name)
            matrix=array(
                [
                    sht.row_values(
                        row,
                        start_cell_index[1]-1,
                        start_cell_index[1]-1+n_cols_range
                    ) for row in range(
                        start_cell_index[0]-1,
                        start_cell_index[0]-1+n_rows_range
                    )
                ]
            )
        elif self.suffix=='xlsx':
            #  sht=load_workbook(self.file_path).get_sheet_by_name(self.sheet_name) # same as:
            sht=load_workbook(self.file_path)[self.sheet_name]
            matrix=array(
                list(
                    sht.iter_rows(
                        min_row=start_cell_index[0],
                        max_row=start_cell_index[0]+n_rows_range-1,
                        min_col=start_cell_index[1],
                        max_col=start_cell_index[1]+n_cols_range-1,
                        values_only=True
                    )
                )
            )
        elif self.suffix=='xlsm':
            #  sht=load_workbook(self.file_path).get_sheet_by_name(self.sheet_name) # same as:
            sht=load_workbook(self.file_path,keep_vba=True)[self.sheet_name]
            matrix=array(
                list(
                    sht.iter_rows(
                        min_row=start_cell_index[0],
                        max_row=start_cell_index[0]+n_rows_range-1,
                        min_col=start_cell_index[1],
                        max_col=start_cell_index[1]+n_cols_range-1,
                        values_only=True
                    )
                )
            )
        else:
            from numpy import zeros
            matrix=zeros((n_rows_range,n_cols_range))
        if type_df==True:
            if has_title==True:
                matrix=DataFrame(
                    data=matrix[1:],
                    columns=matrix[0]
                )
            else:
                matrix=DataFrame(
                    data=matrix
                )
        else:
            pass
        return matrix
    def percentage(self):
        '''
        to calculate percentage for each item in a column;
        '''
        pass
    ### below are not perfect ???
    def save(self,save_path):
        def __save_to_path(save_path): # save to known path, not existing file.
            print('save to known path, not existing file.')
            self.data.to_excel(save_path,sheet_name='-'.join([self.sheet_name,self.pure_file_name]))
            pass
        def __save_to_file(save_path): # save to existing Excel file.
            print('save to an existing file.')
            from pandas import ExcelWriter
            wb=load_workbook(save_path)
            wter=ExcelWriter(save_path)
            wter.book=wb
            self.data.to_excel(wter,sheet_name='-'.join([self.sheet_name,self.pure_file_name]))
            wter.save()
            pass
        def __save_to_dir(save_path): # create file in the directory and save.
            print('create file in the directory and save.')
            from os.path import join as osjoin
            save_path=osjoin(save_path,''.join([get_time_str(),r'.xlsx']))
            if isfile(save_path):
                self.__save_to_file(save_path)
            else:
                self.__save_to_path(save_path)
            pass
        if self.data is None:
            return
        else:
            pass
        if isfile(save_path):
            __save_to_file(save_path)
        elif isdir(save_path):
            __save_to_dir(save_path)
        else:
            # creating Excel file at save_path.
            __save_to_path(save_path)  
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
