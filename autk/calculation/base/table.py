#!/usr/bin/env python
# encoding = 'utf-8'
'''
Table
'''
from copy import deepcopy
from os.path import isfile,isdir
from threading import Thread
from openpyxl import load_workbook
from xlrd import open_workbook
from pandas import concat,DataFrame

from autk.gentk.funcs import start_thread_list
from autk.mapper.base import XlMap
from autk.meta.pmeta import JsonMeta
from autk.calculation.base.xlsht import XlSheet

class ImmortalTable:
    '''
    ImmortalTable.
        1. self.xlmap is passed as an argument only when instantiating an XlSheet object, in method of parsing xlmeta.
        2. key_index and key_name are needed only when you have to combine a few columns as the KEY column. Simply loading data of ImmortalTable does not needed key_index and key_name. 
        key_index and key_name are NOT necessary for ImmortalTable, yet FAIRLY necessary for MGL.
        3. self.xlmeta can be:
            1-dimension list, for example,[path,sheet_name,title];
            2-dimension list, [[path,sheet_name,title],[path,sheet_name,title],...]
            dict,for example,{path_1:[[sheet_1,0],[sheet_2,3]],path_2:[[sheet_3,2],[sheet_4,1]],....};
            directory,all xlsx files and all of their sheets will be loaded;
            json_file_path, same as dict;
            xlsx_file_path, all sheets of the xlsx file will be loaded.
    Remember to call self.__clear_temp(),
    before every calculation.
    '''
    def __init__(
        self,
        xlmap:XlMap=None,#XlMap(),
        xlmeta:JsonMeta=None,#JsonMeta({'BLANK_PATH':[['sheet',0]]}),
    ):
        ### basic attributes:
        self.xlmap=xlmap
        self.xlmeta=xlmeta
        ### calculation attributes:
        self.__df_temp=[]
        self.xlset=[]
        self.data=None
        self.load_count=0
        ### start initialize:
        if isinstance(xlmeta,JsonMeta):
            self.collect_xl()
        pass
    def __str__(self):
        return str(self.show)
    def __len__(self):
        return len(self.xlset)
    def __add__(
        self,
        other
    ):
        resu=self.blank_copy()
        resu.xlmap.extend_col_list(
            other.xlmap.columns
        )
        resu.xlset.extend(self.xlset)
        resu.xlset.extend(other.xlset)
        return resu
    def __clear_temp(self):
        self.__df_temp=[]
    def __clear_data(self):
        self.__df_temp=[]
        self.xlset=[]
        self.data=None
        self.load_count=0
        pass
    @property
    def xlshape(self)->DataFrame:
        shape=[]
        for xl in self.xlset:
            shape.append([
                xl.name,
                xl.show['data_size'][0],
                xl.show['data_size'][1]
            ])
        shape=DataFrame(
            shape,
            columns=[
                self.xlset[0].__class__.__name__ if len(self.xlset)>0 else 'BLANK',
                'rows',
                'columns'
            ],
        )
        return shape
    @property
    def shape(self):
        print(
            '[{}] test shape: rows {}, columns {}'.format(
                self.__class__.__name__,
                self.data.shape[0]==self.xlshape['rows'].sum(),
                self.data.shape[1]==self.xlshape['columns'].max()
            ) if self.data is not None else '[Warning] raw data unloaded.'
        )
        return (
            self.xlshape['rows'].sum(),
            self.xlshape['columns'].max()
        )
    @property
    def show(self):
        show={
            "calculator":
                self.__class__.__name__,
            "xlset":{
                "xl_count":len(self.xlset),
                "total_size":self.shape
            },
            "xlmap":{
                str(type(self.xlmap)):
                None if self.xlmap is None else self.xlmap.show
            },
            "xlmeta":{
                self.xlmeta.__class__.__name__:
                None if self.xlmeta is None else self.xlmeta.data
            },
        }
        return show
    def append_col_name(self,col_name):
        '''
        `XlSheet` in self.xlset share the same
        `XlMap` with `ImmortalTable`, so acctually
        it only matters the only first time that
        `XlMap.append_col_name()` is called.
        '''
        self.apply_xl(
            'append_col_name',
            col_name
        )
        self.load_raw_data()
    def extend_col_list(self,col_list):
        self.apply_xl(
            'extend_col_list',
            col_list
        )
        self.load_raw_data()
    def blank_copy(self):
        return self.__class__(
            xlmap=deepcopy(self.xlmap),
            xlmeta=None
        )
    def blank_sht(self):
        return XlSheet(
            xlmap=self.xlmap,
            shmeta=None
        )
    def regenkey(self):
        self.apply_df_func(
            lambda row_series:'-'.join([
                str(row_series[item]) if item is not None else ''  for item in self.xlmap.key_index
            ]),
            self.xlmap.key_name
        )
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
            xl=XlSheet(
                xlmap=self.xlmap,
                shmeta=shmeta
            )
            if isinstance(self.xlmap,XlMap):
                pass
            else:
                self.xlmap=XlMap()
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
                        'xlsheet'
                    ])
                )
            )
            continue
        start_thread_list(thli)
        pass
    def append_xl_by_meta(self,shmeta):
        '''
        Append the xl object into self.xlset,
        using self.xlmap, by `shmeta`;
        '''
        #TODO never test when self.xlmap=None
        if isinstance(self.xlmap,XlMap):
            pass
        else:
            self.xlmap=XlMap()
            self.xlmap.extend_col_list(
                xl.xlmap.columns
            )
        if len(self.xlset)==0:
            xl=XlSheet(
                xlmap=self.xlmap,
                shmeta=shmeta
            )
        else:
            xl=self.xlset[0].__class__(
                xlmap=self.xlmap,
                shmeta=shmeta
            )
        self.xlset.append(xl)
        if self.load_count>0:
            self.load_raw_data()
        print(
            '[{}] test cols after {} appended to xlset:'.format(
                self.__class__.__name__,
                xl.__class__.__name__
            ),
            self.check_cols()
        )
        pass
    def append_xl_by_map(
        self,
        xl,
        xlmap:XlMap=None
    ):
        '''
        It does not matter whether `xl`
        has `JsonMeta` as `shmeta` or not.
        Yet xl.xlmap is important.
        '''
        #TODO never tested.
        if (
            #  isinstance(self.xlmap,XlMap)
            #  and
            isinstance(xlmap,XlMap)
        ):
            self.xlmap.extend_col_list(
                xlmap.columns
            )
        else:
            pass
        self.xlset.append(xl)
        if self.load_count>0:
            self.load_raw_data()
        print(
            '[{}] test cols after {} appended to xlset by map {}:'.format(
                self.__class__.__name__,
                xl.__class__.__name__,
                xl.xlmap.__class__.__name__
            ),
            self.check_cols()
        )
        pass
    def append_df_by_map(self,df):
        #XlSheet.load_df_by_map()
        if len(self.xlset)==0:
            xl=XlSheet(
                xlmap=self.xlmap,
                shmeta=None
            )
        else:
            xl=self.xlset[0].__class__(
                xlmap=self.xlmap,
                shmeta=None
            )
        xl.load_df_by_map(
            df,
            xlmap=None
        )
        self.xlset.append(xl)
        if self.load_count>0:
            self.load_raw_data()
        print(
            '[{}] test cols after `DataFrame` appended:'.format(
                self.__class__.__name__,
            ),
            self.check_cols()
        )
        pass
    def check_cols(self):
        '''
        check whether columns of all
        `XlSheet.xlmap.columns` fit with
        `self.xlmap.columns` and each other;
        '''
        checkli=[]
        for xl in self.xlset:
            checkli.append(
                self.xlmap.columns==xl.xlmap.columns and xl.check_cols()
            )
            continue
        return [True]*len(self.xlset)==checkli
    def load_raw_data(self):
        '''
        Join all DataFrame of XlSheet in self.xlset, into self.data;
        '''
        self.__clear_temp()
        def __load_single(xl_obj):
            self.__df_temp.append(xl_obj.data)
        thread_list=[]
        for xl_obj in self.xlset:
            thread_list.append(
                Thread(
                    target=__load_single,
                    args=(xl_obj,),
                    name=''.join(
                        [r'load_raw_data->',
                        xl_obj.name,]
                    )
                )
            )
            continue
        start_thread_list(thread_list)
        self.data=self.get_df_temp_data(
            over_write=True,
            type_xl=False
        )
        self.load_count+=1
        self.__clear_temp()
        pass
    def fresh_data(self):
        previous_load_count=deepcopy(self.load_count)
        self.__clear_temp()
        self.__clear_data()
        self.collect_xl()
        if previous_load_count>0:
            self.load_raw_data()
        pass
    def fresh_xl(self):
        self.apply_xl('load_raw_data')
        self.fresh_data()
        pass
    @property
    def rawdf(self):
        '''
        It seems useless, because self.data may be updated when using ImmortalTable.
        Thus the original data loaded from self.xlmeta may not be what you need.
        '''
        if self.xlset !=[]:
            dfli=[]
            def __single_get_rawdf(xl):
                dfli.append(
                    xl.rawdf
                )
                pass
            thread_list=[]
            for xl in self.xlset:
                thread_list.append(
                    Thread(
                        target=__single_get_rawdf,
                        args=(xl,),
                        name=''
                    )
                )
                continue
            start_thread_list(thread_list)
            df=concat(dfli,axis=0,join='outer')
            pass
        else:
            df=None
        return df
    def get_df_temp_data(
            self,
            over_write=False,
            type_xl=False
    ):
        '''
        parameters:
            over_write:bool
                does not affect self.xlset,
                but only changes self.data;
            type_xl:bool
                determines the result will be a DataFrame or XlSheet;
        '''
        if len(self.__df_temp)==0:
            resu=DataFrame([],columns=self.xlmap.columns)
        else:
            resu=concat(
                self.__df_temp,
                axis=0,
                join='outer'
            )
            resu.fillna(0.0,inplace=True)
        if over_write==True:
            self.data=resu
        if type_xl==True:
            xl=XlSheet(xlmap=self.xlmap)
            # with xlmap,without shmeta,
            # XlSheet.data is blank DataFrame,
            # whose columns fit with
            # self.xlmap.columns;
            xl.load_df_by_map(
                resu,
                xlmap=self.xlmap
            )
            return xl
        else:
            return resu
    def __filter_num_single(
            self,
            xl_obj,
            condition_matrix,
            over_write
    ):
        self.__df_temp.append(
            xl_obj.filter_num(
                condition_matrix,
                over_write=over_write
            )
        )
        pass
    def __filter_str_single(
            self,
            xl_obj,
            condition_matrix,
            over_write
    ):
        self.__df_temp.append(
            xl_obj.filter_str(
                condition_matrix,
                over_write=over_write
            )
        )
        pass
    def __filter_adv_single(
            self,
            xl_obj,
            condition_dict,
            over_write
    ):
        self.__df_temp.append(
            xl_obj.filter_adv(
                condition_dict,
                over_write=over_write
            )
        )
        pass
    def __start_filter(
            self,
            afunc,
            condition,
            over_write
    ):
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=afunc,
                args=(xl,condition,over_write),
                name=''.join(
                    [
                        r'__start_filter->',
                        getattr(afunc,'__name__'),
                        r':',
                        xl.name
                    ]
                )
            )
            thread_list.append(t)
        start_thread_list(thread_list)
        pass
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
            over_write:bool
                default to False.
                If True, both self.xlset and self.data will be changed;
        return:
            pandas.core.frame.DataFrame.
        '''
        #TODO can be simplified.
        # same as self.apply_xl_collect_df('filter',*args,**kwargs);
        self.__clear_temp()
        if filter_type=='str':
            self.__start_filter(
                self.__filter_str_single,
                condition,
                over_write
            )
        elif filter_type=='num':
            self.__start_filter(
                self.__filter_num_single,
                condition,
                over_write
            )
        elif filter_type=='adv':
            self.__start_filter(
                self.__filter_adv_single,
                condition,
                over_write
            )
        else:
            pass
        resu=self.get_df_temp_data(
            over_write=over_write,
            type_xl=type_xl
        )
        self.__clear_temp()
        return resu
    def filter_list(
        self,
        #  target_list,
        #  search_col,
        #  over_write=False,
        #  type_xl=False
        *args,
        **kwargs,
    ):
        '''
        Get all columns of data according to the given item list in the column of search_col.
        Same effect as:
            resu=[]
            for 'str_item' in 'target_list':
                # when 'key_col'='resu_col'='search_col':
                d=self.vlookup(str_item,search_col,search_col,if_regex=False,match_mode=False) 
                resu.extend(d)
            return resu
        '''
        self.__clear_temp()
        resu=self.apply_xl_collect_df(
            'filter_list',
            *args,
            **kwargs,
        )
        #  def __filter_list_single(
                #  xl_obj,
                #  target_list,
                #  search_col,
                #  over_write=False
        #  ):
            #  df=xl_obj.filter_list(
                #  target_list,
                #  search_col,
                #  over_write=over_write
            #  )
            #  self.__df_temp.append(df)
        #  thread_list=[]
        #  for xl in self.xlset:
            #  t=Thread(
                #  target=__filter_list_single,
                #  args=(
                    #  xl,
                    #  target_list,
                    #  search_col,
                    #  over_write
                #  ),
                #  name=''.join(
                    #  [
                        #  r'filter_list->',
                        #  xl.name,
                    #  ]
                #  )
            #  )
            #  thread_list.append(t)
        #  start_thread_list(thread_list)
        #  resu=self.get_df_temp_data(
            #  over_write=over_write,
            #  type_xl=type_xl
        #  )
        self.__clear_temp()
        return resu
    def filter_regex_list(
        self,
        *args,
        **kwargs,
    ):
        '''
        This method has not been believed to be effective;
        '''
        self.__clear_temp()
        resu=self.apply_xl_collect_df(
            'filter_regex_list',
            *args,
            **kwargs,
        )
        self.__clear_temp()
        return resu
    def vlookup(
        self,
        str_item,
        key_col,
        resu_col=None,
        if_regex=True,
        match_mode=True
    )->list:
        if resu_col is None:
            resu_col=key_col
        else:
            pass
        resu=[]
        def __xl_vlookup(xl):
            resu.extend(
                xl.vlookup(
                    str_item,
                    key_col,
                    resu_col,
                    if_regex,
                    match_mode
                )
            )
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__xl_vlookup,
                args=(xl,),
                name=''.join(
                    [
                        self.__class__.__name__,
                        r'vlookup-in-->',
                        xl.name
                    ]
                )
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        from pandas import Series
        if len(resu) != 0:
            resu=Series(resu,dtype=type(resu[0]))
        else:
            resu=Series(resu,dtype=str)
        resu=resu.drop_duplicates()
        resu=list(resu)
        return resu
    def vlookups(
        self,
        *args,
        **kwargs
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
        #  self.__clear_temp()
        #  resuli=[]
        #  def __xl_vlookups(xl):
            #  resuli.extend(
                #  xl.vlookups(
                    #  *args,
                    #  **kwargs
                #  )
            #  )
            #  pass
        #  self.apply_func(
            #  __xl_vlookups,
        #  )
        #  resuli=list(
            #  set(resuli)
        #  )
        #  return resuli
        resuli=[]
        resu_dict=self.apply_xl_collect(
            'vlookups',
            *args,
            **kwargs
        )
        for resu in list(resu_dict.values()):
            resuli.extend(resu)
        if args[3]==True:
            resuli=list(set(resuli))
        return resuli
    def sumifs(
        self,
        *args,
        **kwargs,
    ):
        '''
        condition can be matrix or dict;
        '''
        return sum(set(
            self.apply_xl_collect('sumifs',*args,**kwargs).values()
        ))
    def apply_func(
        self,
        xl_func,# a function
        *args,
        **kwargs,
    ):
        '''
        This method starts multi-thread
        to manipulate every xl in self.xlset.
        `xl_func` must be customized, whose first
        parameter is xl;
        When designing what `xl_func`, think about
        what `xl_func` can do with its first argument
        as `xl`.
        This function returns 0.
        '''
        print(
            '[{}|apply_func] external function `{}` applied.'.format(
                self.__class__.__name__,
                xl_func.__name__,
            )
        )
        thread_list=[]
        for xl in self.xlset:
            thread_list.append(
                Thread(
                    target=xl_func,
                    args=(xl,*args),
                    name='~'.join([
                        self.__class__.__name__,
                        r'apply',
                        xl_func.__name__,
                    ])
                )
            )
            continue
        #  print(
            #  '[{}] function `{}` is to be applied.'.format(
                #  self.__class__.__name__,
                #  xl_func.__name__,
            #  )
        #  )
        start_thread_list(thread_list)
        return 0
    def apply_xl(
        self,
        xl_func_name:str,
        *args,
        **kwargs
    ):
        print(
            '[{}|apply_xl] every member in `self.xlset` calls `{}`.'.format(
                self.__class__.__name__,
                xl_func_name,
            )
        )
        def __single_apply(xl):
            getattr(
                xl,
                xl_func_name
            )(*args,**kwargs)
            pass
        print(
            '[{}] function `{}` of `{}` to be applied.'.format(
                self.__class__.__name__,
                xl_func_name,
                self.xlset[0].__class__.__name__,
            )
        )
        self.apply_func(
            __single_apply,
        )
        pass
    def apply_xl_collect(
        self,
        xl_func_name:str,
        *args,
        **kwargs
    )->dict:
        '''
        Returns:
            {
                "xl_obj_1":result_1,
                "xl_obj_2":result_2,
                "xl_obj_3":result_3,
                ....
            }
        where:
            results may be XlSheet,DataFrame,list,or set,etc.
            if results are XlSheet/DataFrame, they can be
            joined into a single DataFrame and size are known.
        '''
        print(
            '[{}|apply_xl_collect] start to collect `{}` into dict.'.format(
                self.__class__.__name__,
                xl_func_name,
            )
        )
        resuli={} # key:XlSheet.name,value:results
        def __collect_apply(xl,xl_func_name):
            xl_resu=getattr(
                xl,
                xl_func_name
            )(*args,**kwargs)
            resuli.update({
                xl.name:xl_resu
            })
        # the following can be replaced by
        # `self.apply_func`
        #  thli=[]
        #  for xl in self.xlset:
            #  thli.append(
                #  Thread(
                    #  target=__collect_apply,
                    #  args=(xl,xl_func_name,),
                    #  name='~'.join([
                        #  self.__class__.__name__,
                        #  'apply',
                        #  xl.name,
                        #  xl_func_name
                    #  ])
                #  )
            #  )
            #  continue
        #  start_thread_list(thli)
        self.apply_func(
            __collect_apply,
            xl_func_name
        )
        return resuli
    def apply_xl_collect_df(
        self,
        xl_func_name:str,
        *args,
        **kwargs
    )->DataFrame:
        '''
        Transform what `self.apply_xl_collect` returns
        into DataFrame.
        If `self.apply_xl_collect` called,
        each thread of `getattr(XlSheet,xl_func_name)(*args,*kwargs)`
        returns DataFrame or XlSheet,
        this method will collect them into a DataFrame.
        '''
        print(
            '[{}|apply_xl_collect_df] start to collect `{}` into DataFrame.'.format(
                self.__class__.__name__,
                xl_func_name,
            )
        )
        xl_collect=self.apply_xl_collect(
            xl_func_name,
            *args,
            **kwargs
        )
        resuli=[]
        for xl_name,xl in xl_collect.items():
            if isinstance(xl,XlSheet):
                resuli.append(xl.data)
            elif isinstance(xl,DataFrame):
                resuli.append(xl)
            else:
                resuli.append(
                    DataFrame([],columns=self.xlmap.columns)
                )
        data=concat(
            resuli,
            axis=0,
            join='outer'
        )
        return data
    def apply_xl_collect_list(
        self,
        xl_func_name:str,
        *args,
        **kwargs
    )->list:
        '''
        If results of `self.apply_xl_collect` are list,
        join them into a single list.
        '''
        print(
            '[{}|apply_xl_collect_list] start to collect list results of `{}`.'.format(
                self.__class__.__name__,
                xl_func_name,
            )
        )
        xl_collect=self.apply_xl_collect(
            xl_func_name,
            *args,
            **kwargs
        )
        resuli=[]
        for xl_name,xl in xl_collect.items():
            if isinstance(xl,list):
                resuli.extend(xl)
            if isinstance(xl,set):
                resuli.extend(list(xl))
            else:
                #  resuli.append(xl)
                pass
        return resuli
    def apply_xl_collect_size(
        self,
        xl_func_name:str,
        *args,
        **kwargs
    )->DataFrame:
        '''
        Size of the return of `self.apply_xl_collect_df`.
        '''
        print(
            '[{}|apply_xl_collect_size] calculate the size of data colleced by `{}`.'.format(
                self.__class__.__name__,
                xl_func_name,
            )
        )
        data=self.apply_xl_collect(
            xl_func_name,
            *args,
            **kwargs
        )
        size=[]
        for xl_name,xl in data.items():
            if isinstance(xl,XlSheet):
                size.append([xl_name,xl.data.shape[0],xl.data.shape[1]])
            elif isinstance(xl,DataFrame):
                size.append([xl_name,xl.shape[0],xl.shape[1]])
            elif isinstance(xl,list):
                size.append([xl_name,len(xl),0])
            else:
                size.append([xl_name,None,None])
            continue
        size_df=DataFrame(
            size,
            columns=[
                self.xlset[0].__class__.__name__,
                'rows',
                'columns'
            ],
        )
        return size_df
    def apply_df_func(
        self,
        row_series_func,# a function
        col_name:str,
    ):
        '''
        Insert a column named 'col_name', where store
        the result of calculation performed by the
        function 'row_series_func', whose only
        parameter is `row_series`.
        This method changes each xl.data in
        self.xlset, but returns nothing;
        This function changes columns of
        both self.xlmap and earch xlmap of
        XlSheet in self.xlset.
        Parameters:
            row_series_func:function
            col_name:str
        '''
        print(
            '[{}|apply_df_func] call function `{}` and get result in column:`{}`.'.format(
                self.__class__.__name__,
                row_series_func.__name__,
                col_name,
            )
        )
        if self.xlmap.has_cols([col_name]):
            pass
        else:
            self.append_col_name(col_name)
        def __apply_single(xl):
            xl.apply_df_func(
                row_series_func,
                col_name,
                col_index=None,# this will be deleted later.
            )
            pass
        #  thread_list=[]
        #  for xl in self.xlset:
            #  t=Thread(
                #  target=__apply_single,
                #  args=(xl,),
                #  name=''.join(
                    #  [
                        #  r'apply->',
                        #  getattr(row_series_func,'__name__'),
                        #  ',for:',
                        #  xl.name
                    #  ]
                #  )
            #  )
            #  thread_list.append(t)
            #  continue
        #  start_thread_list(thread_list)
        #  self.load_raw_data()
        self.apply_func(__apply_single,)
        if self.load_count>0:
            self.load_raw_data()
        pass
    def filter_key_record(
        self,
        condition,
        filter_type='adv'
    ):
        '''
        Expected to be perfect.
        Return all keys (data in column 'key_name') according to the passed argument condition_matrix.
        '''
        resu_keys=list(self.apply_xl_collect_df(
            'filter',
            condition,
            filter_type

        )[self.xlmap.key_name].drop_duplicates())
        return self.filter_list(
            resu_keys, 
            self.xlmap.key_name,
            False,
            False
        )
    def change_dtype(self,col_name,target_type=str):
        self.apply_xl(
            'change_dtype',
            col_name,
            target_type,
        )
        pass
    def change_float_to_str(self,col_name):
        self.apply_xl(
            'change_float_to_str',
            col_name
        )
        pass
    def split(self,by:str):
        '''
        This method is completed.
        return:
            class ImmortalTable;
        '''
        #TODO
        # not perfect when same columns appear in different XlSheet;
        # the result may duplicate.
        table=self.blank_copy()
        table.xlset=self.apply_xl_collect_list(
            'split',
            by,
        )
        return table
    ### the following are not perfect !
    def join(self,if_split=False)->XlSheet:
        '''
        Join all sheets.
        This method is not completed.
        '''
        if if_split==False: # do not split into different sheets;
            pass
        else: # split into different sheets;
            pass
        pass
    pass
if __name__=='__main__':
    pass
