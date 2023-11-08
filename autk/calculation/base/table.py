#!/usr/bin/env python
# encoding = 'utf-8'
'''
Table
'''
import pysnooper

from copy import deepcopy
from os.path import isfile,isdir
from threading import Thread
from openpyxl import load_workbook
from xlrd import open_workbook
from pandas import concat,DataFrame

from autk.gentk.funcs import start_thread_list
from autk.calculation.base.xlsht import XlSheet
from autk.mapper.map import XlMap
from autk.meta.pmeta import JsonMeta

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
        xlmap:XlMap=XlMap(),
        xlmeta:JsonMeta=JsonMeta({'BLANK_PATH':[['sheet',0]]}),
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
        self.collect_xl()
        pass
    def __str__(self):
        return str(self.show)
    def __clear_temp(self):
        self.__df_temp=[]
    def __clear_data(self):
        self.__df_temp=[]
        self.xlset=[]
        self.data=None
        self.load_count=0
    @property
    def show(self):
        show={
            "calculator":self.__class__.__name__,
            "xlmap":{
                str(type(self.xlmap)):
                self.xlmap.show
            },
            "xlmeta":{
                self.xlmeta.__class__.__name__:
                self.xlmeta.data
            },
            #  "xlset":[xl.show for xl in self.xlset]
        }
        return show
    def append_col_name(self,col_name):
        self.xlmap.append_col_name(col_name)
        print('[Note]check cols:',self.check_cols())
    def collect_xl(self):
        self.__clear_data()
        for shmeta in self.xlmeta.split_to_shmeta():
            self.append_xl_by_meta(shmeta)
            continue
        pass
    def append_xl_by_meta(self,shmeta):
        '''
        Append the xl object into self.xlset,
        using self.xlmap;
        '''
        self.xlset.append(
            XlSheet(
                xlmap=self.xlmap,
                shmeta=shmeta
            )
        )
        pass
    def append_df_by_map(self,df):
        pass
    def check_cols(self):
        '''
        check whether columns of all xl_obj fit with each other;
        '''
        checkli=[]
        for xl in self.xlset:
            checkli.append(
                self.xlmap.columns==xl.xlmap.columns
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
                        xl_obj.shmeta.path,]
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
        self.__clear_temp()
        self.__clear_data()
        self.load_count=0
        self.collect_xl()
        self.load_raw_data()
        pass
    @property
    def colscan(self):
        self.check_xl_cols()
        df_meta=[]
        for xl in self.xlset:
            bs=[xl.pure_file_name,xl.sheet_name] # book-sheet as bs
            col=xl.columns
            bs.extend(col)
            df_meta.append(bs)
            continue
        df_meta=DataFrame(df_meta)
        meta=df_meta.T
        return meta
    @property
    def xlscan(self):
        scan_xl_df=[]
        for xl in self.xlset:
            if xl.xlmap is not None:
                scan_xl_df.append(
                    [
                        xl.pure_file_name,
                        xl.sheet_name,
                        xl.title,
                        xl.data.shape,
                        xl.xlmap.name
                    ]
                )
            else:
                scan_xl_df.append(
                    [
                        xl.pure_file_name,
                        xl.sheet_name,
                        xl.title,
                        xl.data.shape,
                        'no_map'
                    ]
                )
        scan_xl_df=DataFrame(
            scan_xl_df,
            columns=['file','sheet_name','title','shape','map']
        )
        return scan_xl_df
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
    #  def append_df_to_temp(self,df):
        #  self.__df_temp.append(df)
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
    def join(self,if_split=False):
        '''
        Join all sheets.
        This method is not completed.
        '''
        if if_split==False: # do not split into different sheets;
            pass
        else: # split into different sheets;
            pass
        pass
    def split(self,by):
        '''
        return:
            class ImmortalTable;
        '''
        # cal=self.get_fake(copy=False, use_meta=False)
        cal=self.duplicate(use_meta=False)
        cal.xlset=[]
        def __split_single(single_value):
            fake_cal=self.duplicate(use_meta=True)
            single_xl=fake_cal.filter(
                [[single_value,by,False,True]],
                filter_type='str',
                over_write=True,
                type_xl=True
            )
            setattr(single_xl,'pure_file_name',str(single_value))
            setattr(single_xl,'sheet_name',str(single_value))
            cal.xlset.append(single_xl)
            pass
        possible_values=self.vlookup(
            r'.*',
            by,
            resu_col=None,
            if_regex=True,
            match_mode=False
        )
        thread_list=[]
        for single_value in possible_values:
            t=Thread(
                target=__split_single,
                args=(single_value,),
                name=''.join([
                    'Table-split-',
                    str(single_value)
                ])
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        return cal
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
                        xl.pure_file_name,
                        xl.sheet_name
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
        target_list,
        search_col,
        over_write=False,
        type_xl=False
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
        def __filter_list_single(
                xl_obj,
                target_list,
                search_col,
                over_write=False
        ):
            df=xl_obj.filter_list(
                target_list,
                search_col,
                over_write=over_write
            )
            self.__df_temp.append(df)
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__filter_list_single,
                args=(
                    xl,
                    target_list,
                    search_col,
                    over_write
                ),
                name=''.join(
                    [
                        r'filter_list->',
                        xl.pure_file_name,
                        xl.sheet_name
                    ]
                )
            )
            thread_list.append(t)
        start_thread_list(thread_list)
        resu=self.get_df_temp_data(
            over_write=over_write,
            type_xl=type_xl
        )
        self.__clear_temp()
        return resu
    def filter_regex_list(
        self,
        regex_list,
        search_col,
        match_mode=False,
        over_write=False,
        type_xl=False
    ):
        '''
        This method has not been believed to be effective;
        '''
        self.__clear_temp()
        self.apply_xl_func(
            XlSheet.filter_regex_list,
            (regex_list,search_col,match_mode,over_write)
        )
        resu=self.get_df_temp_data(
            over_write=over_write,
            type_xl=type_xl
        )
        self.__clear_temp()
        return resu
    def fake_filter(
        self,
        condition,
        filter_type='adv',
        copy=False
    ):
        '''
        parameters: same as self.filter (except over_write);
        return: class ImmortalTable;
        '''
        if copy==False:
            cal=self.caltable(use_meta=True)
        else:
            cal=self.duplicate(use_meta=True)
        cal.filter(
            condition,
            filter_type=filter_type,
            over_write=True,
            type_xl=False
        )
        return cal
    def fake_filter_list(
        self,
        target_list,
        search_col,
        copy=False
    ):
        if copy==False:
            cal=self.caltable(use_meta=True)
        else:
            cal=self.duplicate(use_meta=True)
        cal.filter_list(
            target_list,
            search_col,
            over_write=True,
            type_xl=False
        )
        return cal
    def fake_filter_regex_list(
        self,
        regex_list,
        search_col,
        match_mode=False,
        copy=False
    ):
        if copy==False:
            cal=self.caltable(use_meta=True)
        else:
            cal=self.duplicate(use_meta=True)
        cal.filter_regex_list(
            regex_list,
            search_col,
            match_mode=match_mode,
            over_write=True,
            type_xl=False
        )
        return cal
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
        #  self.apply_xl_func(
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
        '''
        resuli={} #[]
        def __collect_apply(xl,xl_func_name):
            xl_resu=getattr(xl,xl_func_name)(*args,**kwargs)
            #  if xl_resu is None:
                #  pass
            #  elif isinstance(xl_resu,list):
                #  resuli.extend(xl_resu)
            #  else:
                #  resuli.append(xl_resu)
            resuli.update({
                xl.name:xl_resu
            })
        thli=[]
        for xl in self.xlset:
            thli.append(
                Thread(
                    target=__collect_apply,
                    args=(xl,xl_func_name,),
                    name='~'.join([
                        self.__class__.__name__,
                        'apply',
                        xl.name,
                        xl_func_name
                    ])
                )
            )
            continue
        start_thread_list(thli)
        return resuli
    def apply_xl_func(
        self,
        xl_func,
        *args,
        **kwargs,
    ):
        '''
        This method starts multi-thread to manipulate every xl,
        in self.xlset to call one of his method named `xl_func`,
        so as to collect data into outside data-capsule.
        Then you get data by self.get_df_temp_data();
        This method works with self.get_df_temp_data;
        `xl_func` must be customized, whose first
        parameter is xl, followed by other arguments 
        needed by `xl_func`;
        '''
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
        start_thread_list(thread_list)
        return
    def apply_df_func(
        self,
        df_apply_func,
        col_name:str,
    ):
        '''
        Insert a column named 'col_name', where store
        the result of calculation performed by the
        function 'df_apply_func', whose only
        parameter is `row_series`.
        This method changes each xl.data in
        self.xlset, but returns nothing;
        This function changes columns of
        both self.xlmap and earch xlmap of
        XlSheet in self.xlset.
        Parameters:
            df_apply_func:function
            col_name:str
        '''
        if self.xlmap.has_cols([col_name]):
            pass
        else:
            self.xlmap.append_col_name(col_name)
        def __apply_single(xl):
            xl.apply_df_func(
                df_apply_func,
                col_name,
                col_index=None,
            )
            pass
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__apply_single,
                args=(xl,),
                name=''.join(
                    [
                        r'apply->',
                        getattr(df_apply_func,'__name__'),
                        ',for:',
                        xl.name
                    ]
                )
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        #  self.load_raw_data()
        pass
    def change_dtype(self,col_name,target_type=str):
        def __change_single(xl,col_name,target_type):
            xl.change_dtype(col_name,target_type)
            pass
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__change_single,
                args=(xl,col_name,target_type),
                name=''.join(
                    [
                        r'change_dtype->',
                        xl.pure_file_name,
                        xl.sheet_name
                    ]
                )
            )
            thread_list.append(t)
        start_thread_list(thread_list)
        pass
    def change_float_to_str(self,col_name):
        def __change_single(xl,col_name):
            xl.change_float_to_str(col_name)
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__change_single,
                args=(xl,col_name,),
                name=''.join(
                    [
                        r'change_float_to_str->',
                        xl.pure_file_name,
                        xl.sheet_name
                    ]
                )
            )
            thread_list.append(t)
        start_thread_list(thread_list)
        pass
    ### the following are not perfect !
    def filter_key_record(self,condition_matrix):
        '''
        Expected to be perfect.
        Return all keys (data in column 'key_name') according to the passed argument condition_matrix.
        '''
        # self.__clear_temp()
        d=self.filter(condition_matrix,filter_type='str')
        resu_keys=list(d[self.key_name].drop_duplicates())
        # self.__clear_temp()
        return self.filter_list(resu_keys, search_col=self.key_name)
    ### the above are not perfect.
    pass
if __name__=='__main__':
    pass
