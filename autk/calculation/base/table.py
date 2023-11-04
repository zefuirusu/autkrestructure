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
    '''
    def __init__(
        self,
        xlmap:XlMap=None,
        xlmeta:JsonMeta=None,
        #  common_title=0,
        #  use_map=False,
        #  auto_load=False,
        #  keep_meta_info=True,
        #  key_index=[],
        #  key_name='key_id'
    ):
        self.xlmap=xlmap
        self.xlmeta=xlmeta
        ### 
        self.__df_temp=[]
        self.xlset=[]
        self.data=None
        self.load_count=0
        ###
        #  self.use_map=use_map
        #  self.keep_meta_info=keep_meta_info
        #  self.__set_key(key_index,key_name)
        self.__parse_meta(xlmeta,common_title)
        self.check_xl_cols()
        if auto_load==True:
            self.load_raw_data()
        self.fake=False
        pass
    def __str__(self):
        if self.xlmeta is not None:
            str_info=''.join([
                'mem_addr: ',
                str(id(self)),'\n',
                'meta:\t',
                str(self.xlmeta),'\n'
                ])
        else:
            str_info=''.join([
                'mem_addr: ',
                str(id(self)),'\n',
                'meta:\t',
                'None','\n',
                'blank table','\n'
                ])
        return str_info
    def __set_key(self,key_index,key_name):
        '''
        argument passed has a higher level than xlmap;
        '''
        self.key_list=None # all possible values of key
        if self.use_map==False:
            self.key_index=key_index
            self.key_name=key_name
            pass
        else:
            self.key_index=self.xlmap.key_index
            self.key_name=self.xlmap.key_name
            pass
        if self.key_index==[]:
            print('[Warning] key_index:',self.key_index)
        pass
    def parse_meta(self,xlmeta,common_title,auto_load=False):
        '''
        This method will update self.xlmeta and self.common_title.
        '''
        print('parsing outsource meta data...')
        self.__parse_meta(xlmeta,common_title)
        if auto_load==True:
            self.load_raw_data()
        else:
            pass
    def __parse_meta(self,xlmeta,common_title):
        self.xlmeta=xlmeta
        self.common_title=common_title
        if self.xlmeta is None:
            print('Parsing None xlmeta...')
            self.__parse_None()
            print('Initialized with None xlmeta:')
        elif isinstance(self.xlmeta,dict):
            print('Parsing dict...')
            self.__parse_dict(self.xlmeta)
            print('Initialized with dict data:',self.xlmeta)
        elif isinstance(self.xlmeta,list):
            print('Parsing list...')
            self.__parse_list(self.xlmeta)
            print('Initialized with list:',self.xlmeta)
        elif isfile(self.xlmeta):
            import os
            import re
            file_name=self.xlmeta.split(os.sep)[-1]
            file_suffix=re.sub(r'^.*\.','',file_name)
            if file_suffix=='json':
                print('Parsing json file...')
                from json import load
                with open(self.xlmeta,mode='r',encoding='utf-8') as f:
                    self.__parse_dict(load(f))
                print('Initialized with json file:',self.xlmeta)
            elif file_suffix=='xlsx':
                print('Parsing xlsx file...')
                self.__parse_xlsx(self.xlmeta)
                print('Initialized with Microsoft Excel file:',self.xlmeta)
            elif file_suffix=='xls':
                print('Parsing xls file...')
                self.__parse_xls(self.xlmeta)
                print('Initialized with Microsoft Excel 2003 file:',self.xlmeta)
            pass
        elif isdir(self.xlmeta):
            print('Parsing directory...')
            self.__parse_dir(self.xlmeta)
            print('Initialized with all files in directory:',self.xlmeta)
        else:
            print('invalid argument:xlmeta','\n',self.xlmeta)
            pass
        pass
    def append_xl_by_meta(self,shmeta):
        '''
        Append the xl object into self.xlset;
        '''
        self.xlset.append(
            XlSheet(
                shmeta,
                keep_meta_info=self.keep_meta_info,
                xlmap=self.xlmap,
                use_map=self.use_map,
                key_index=self.key_index,
                key_name=self.key_name
            )
        )
        pass
    def append_df_by_meta(self,shmeta):
        self.append_xl_by_meta(shmeta)
        self.load_raw_data()
    def __parse_xlsx(self,xlmeta): 
        '''
        xlmeta is the file path of an Microsoft Excel file (xlsx).
        This method will load all sheets;
        This method can also parse "xls" file, through self.append_xl_by_meta;
        '''
        print('Parsing xlsx file...')
        shtli=load_workbook(xlmeta).sheetnames # openpyxl for xlsx
        #  shtli=open_workbook(xlmeta).sheet_names() # xlrd for xls
        for sht in shtli:
            shmeta=[xlmeta,sht,self.common_title]
            self.append_xl_by_meta(shmeta)
        pass
    def __parse_xls(self,xlmeta): 
        print('Parsing xls file...')
        #  shtli=load_workbook(xlmeta).sheetnames # openpyxl for xlsx
        shtli=open_workbook(xlmeta).sheet_names() # xlrd for xls
        for sht in shtli:
            shmeta=[xlmeta,sht,self.common_title]
            self.append_xl_by_meta(shmeta)
        pass
    def __parse_dir(self,xlmeta): 
        '''
        xlmeta is a directory where many Microsoft Excel files exists;
        # self.xlmeta is a directory and title is 0 as default;
        '''
        from os import listdir,sep
        from os.path import join as osjoin
        import re
        thread_list=[]
        file_li=listdir(xlmeta)
        print('files to parse:\n',file_li)
        for file_name in file_li:
            file_path=osjoin(xlmeta,file_name)
            ## to decide self.__parse_xls or self.__parse_xlsx
            file_name=file_path.split(sep)[-1]
            file_suffix=re.sub(r'^.*\.','',file_name)
            if file_suffix=='xlsx':
                thread_list.append(
                    Thread(
                        target=self.__parse_xlsx,
                        args=(file_path,),
                        name=''.join(['parse_xlsx-',file_name])
                    )
                )
            elif file_suffix=='xls':
                thread_list.append(
                    Thread(
                        target=self.__parse_xls,
                        args=(file_path,),
                        name=''.join(['parse_xls-',file_name])
                    )
                )
            continue
        start_thread_list(thread_list)
        return
    def __parse_list(self,xlmeta): 
        '''
        xlmeta is a list, maybe 1-dimension or 2-dimension;
        # self.xlmeta is a list;
        '''
        from numpy import array
        xlmeta_fake=array(self.xlmeta)
        if xlmeta_fake.ndim==1: # 1-dimension
            self.append_xl_by_meta(xlmeta)
        elif xlmeta_fake.ndim==2: # 2-dimension
            thread_list=[]
            for meta in self.xlmeta:
                thread_list.append(
                    Thread(
                        target=self.append_xl_by_meta,
                        args=(meta,)
                    )
                )
                # self.append_xl_by_meta(meta)
                continue
            start_thread_list(thread_list)
        else:
            print('Error:dimension of xlmeta must be 1 or 2!')
        return
    def __parse_dict(self,xlmeta): 
        '''
        xlmeta is a dict:
        {
            "file_name_1":[
                ["sheet_name_1",title_1],
                ["sheet_name_2",title_2],
                ....
            ],
            "file_name_2":[
                ["sheet_name_3",title_3],
                ["sheet_name_4",title_4],
                ....
            ],
            ...
        }
        # self.xlmeta is a dict;
        '''
        if len(list(set(xlmeta.keys())))==len(set(xlmeta.keys())):
            thread_list=[]
            for file_path in xlmeta:
                for sheet_meta in xlmeta[file_path]:
                    shtna=sheet_meta[0]
                    title=sheet_meta[1]
                    shmeta=[file_path,shtna,title]
                    thread_list.append(
                        Thread(
                            target=self.append_xl_by_meta,
                            args=(shmeta,),
                            name=''.join([
                                'parse_dict-',shmeta[0]
                            ])
                        )
                    )
                    continue
                continue
            start_thread_list(thread_list)
            pass
        else:
            for file_path in xlmeta:
                for sheet_meta in xlmeta[file_path]:
                    shtna=sheet_meta[0]
                    title=sheet_meta[1]
                    shmeta=[file_path,shtna,title]
                    self.append_xl_by_meta(shmeta)
                    continue
                continue
        return
    def __parse_None(self):
        print('self.xlmeta is None.\nEmpty Table created.')
        print('self.xlset=[]')
        # self.xlset.append(XlSheet(None, None, 0, key_index=[],key_name='key_id',xlmap=None,keep_meta_info=True))
        pass
    def update_columns(self,new_cols):
        '''
        update columns according to input list;
        new_cols is list-like, iterable;
        '''
        setattr(
            self,
            'columns',
            deepcopy(
                list(new_cols)
            )
        )
    def check_xl_cols(self):
        '''
        check whether columns of all xl_obj fit with each other;
        this method will update columns;
        if check result is ok, it will update and return common columns,
        otherwise update/return [];
        '''
        print('checking columns of all tables...')
        if self.xlmeta is not None:
            col_set_0=[]
            for xl in self.xlset:
                xl.fresh_columns()
                col_set_0.extend(xl.columns)
                continue
            col_set_0=set(col_set_0)
            # till now, common columns are in hand: col_set_0;
            print('cols in common:',col_set_0)
            check_result=[]
            def check_func(xl):
                diff_cols=set(xl.columns)-col_set_0
                print(
                    'check:',
                    xl.pure_file_name,
                    '->',
                    diff_cols
                )
                check_result.append(
                    0==len(diff_cols)
                )
                pass
            thread_list=[]
            for xl in self.xlset:
                t=Thread(
                    target=check_func,
                    args=(xl,),
                    name=''.join(
                        [
                            r'check_xl_set->',
                            xl.pure_file_name,
                            xl.sheet_name
                        ]
                    )
                )
                thread_list.append(t)
                continue
            start_thread_list(thread_list)
            #  print('check_result:',check_result)
            if check_result==[True]*len(self.xlset):
                self.update_columns(self.xlset[0].columns)
                print('columns checked ok.')
            else:
                print('[Warning:Table] columns not fit.')
                self.update_columns([])
        else:
            print('self.xlmeta is None so nothing to check.\n columns is set by self.xlmap.')
            self.update_columns(
                self.xlmap.columns
            )
            pass
        return deepcopy(self.columns)
    def reload(self):
        self.parse_meta(
            self.xlmeta,
            self.common_title,
            auto_load=True ## self.auto_load
        )
        pass
    def reload_by_map(self,xlmap,keep_meta_info=False):
        self.__init__(
            xlmeta=deepcopy(self.xlmeta),
            common_title=deepcopy(self.common_title),
            xlmap=xlmap,
            use_map=True,
            auto_load=True,
            keep_meta_info=keep_meta_info,
            key_index=deepcopy(self.key_index),
            key_name=deepcopy(self.key_name)
        )
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
    def duplicate(self,use_meta=False):
        table=deepcopy(self)
        table.__clear_data()
        table.__clear_temp()
        if use_meta==False:
            table.xlset=[]
        else:
            pass
        return table
    def caltable(self,use_meta=False):
        if use_meta==True:
            xlmeta=self.xlmeta
        else:
            xlmeta=None
        table=ImmortalTable(
            xlmeta=xlmeta,
            common_title=self.common_title,
            auto_load=False,
            key_index=self.key_index,
            key_name=self.key_name,
            xlmap=self.xlmap,
            use_map=self.use_map,
            keep_meta_info=self.keep_meta_info
        )
        table.fake=True
        return table
    def get_fake(self,copy=False,use_meta=False):
        '''
        if copy is True, call self.duplicate;
        else call self.caltable;
        '''
        if copy==False:
            return self.caltable(use_meta=use_meta)
        else:
            return self.duplicate(use_meta=use_meta)
    def calxl(self,df=None):
        '''
        Generate an calculator XlSheet.
        '''
        calculator=XlSheet(
            [None,'',self.common_title],
            keep_meta_info=False,
            xlmap=self.xlmap,
            use_map=self.use_map,
            key_index=self.key_index,
            key_name=self.key_name
        )
        if df is None:
            return calculator
        else:
            calculator.accept_df(df)
            return calculator
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
                        xl_obj.pure_file_name,
                        xl_obj.sheet_name])
                )
            )
            continue
        start_thread_list(thread_list)
        #  get_df_temp_data(self,over_write=False,type_xl=False)
        #  self.data=concat(self.__df_temp,axis=0,join='outer')
        self.data=self.get_df_temp_data(over_write=True,type_xl=False)
        print('load_raw_data, if columns fit to self.data:',self.columns==list(self.data.columns))
        if self.key_name in self.data.columns:
            self.key_list=list(self.data[self.key_name].drop_duplicates())
        self.load_count+=1
        self.__clear_temp()
        pass
    def __clear_data(self):
        self.data=None
    def get_data(self):
        return deepcopy(self.data)
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
    def clear_temp_df(self):
        self.__clear_temp()
    def __clear_temp(self):
        self.__df_temp=[]
    def append_df_to_temp(self,df):
        self.__df_temp.append(df)
    def get_df_temp_data(self,over_write=False,type_xl=False):
        '''
        parameters:
            over_write:bool
                does not affect self.xlset,
                but only changes self.data;
            type_xl:bool
                determines the result will be a DataFrame or XlSheet;
        '''
        if len(self.__df_temp)==0:
            resu=DataFrame([])
        else:
            resu=concat(
                self.__df_temp,
                axis=0,
                join='outer'
            )
            resu.fillna(0.0,inplace=True)
        if over_write==True:
            self.data=resu
        # self.__clear_temp()
        if type_xl==True:
            resu=self.calxl(df=resu)
            return resu
        else:
            return resu
    def xl2df(self,xl):
        '''
        Transfer XlSheet into it's data;
        '''
        df=deepcopy(xl.data)
        return df
    def df2xl(self,df):
        xl=self.calxl()
        xl.accept_df(deepcopy(df))
        return xl
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
    def save_xlset(self,save_path):
        from autk.parser.funcs import save_df
        for xl in self.xlset:
            save_df(xl.data,xl.sheet_name,save_path)
            continue
        pass
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
    def list_col(self):
        pass
    def vlookup(
        self,
        str_item,
        key_col,
        resu_col=None,
        if_regex=True,
        match_mode=True
    ):
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
                        r'vlookup->',
                        xl.pure_file_name,
                        xl.sheet_name
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
        #  resu_col,
        #  condition_matrix,
        #  filter_type='adv',
        #  unique=False
        *args,
        **kwargs
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
        self.__clear_temp()
        resuli=[]
        def __xl_vlookups(xl,*args,**kwargs):
            resuli.extend(
                xl.vlookups(
                    *args,
                    **kwargs
                )
            )
            pass
        self.apply_xl_func(
            __xl_vlookups,
            *args,
            **kwargs
        )
        resuli=list(
            set(resuli)
        )
        return resuli
    def sumifs(self,target_col,condition,filter_type='adv'):
        '''
        condition can be matrix or dict;
        '''
        to_sum=[]
        def __xl_sumifs(xl):
            to_sum.append(
                xl.sumifs(
                    target_col,
                    condition,
                    filter_type=filter_type
                )
            )
            pass
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__xl_sumifs,
                args=(xl,),
                name=''.join([
                    'sumifs-',
                    xl.pure_file_name,
                    '-',
                    xl.sheet_name,
                    '-',
                    str(xl.title)
                ])
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        return sum(to_sum)
    def apply_df_func(self,df_apply_func,col_index,col_name):
        '''
        Insert a column named 'col_name', at the location 'col_index', where calculates the result of the function 'df_apply_func'.
        df_apply_func:function
        col_index:int
        col_name:str
        This method changes each xl.data in self.xlset, but returns
        nothing;
        '''
        # def __apply_single(
        #         xl,
        #         df_apply_func,
        #         col_index,
        #         col_name
        # ):   
        #     if col_name in xl.data.columns:
        #         xl.data[col_name]=xl.data.apply(
        #             df_apply_func,
        #             axis=1
        #         )
        #     else:
        #         xl.data.insert(
        #             col_index,
        #             col_name,
        #             xl.data.apply(
        #                 df_apply_func,
        #                 axis=1,
        #                 raw=False,
        #                 result_type='reduce'
        #             ),
        #             allow_duplicates=False
        #         )
        #     xl.columns=list(xl.data.columns)
        #     pass
        def __apply_single(xl):
            xl.apply_df_func(df_apply_func,col_index,col_name)
            pass
        thread_list=[]
        for xl in self.xlset:
            t=Thread(
                target=__apply_single,
                args=(
                    xl,
                    # df_apply_func,
                    # col_index,
                    # col_name
                ),
                name=''.join(
                    [
                        r'apply->',
                        getattr(df_apply_func,'__name__'),
                        ',for:',
                        xl.pure_file_name,
                        r',',
                        xl.sheet_name
                    ]
                )
            )
            thread_list.append(t)
        start_thread_list(thread_list)
        pass
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
        `xl_func` must be customized, whose first parameter is xl;
        '''
        thread_list=[]
        for xl in self.xlset:
            thread_list.append(
                Thread(
                    target=xl_func,
                    args=(xl,*args),
                    name=''.join([
                        r'apply_xl_func->',
                        xl_func.__name__,
                        xl.pure_file_name,
                        xl.sheet_name
                    ])
                )
            )
            continue
        start_thread_list(thread_list)
        return
    def append_df(self,in_df):
        '''
        not perfect yet.
        '''
        xl=XlSheet(
            [None,'',self.common_title],
            keep_meta_info=self.keep_meta_info,
            xlmap=self.xlmap,
            use_map=self.use_map,
            key_index=self.key_index,
            key_name=self.key_name
        )
        xl.accept_df(in_df)
        table=ImmortalTable(
            key_index=self.key_index,
            key_name=self.key_name,
            xlmap=self.xlmap,
            keep_meta_info=self.keep_meta_info
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
        '''
        not perfect.
        '''
        # xl=XlSheet(file_path=file_path,sheet_name=sheet_name,title=title,key_index=self.key_index,key_name=self.key_name,xlmap=self.xlmap,keep_meta_info=self.keep_meta_info)
        # self.xlset.append(xl)
        table=ImmortalTable(
            key_index=self.key_index,
            key_name=self.key_name,
            xlmap=self.xlmap,
            keep_meta_info=self.keep_meta_info
        )
        table.parse_meta([file_path,sheet_name,title], self.common_title, auto_load=True)
        if self.data is not None:
            self.data=concat([self.data,table.data],axis=0,join='outer')
        else:
            self.data=table.data
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
