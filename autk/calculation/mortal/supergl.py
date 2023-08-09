#!/usr/bin/env python
# encoding = 'utf-8'
from copy import deepcopy
from pandas import concat
from threading import Thread

from autk.reader.mortal.mortalgl import MGL
from autk.parser.funcs import f2dict,get_time_str,start_thread_list,save_df

class SGL:
    def __init__(
            self,
            meta_json_path=None,
            nick_name='sgl',
            great_key_name='great_glid',
            entity_col='entity'
    ):
        '''
        meta_file is a json file like:
            {
                "name_1":mgl_args_tuple_1,
                "name_2":mgl_args_tuple_2,
                ...
            }
        '''
        if nick_name !='sgl':
            self.name=nick_name
        else:
            self.name=nick_name+get_time_str()
        self.great_key_name=great_key_name
        self.entity_col=entity_col
        self.mgls=[]
        self.mgl_ref={}
        self.meta_json_path=meta_json_path
        if self.meta_json_path is not None:
            self.sglmeta=f2dict(self.meta_json_path)
            self.load_mgl()
            self.set_entity_name()
            self.set_great_glid()
            self.fresh_columns()
        else:
            self.sglmeta=None
        pass
    def load_mgl(self):
        def __load_single_gl(gl_name):
            mgl_arg_dict=self.sglmeta[gl_name]
            mgl_arg_dict.update({"nick_name":gl_name})
            single_mgl=MGL(**mgl_arg_dict)
            self.mgls.append(single_mgl)
            self.mgl_ref.update({gl_name:single_mgl})
            #  setattr(self,gl_name,single_mgl)
        thread_list=[]
        for gl_name in self.sglmeta.keys():
            t=Thread(
                target=__load_single_gl,
                args=(gl_name,),
                name='appnd_mgl_to'+self.name
            )
            thread_list.append(t)
        start_thread_list(thread_list)
        pass
    def clear_data(self):
        self.mgls=[]
        self.mgl_ref={}
        pass
    def reload(self):
        self.clear_data()
        self.load_mgl()
        self.fresh_columns()
        pass
    def accept_mgl_list(self,mgl_list,over_write=False):
        if over_write==True:
            self.clear_data()
        self.mgls.extend(mgl_list)
        for mgl in mgl_list:
            # does method "update" of dict result in deepcopy?
            self.mgl_ref.update({mgl.name:mgl})
        pass
    def get_mgl_ref(self,mgl_name):
        return self.mgl_ref[mgl_name]
    def get_mgl_copy(self,mgl_name):
        return deepcopy(
            self.mgl_ref[mgl_name]
        )
    def xl_apply(self,df_apply_func,col_index,col_name):
        def __apply_single(xl):
            xl.apply_df_func(df_apply_func,col_index,col_name)
        thread_list=[]
        for mgl in self.mgls:
            for xl in mgl.xlset:
                thread_list.append(
                    Thread(
                        target=__apply_single,
                        args=(xl,),
                        name='_'.join([
                            mgl.name,
                            xl.name,
                            'apply',
                            df_apply_func.__name__
                        ])
                    )
                )
                continue
            continue
        start_thread_list(thread_list)
        pass
    def mgl_apply(self,df_apply_func,col_index,col_name):
        def __apply_single(mgl):
            mgl.apply_df_func(df_apply_func,col_index,col_name)
        thread_list=[]
        for mgl in self.mgls:
            thread_list.append(
                Thread(
                    target=__apply_single,
                    args=(mgl,),
                    name='_'.joion([
                        mgl.name,'apply',df_apply_func.__name__
                    ])
                )
            )
            continue
        start_thread_list(thread_list)
        pass
    def set_entity_name(self):
        def __set_entity_name_single(mgl):
            mgl.apply_df_func(
                lambda series:mgl.name,
                0,
                self.entity_col
            )
            pass
        thread_list=[]
        for gl in self.sglmeta.keys():
            t=Thread(
                target=__set_entity_name_single,
                args=(self.get_mgl_ref(gl),),
                name=gl+'_set_entity_name'
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        pass
    def set_great_glid(self):
        self.xl_apply(
            lambda series:'-'.join([
                series[self.entity_col],
                series['glid']
            ]),
            2,
            self.great_key_name
        )
        pass
    def export(self,savepath):
        for mgl in self.mgls:
            mgl.load_raw_data()
            save_df(mgl.data,mgl.name,savepath)
        pass
    @property
    def mglscan(self):
        '''
        ['mgl_name','file','sheet_name','title','shape','map']
        '''
        mglscan_col=['mgl_name','file','sheet_name','title','shape','map']
        mglscan_df=[]
        for gl in self.mgls:
            for xl in gl.xlset:
                mglscan_df.append(
                    [
                        gl.name,
                        xl.pure_file_name,
                        xl.sheet_name,
                        xl.title,
                        xl.data.shape,
                        xl.xlmap.name
                    ]
                )
            continue
        from pandas import DataFrame
        mglscan_df=DataFrame(mglscan_df,columns=mglscan_col)
        return mglscan_df
    @property
    def colscan(self):
        '''
        在各MGL表头结构不同的情况下，这个函数的结果目前不能对齐；
        '''
        col_meta=[]
        for gl in self.mgls:
            for xl in gl.xlset:
                cols=xl.columns
                col_meta_fake=[gl.name,xl.pure_file_name,xl.sheet_name]
                col_meta_fake.extend(cols)
                col_meta.append(col_meta_fake)
                continue
            continue
        from pandas import DataFrame
        col_meta=DataFrame(col_meta).T
        return col_meta
    def fresh_columns(self):
        common_cols=[]
        def __mgl_fresh_cols(mgl):
            common_cols.extend(mgl.check_xl_cols())
            pass
        thread_list=[]
        for mgl in self.mgls:
            t=Thread(
                target=__mgl_fresh_cols,
                args=(mgl,),
                name=mgl.name+'_fresh_cols'
            )
            thread_list.append(t)
            continue
        start_thread_list(thread_list)
        common_cols=list(
                set(common_cols)
            )
        diff_cols={}
        def __mgl_check(mgl):
            diff_cols.update(
                {mgl.name:
                list(
                    set(common_cols)-set(mgl.columns)
                )}
            )
            pass
        thread_list=[]
        for mgl in self.mgls:
            thread_list.append(
                Thread(
                    target=__mgl_check,
                    args=(mgl,),
                    name=mgl.name+'_check_cols'
                )
            )
        start_thread_list(thread_list)
        if [[]]*len(diff_cols.values()) == list(diff_cols.values()):
            self.update_columns(self.mgls[0].columns)
            return deepcopy(self.mgls[0].columns)
        else:
            self.update_columns([])
            print('columns not fit:',diff_cols)
            return diff_cols
    def update_columns(self,new_cols):
        setattr(
            self,
            'columns',
            deepcopy(
                list(new_cols)
            )
        )
        return new_cols
    def calxl(self):
        pass
    def calmgl(self):
        pass
    def calsgl(self,use_meta=False):
        if use_meta==True:
            new_meta_path=deepcopy(self.meta_json_path)
        else:
            new_meta_path=None
        new_sgl=SGL(
            meta_json_path=new_meta_path,
            nick_name=self.name+'_copy',
            great_key_name=deepcopy(self.great_key_name),
            entity_col=deepcopy(self.entity_col)
        )
        return new_sgl
    @property
    def data(self):
        dfli=[]
        def __mgl_load_raw_data(mgl):
            mgl.load_raw_data()
            dfli.append(mgl.data)
            pass
        thread_list=[]
        for mgl in self.mgls:
            thread_list.append(
                Thread(
                    target=__mgl_load_raw_data,
                    args=(mgl,),
                    name=''
                )
            )
            continue
        start_thread_list(thread_list)
        return concat(
            dfli,
            axis=0,
            join='outer'
        )
        pass
    def dfli_concat(self,dfli,type_gl=False):
        '''
        this method often works with self.apply_mgl_func;
        '''
        if type_gl==False:
            resu=concat(dfli,axis=0,join='outer')
        else:
            resu=self.calsgl(use_meta=False)
            resu.accept_mgl_list(dfli,over_write=True)
        return resu
    def apply_mgl_func(self,mgl_func,*args):
        '''
        This method starts multi-thread to manipulate every mgl,
        in self.mgl to call one of his method,
        so as to collect data into outside data-capsule.
        This method often works with self.dfli_concat.
        -------
        mgl_func is a customized function to manipulate mgl object in self.mgls,
        whose first parameter of mgl_func must be mgl object,
        where you can call method of mgl inside function mgl_func,
        and collect what is returned into data_capsule outside;
        '''
        thread_list=[]
        for mgl in self.mgls:
            thread_list.append(
                Thread(
                    target=mgl_func,
                    args=(mgl,*args),
                    name=mgl.name+mgl_func.__name__
                )   
            )
            continue
        start_thread_list(thread_list)
        pass
    def mgl_run_collect(self,customized_func_name,args):
        '''
        run func_name of mgl in self.mgls and collect df into dfli, then return dfli.
        this method seems failed.
        '''
        dfli=[]
        return dfli
    def filter(
        self,
        *args,
        **kwargs
    ):
        '''
        Note:keyword arguments are not allowed.
        '''
        dfli=[]
        def __mgl_filter(
            mgl,
            *args,
            **kwargs
        ):
            dfli.append(
                mgl.filter(
                    *args,
                    **kwargs
                )
            )
            pass
        self.apply_mgl_func(__mgl_filter,*args,**kwargs)
        return self.dfli_concat(dfli,False)
    def vlookup(self,*args,**kwargs):
        dfli=[]
        def __mgl_vlookup(mgl,*args,**kwargs):
            dfli.append(
                mgl.vlookup(
                    *args,
                    **kwargs
                )
            )
            pass
        self.apply_mgl_func(__mgl_vlookup,*args,**kwargs)
        return self.dfli_concat(dfli,False)
    def vlookups(self):
        pass
    def sumifs(self,*args,**kwargs):
        dfli=[]
        def __mgl_sumifs(mgl,*args,**kwargs):
            dfli.append(
                mgl.sumifs(
                    *args,
                    **kwargs
                )
            )
        self.apply_mgl_func(__mgl_sumifs,*args,**kwargs)
        return sum(dfli)
    def getitem(
        self,
        regex_str,
        by='item_name',
        key_name=None,
        over_write=False,
        type_gl=False
    ):
        dfli=[]
        def __mgl_getitem(mgl,type_gl=False):
            dfli.append(
                mgl.getitem(
                    regex_str,
                    by,
                    key_name,
                    over_write,
                    type_gl
                )
            )
            pass
        thread_list=[]
        for mgl in self.mgls:
            thread_list.append(
                Thread(
                    target=__mgl_getitem,
                    args=(mgl,type_gl),
                    name=mgl.name+''+__mgl_getitem.__name__
                )
            )
            continue
        start_thread_list(thread_list)
        if type_gl==False:
            resu=concat(dfli,axis=0,join='outer')
        else:
            resu=self.calsgl(use_meta=False)
            resu.accept_mgl_list(dfli,over_write=True)
        return resu
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
        '''
        accid_item,
        side='all',
        pure=False,
        accurate=False,
        over_write=False,
        type_xl=False,
        accid_label=None
        '''
        pass
    def correspond(
        self,
        *args,
        **kwargs
    ):
        '''
        cr_accid,
        dr_accid,
        accurate=False,
        type_gl=False
        '''
        dfli=[]
        def __mgl_correspond(mgl,*args,**kwargs):
            dfli.append(
                mgl.correspond(
                    *args,
                    **kwargs
                )
            )
            pass
        self.apply_mgl_func(__mgl_correspond,*args,**kwargs)
        return self.dfli_concat(dfli,type_gl=args[3])
    def multi_acct_analysis(self,*args,**kwargs):
        '''
        Not perfect.
        Each entity share the same sheet_name in the ouput file saved.
        '''
        def __mgl_multi_acct_analysis(mgl,*args,**kwargs):
            mgl.multi_acct_analysis(*args,**kwargs)
            pass
        self.apply_mgl_func(__mgl_multi_acct_analysis,*args,**kwargs)
        pass
    def inner_sale(self,incomli):
        '''
        incomli: inner company name list;
        '''
        pass
    def scan_acct(self,by_id=False):
        pass
    pass
if __name__=='__main__':
    pass
