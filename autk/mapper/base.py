#!/usr/bin/env python
# coding=utf-8

from copy import deepcopy

class XlMap:
    '''
    class XlMap indicates the index of a data sheet's columns.
    XlMap is mapping column name to column index.
    If you need a new column which does not exist, set its value to None.
    col_name_in_xlmap(col_name=name of attribute) ---> col_index_in_xlmap(col_index=value of attribute) ---> col_name_in_actual_file (col_name=column_of_file[col_index])
    columns of GL is right in the same order of attributes of map object.
    Remember:index starts from 0.
    '''
    def __add__(self,other):
        resu=self.__class__.from_dict(
            {**self.show,**other.show}
        )
        return resu
    def clear_cols(self):
        self.__dict__={}
    def get_index(self,col_name):
        return self.show[col_name]
    def has_cols(self,col_list):
        checkli=[]
        for col in col_list:
            checkli.append(col in self.columns)
            continue
        return [True]*len(col_list)==checkli
    def invert_has_cols(self,col_list):
        checkli=[]
        for col in self.columns:
            checkli.append(col in col_list)
            continue
        return [True]*len(self.columns)==checkli
    def accept_json(self,json_str,over_write=False):
        '''
        The passed argument 'json_str' indicates the location of the column
        in the parsing xlsx file, by its value of each key (representing the column name);
        {
            "column_name_1":location_number_1,
            "column_name_2":location_number_2,
            "column_name_3":location_number_3
        }
        '''
        if over_write==True:
            self.clear_cols()
        from os.path import isfile
        if isinstance(json_str,dict):
            pass
        elif isfile(json_str):
            from autk.gentk.funcs import f2dict
            json_str=f2dict(json_str)
        else:
            pass
        for k in self.__dict__.keys():
            if k not in json_str.keys():
                # those attributes not in json_str must be set to None;
                setattr(self,k,None)
            continue
        for k in json_str:
            # then set correct attributes according to json_str;
            setattr(self,k,json_str[k])
            continue
    def append_col_name(self,col_name):
        if col_name in self.columns:
            print(
                '[Warning][{}]: `{}` already included in {}.'.format(
                    self.__class__.__name__,
                    col_name,
                    self.__class__.__name__
                )
            )
            pass
        else:
            col_index=len(self.columns) # may cause bug when col_index not in original columns from XLSX book;
            setattr(self,col_name,None) # so `col_index` was replaced by `None`;
            print(
                '[{}] append new column `{}` at index `{}`.'.format(
                    self.__class__.__name__,
                    col_name,
                    col_index
                )
            )
    def extend_col_list(self,col_list):
        for col in col_list:
            self.append_col_name(col)
            continue
        pass
    def change_cols(self,new_cols):
        '''
        `new_cols` can be dict or list.
        '''
        if isinstance(new_cols,dict):
            self.accept_json(new_cols,over_write=True)
        elif isinstance(new_cols,list):
            self.clear_cols()
            self.extend_col_list(new_cols)
        pass
    def insert_col_name(self,col_name,col_index):
        # TODO
        pass
    def save(self,savepath):
        from json import dumps
        with open(savepath,'w',encoding='utf-8') as f:
            f.write(
                dumps(
                    self.show,
                    ensure_ascii=False,
                    indent=4
                )
            )
        print(
            '[{}] data saved to:{}'.format(
                self.__class__.__name__,
                savepath
            )
        )
        pass
    @property
    def mapname(self):
        return str(self.__class__.__name__)
    @property
    def data(self):
        return self.__dict__
    @property
    def show(self):
        return self.__dict__
    @property
    def columns(self):
        return list(
            self.show.keys()
        )
    @property
    def key_name(self):
        return 'keyid'
    @property
    def key_index(self):
        return []
    @classmethod
    def from_list(cls,columns):
        print(
            '[{}] creates map from list: {}.'.format(
                cls.__name__,
                columns
            ),
        )
        xlmap=cls()
        xlmap.clear_cols()
        xlmap.extend_col_list(columns)
        print(
            '[{}] new map created:\n{}'.format(
                cls.__name__,
                xlmap.show
            ),
        )
        return xlmap
    @classmethod
    def from_dict(cls,columns):
        print(
            '[{}] create map from dict:\n{}.'.format(
                cls.__name__,
                columns,
            ),
        )
        xlmap=cls()
        #  xlmap.clear_cols()
        xlmap.accept_json(columns,over_write=True)
        return xlmap
    pass

# standard item json:
inv_gl_json={
    "row_key":0,
    "date":1,
    "warehouse":2,
    "material_id":3,
    "material":4,
    "model":5,
    "measure":6,
    "product_id":7,
    "product":8,
    "note":9,
    "year":10,
    "month":11,
    "start_price":12,
    "start_num":13,
    "start_amount":14,
    "in_price":15,
    "in_num":16,
    "in_amount":17,
    "out_price":18,
    "out_num":19,
    "out_amount":20,
    "end_price":21,
    "end_num":22,
    "end_amount":23,
    "recal_end_price":24,
    "recal_end_amount":25,
    "recal_amount_allocated":26
}
gl_item_json={
    "if_sampled":False,
    "glid":None,
    "top_accid":None,
    "top_accna":None,
    "dr_amount":7,
    "cr_amount":8,
    "accid":5,
    "accna":6,
    "note":4, # 摘要
    "item_name":16, # 核算名称
    "item_id":15, # 核算编号
    "date":1,
    "year":None,
    "month":None,
    "mark":2, # 字
    "jrid":3, # journal entry id ,号
    "opposite_accid":None,
    "opposite_accna":None,
    "num":12, # 数量
    "measure":14, # 量纲
    "price":13, # 价格
    "exchange_rate":9, # 汇率
    "exchange_amount":10, # 外币金额
    "exchange_name":11, # 外币名称
    "drcr":None # 一个数字表示金额，正数为借方，负数为贷方
}
ca_item_json={
    "top_accid":None, # 一级科目编号
    "top_accna":None, # 一级科目名称
    "start_amount":5,
    "dr_amount":6,
    "cr_amount":7,
    "end_amount":8,
    "accid":1, # 科目编号
    "accna":2, # 科目名称
    "currency_type":3, # 数据类型
    "bal_type":4, #科目方向，余额方向
    "num":None, # 数量
    "measure":None, # 量纲
    "price":None, # 价格
    "exchange_rate":None, # 汇率
    "exchange_amount":None, # 外币金额
    "exchange_name":None, # 外币名称
    "drcr":None, # 一个数字表示期末余额，正数为借方，负数为贷方
    "date":None, # 一般科目余额表没有日期，只有年份和月份
    "year":None,
    "month":None
}

# row_key
# date
# warehouse
# material_id
# material
# model
# measure
# product_id
# product
# note
# year
# month
# start_price
# start_num
# start_amount
# in_price
# in_num
# in_amount
# out_price
# out_num
# out_amount
# end_price
# end_num
# end_amount
# recal_end_price
# recal_end_amount
# recal_amount_allocated


if __name__=='__main__':
    pass
