#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.mapper.base import XlMap

class MglMap(XlMap):
    '''
    columns must be included:
        glid,date,mark,jrid,accid,accna,dr_amount,cr_amount;
    '''
    def __init__(self):
        pass
    @property
    def key_name(self):
        return 'glid'
    @property
    def key_index(self):
        return ['date','mark','jrid']
    @property
    def drcrdesc(self):
        return ['dr','cr']
    @property
    def accid_col(self):
        return 'accid'
    @property
    def accna_col(self):
        return 'accna'
    @property
    def top_accid_col(self):
        return 'top_accid'
    @property
    def top_accna_col(self):
        return 'top_accna'
    @property
    def date_col(self):
        return 'date'
    @property
    def top_accid_len(self):
        return 4
    @property
    def accna_split_by(self):
        return r'/'
    @property
    def date_split_by(self):
        return r'-'
    pass
class EglMap(MglMap):
    '''
    Mapping for keys from MGL object towards GeneralLedger Template.
    ['抽', '凭证日期', '字', '号', '摘要', '科目编号', '科目全路径', '借方发生金额', '贷方发生金额', '汇率',
       '外币金额', '外币名称', '数量额', '单价', '计量单位', '核算编号', '核算名称']
    '''
    def __init__(self):
        # self.if_index_map=if_index_map
        self.glid=None
        self.top_accid=None
        self.top_accna=None
        self.dr_amount=6
        self.cr_amount=7
        self.accid=4
        self.accna=5
        self.note=3 # 摘要
        self.item_name=15 # 核算名称
        self.item_id=14 # 核算编号
        self.date=0
        self.year=None
        self.month=None
        self.mark=1 # 字
        self.jrid=2 # journal entry id ,号
        self.opposite_accid=None
        self.opposite_accna=None
        self.num=11 # 数量
        self.measure=13 # 量纲
        self.price=12 # 价格
        self.exchange_rate=8 # 汇率
        self.exchange_amount=9 # 外币金额
        self.exchange_name=10 # 外币名称
        self.drcr=None # 一个数字表示金额，正数为借方，负数为贷方
        pass
    def _overwt_dict(self):
        self.__dict__={
            'glid':None,
            'accid':None,
            'accna':None,
            'date':None,
            'mark':None,
            'jrid':None
        }
    @property
    def key_index(self):
        return ['date','mark','jrid']
    @property
    def key_name(self):
        return 'glid'
    @property
    def drcrdesc(self):
        return ['dr_amount','cr_amount']
    @property
    def accid_col(self):
        return 'accid'
    @property
    def accna_col(self):
        return 'accna'
    @property
    def date_col(self):
        return 'date'
    @property
    def date_split_by(self):
        return r'-'
    pass
class SampleEglMap(EglMap):
    def __init__(self):
        # self.if_index_map=if_index_map
        self.if_sampled=0
        self.glid=None
        self.top_accid=None
        self.top_accna=None
        self.dr_amount=7
        self.cr_amount=8
        self.accid=5
        self.accna=6
        self.note=4 # 摘要
        self.item_name=16 # 核算名称
        self.item_id=15 # 核算编号
        self.date=1
        self.year=None
        self.month=None
        self.mark=2 # 字
        self.jrid=3 # journal entry id ,号
        self.opposite_accid=None
        self.opposite_accna=None
        self.num=12 # 数量
        self.measure=14 # 量纲
        self.price=13 # 价格
        self.exchange_rate=9 # 汇率
        self.exchange_amount=10 # 外币金额
        self.exchange_name=11 # 外币名称
        self.drcr=None # 一个数字表示金额，正数为借方，负数为贷方
        pass
    pass
class InvGlMap(XlMap):
    '''
    'Inv' symbolizes "Inventory".
    '''
    def __init__(self):
        self.row_key=0
        self.date=1
        self.warehouse=2
        self.material_id=3
        self.material=4
        self.model=5
        self.measure=6
        self.product_id=7
        self.product=8
        self.note=9
        self.year=10
        self.month=11
        self.start_price=12
        self.start_num=13
        self.start_amount=14
        self.in_price=15
        self.in_num=16
        self.in_amount=17
        self.out_price=18
        self.out_num=19
        self.out_amount=20
        self.end_price=21
        self.end_num=22
        self.end_amount=23
        self.recal_end_price=24
        self.recal_end_amount=25
        self.recal_amount_allocated=26
        pass
    @property
    def item_json(self):
        resu={}
        for k in self.__dict__:
            resu[k]=self.__dict__[k]
        return resu
    @property
    def key_index(self):
        return 'material_id'
    @property
    def key_name(self):
        return 'material_id'
    pass
