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
    def _overwt_dict(self):
        self.__dict__={}
    def has_cols(self,col_list):
        checkli=[]
        for col in col_list:
            checkli.append(col in self.columns)
            continue
        return [True]*len(col_list)==checkli
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
            self._overwt_dict()
        from os.path import isfile
        if isfile(json_str):
            from autk.gentk.funcs import f2dict
            json_str=f2dict(json_str)
        elif isinstance(json_str,dict):
            pass
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
    def insert_col_name(self,col_name,col_index):
        # TODO
        pass
    def append_col_name(self,col_name):
        if col_name in self.columns:
            print(
                '[Warning]: {} already included in {}.'.format(
                    col_name,self.__class__.__name__
                )
            )
            pass
        else:
            setattr(self,col_name,len(self.show.keys()))
    def extend_col_list(self,col_list):
        for attr_name in col_list:
            self.append_col_name(attr_name)
            continue
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
            '[Note] {} data saved to:{}'.format(
                self.__class__.__name__,
                savepath
            )
        )
        pass
    @property
    def mapname(self):
        return str(self.__class__.__name__)
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
        print('[Note] create map from list: ',columns)
        xlmap=cls()
        xlmap._overwt_dict()
        xlmap.extend_col_list(columns)
        print('[Note] new map created:\n',xlmap.show)
        return xlmap
    @classmethod
    def from_dict(cls,columns):
        print('[Note] create map from dict: ',columns)
        xlmap=cls()
        #  xlmap._overwt_dict()
        xlmap.accept_json(columns,over_write=True)
        return xlmap
    pass
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
class GenChartMap(XlMap):
    def __init__(self):
        self.entity=None
        self.accid=1 #科目编号
        self.accna=2 #科目名称
        self.start_bal_type=4 #期初余额方向
        self.start_amount=5
        self.dr_amount=6
        self.cr_amount=7
        self.end_bal_type=4 #期末余额方向
        self.end_amount=8
        pass
    @property
    def accid_col(self):
        return 'accid'
    @property
    def accna_col(self):
        return 'accna'
    @property
    def drcrdesc(self):
        return ['dr_amount','cr_amount']
    @property
    def top_accid_len(self):
        return 4
    @property
    def accna_split_by(self):
        return r'/'
    #  @property
    #  def key_cols(self):
        #  '''
        #  key_cols必须和Acct类的accept_key_chart_row(key_chart_row)方法联动，否则MCA类的getAcct(accid=6001)方法会得到错误的数据！
        #  '''
        #  return ['accid','accna','start_amount','dr_amount','cr_amount','end_amount','end_bal_type','entity']
    pass
class ChartMap(GenChartMap):
    def __init__(self):
        '''
        key_cols必须和Acct类的accept_key_chart_row(key_chart_row)方法联动，否则MCA类的getAcct(accid=6001)方法会得到错误的数据！
        '''
        # self.if_index_map=if_index_map
        self.top_accid=None #一级科目编号
        self.top_accna=None #一级科目名称
        self.start_amount=5
        self.dr_amount=6
        self.cr_amount=7
        self.end_amount=8
        self.accid=1 #科目编号
        self.accna=2 #科目名称
        self.currency_type=3 # 币种
        self.bal_type=4 #科目方向，余额方向
        # self.num=None # 数量
        # self.measure=None # 量纲
        # self.price=None # 价格
        # self.exchange_rate=None # 汇率
        # self.exchange_amount=None # 外币金额
        # self.exchange_name=None # 外币名称
        # self.drcr=None # 一个数字表示期末余额，正数为借方，负数为贷方，余额表计算这个没有意义
        # self.date=None # 一般科目余额表没有日期，只有年份和月份
        # self.year=None
        # self.month=None
        pass
    @property
    def accid_col(self):
        return 'accid'
    @property
    def accna_col(self):
        return 'accna'
    @property
    def drcrdesc(self):
        return ['dr_amount','cr_amount']
    @property
    def key_cols(self):
        return ['accid','accna','start_amount','dr_amount','cr_amount','end_amount']
    pass
class ApArMap(ChartMap):
    '''
    columns:[科目编号,科目名称,核算项目编号,核算项目名称,币种,方向,期初余额,本期借方发生额,本期贷方发生额,期末余额];
    '''
    def __init__(self):
        self.entity=0
        self.accna=1 # 一级科目名称
        self.company=2 # 客商名称
        self.relation=3 # 与客商的关系，如合并外关联方、合并内关联方、非关联方
        self.year=4 # 年度
        self.start_bal_type=5 # 期初余额方向
        self.start_amount=6
        self.dr_amount=7
        self.cr_amount=8
        self.end_amount=9
        self.end_bal_type=10 # 期末余额方向
        self.drcr=11 # 一个数字表示期末余额，正数为借方，负数为贷方
        # 一年以内
        # self.currency_type=3 # 币种
        # self.num=None # 数量
        # self.measure=None # 量纲
        # self.price=None # 价格
        # self.exchange_rate=None # 汇率
        # self.exchange_amount=None # 外币金额
        # self.exchange_name=None # 外币名称
        # self.date=None # 一般科目余额表没有日期，只有年份和月份
        # self.month=None
        pass
    @property
    def aging_items(self):
        #  return ['0-1','1-2','2-3','3-4','4-5','5-inf'] # 账龄项目的划分,inf代表"无穷"。"inf" represents "infinite"；
        return ['age0','age1','age2','age3','age4','age5'] # 账龄项目的划分,inf代表"无穷"。"inf" represents "infinite"；
    pass
class InvChartMap(XlMap):
    '''
    汇总进销存;
    '''
    def __init__(self,ages_count):
        self.name=0
        self.invid=1
        self.measure=2
        self.append_col_list(self.num_cols)
        self.append_col_list(self.amt_cols)
        self.set_age_cols(ages_count)
        #  self.append_col_list(self.get_age_cols(ages_count))
        pass
    @property
    def num_cols(self):
        return ['num_start','num_in','num_out','num_bal']
    @property
    def amt_cols(self):
        return ['amt_start','amt_in','amt_out','amt_bal']
    @property
    def key_index(self):
        return ['id']
    @property
    def key_name(self):
        return 'id'
    def set_age_cols(self,ages_count):
        age_cols=[]
        for n in range(ages_count):
            age_cols.append(
                r'age'+str(n)
            )
            continue
        self.append_col_list(age_cols)
        return age_cols
    def get_age_cols(self,ages_count):
        age_cols=[]
        for n in range(ages_count):
            age_cols.append(
                r'age'+str(n)
            )
            continue
        return age_cols
    pass
def InvMonthMap(InvChartMap):
    '''
    分月进销存;
    '''
    def __init__(self,ages_count):
        self.month=0
        pass
    pass
## the following 2 functions may be useless.
def get_glmap(columns,key_index=['date','mark','jrid'],drcrdesc=['dr_amount','cr_amount']):
    '''
    columns must be included:
        glid,date,mark,jrid,accid,accna,dr_amount,cr_amount,item_name,note;
    '''
    print('[Note] columns of glmap:\n',columns)
    class InstantMap(MglMap):
        def __init__(self):
            if isinstance(columns,list):
                for n in range(len(columns)):
                    setattr(self,columns[n],n)
                    continue
            elif isinstance(columns,dict):
                for k in columns.keys():
                    setattr(self,k,columns[k])
            else:
                print('[Warning] invalid columns:',columns)
                pass
            pass
        @property
        def key_index(self):
            key_index=deepcopy(key_index)
            return key_index
        @property
        def drcrdesc(self):
            drcrdesc=deepcopy(drcrdesc)
            return drcrdesc
        pass
    return InstantMap
def get_chmap(columns,drcrdesc=['dr_amount','cr_amount'],accna_split_by=r'/',top_accid_len=4):
    '''
    columns must be included:
        accid,accna,dr_amount,cr_amount;
        glid,date,mark,jrid,accid,accna,dr_amount,cr_amount,item_name,note;
    '''
    print('[Note] columns of chmap:\n',columns)
    class InstantMap(GenChartMap):
        def __init__(self):
            if isinstance(columns,list):
                for n in range(len(columns)):
                    setattr(self,columns[n],n)
                    continue
            elif isinstance(columns,dict):
                for k in columns.keys():
                    setattr(self,k,columns[k])
            else:
                print('[Warning] invalid columns:',columns)
                pass
            pass
        pass
        #  @property
        #  def drcrdesc(self):
            #  drcrdesc=deepcopy(drcrdesc)
            #  return drcrdesc
        #  @property
        #  def accna_split_by(self):
            #  accna_split_by=deepcopy(accna_split_by)
            #  return accna_split_by
        #  @property
        #  def top_accid_len(self):
            #  top_accid_len=deepcopy(top_accid_len)
            #  return top_accid_len
    return InstantMap
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
