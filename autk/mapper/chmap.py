#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.mapper.base import XlMap

class MchMap(XlMap):
    def __init__(self):
        pass
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
    def top_accid_len(self):
        return 4
    @property
    def accna_split_by(self):
        return r'/'
    pass
class EchMap(MchMap):
    def __init__(self):
        '''
        key_cols必须和Acct类的accept_key_chart_row(key_chart_row)方法联动，
        否则MCA类的getAcct(accid=6001)方法会得到错误的数据！
        '''
        # self.if_index_map=if_index_map
        self.entity=None
        self.year=None
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
    pass
class ApArMap(MchMap):
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
    print(
        '[{}] columns of glmap:\n{}.'.format(
            self.__class__.__name__,
            columns
        ),
    )
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
                print(
                    '[Warning][{}] invalid columns:{}'.format(
                        self.__class__.__name__,
                        columns
                    ),
                )
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
