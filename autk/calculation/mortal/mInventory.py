#!/usr/bin/env python
# encoding = 'utf-8'
from autk.parser.funcs import transType,get_time_str
from autk.mapper.map import InvGlMap,XlMap

from autk.calculation.base.xlsht import XlSheet
from autk.calculation.base.table import ImmortalTable

#  fake_cal_map=XlMap()
class CalUnit(XlSheet):
    '''
    XlSheet has the following methods:
        accept_data
        accept_key_index
        filter_adv
        filter_key_record
        filter_list
        filter_num
        filter_str
        get_data
        get_shtli
        save
        vlookup
    An XlMap is necessary for a CalUnit, in that a Calculate Unit must know the index of a specific column.
    '''
    def __init__(self,cal_data,xlmap,cal_name,cal_id):
        super().__init__(['virtual','cal_unit',0], key_index=[], key_name='key_id', xlmap=xlmap,keep_meta_info=False)
        self.xlmap=xlmap
        self.accept_data(cal_data) # self.data=cal_data
        self.columns=self.data.columns
        self.year=None
        self.month=None
        self.cal_id=cal_id
        self.cal_name=cal_name # what are you calculating?
        # self.u_start_num=self.data.iloc[0,self.xlmap.start_num] # unit universal start number
        # self.u_start_amount=self.data.iloc[0,self.xlmap.start_amount] # unit universal start amount
        pass
    def cal_price(self,moving_weighted_average=True,over_write=False,accurate=False):
        '''
        This method will not update self.price_matrix;
        Moving Weighted Average is set to default.
        return:
            DataFrame of self.data with column 'recal_end_price' and 'recal_end_amount'.
        ''' 
        for i in range(self.data.shape[0]):
            amount_temp=self.data.iloc[i,self.xlmap.in_amount]+self.data.iloc[i,self.xlmap.start_amount] # 金额：本期借方+本期期初
            num_temp=self.data.iloc[i,self.xlmap.in_num]+self.data.iloc[i,self.xlmap.start_num] # 数量：本期借方+本期期初
            if isinstance(num_temp, int or float) and num_temp !=0:
                price_temp=amount_temp/num_temp # 试算期末加权平均价格
            else:
                price_temp=0.0
            end_num_temp=self.data.iloc[i,self.xlmap.end_num]
            end_amount_temp=price_temp*end_num_temp
            self.data.iloc[i,self.xlmap.recal_end_price]=price_temp
            # self.data.iloc[i,self.xlmap.recal_end_amount]=end_amount_temp # can be uncommented
            if end_amount_temp != self.data.iloc[i,self.xlmap.end_amount]:
                print(
                    'error:','\n',
                    'row_key:',self.data.iloc[i,self.xlmap.row_key],
                    'materia:',self.data.iloc[i,self.xlmap.material_id],
                    'start_am:',self.data.iloc[i,self.xlmap.start_amount],
                    'in_am:',self.data.iloc[i,self.xlmap.in_amount],
                    'start_n:',self.data.iloc[i,self.xlmap.start_num],
                    'in_n:',self.data.iloc[i,self.xlmap.in_num],
                    'end_price:',price_temp,
                    'end_num:',self.data.iloc[i,self.xlmap.end_num],
                    ';end_amount:',price_temp*self.data.iloc[i,self.xlmap.end_num]
                    )
                print(amount_temp,num_temp)
            continue
        self.data['recal_end_amount']=self.data['end_num']*self.data['recal_end_price']
        pass
    def cal_amount_allocated(self,moving_weighted_average=False):
        '''
        Calculate amount allocated from credit of material cost and write into column 'recal_amount_allocated'.
        This method will overwrite self.data.
        parameters:
            moving_weighted_average: True or False;
        return:
            Updated self.data.
        '''
        pass
    pass
class Inventory(ImmortalTable):
    def __init__(
            self,
            xlmeta,
            auto_load=False,
            common_title=1,
            key_index=[],
            key_name='key_id',
            invmap=InvGlMap()
        ):
        self.calu_set=[] # set of calculate unit
        self.xlmeta=xlmeta
        self.xlmap=invmap
        self.__parse_key()
        self.raw_data=None # whole data in the form of pandas.DataFrame.
        # Core-data attributes:
        self.data=None # updating at anytime.
        self.columns=None
        self.price_matrix=None
        self.data_set=[]# data prepared for multi-thread-data-processing, in which elements are pandas.DataFrame.
        self.row_set=[] # data cache for each thread of row-iteration, in which elements are class 'InvRow'.
        # Detailed-data attributes:
        self.years=None
        self.months=None
        self.material_items=None # raw materials
        self.product_items=None # product project
        self.material_json={}
        self.product_json={}
        # Temporary data attributes:
        self.row_temp=[] # data cache for each thread of filter_sum, after which self.row_temp will be cleared by self.__clear_row_temp(). Elements are pandas.Series yielded by DataFrame.iterrows().
        self.data_temp=[] # used in self.cal_price.
        self.meta_cols=[] # seems useless, used in self.get_pvt.
        self.data_cols=[] # seems useless, used in self.get_pvt.
        super().__init__(xlmeta,common_title=common_title,auto_load=auto_load,key_index=key_index,key_name=key_name,xlmap=invmap)
        pass
    def __parse_key(self):
        if isinstance(self.xlmap, InvMap):
            self.item_json=self.xlmap.item_json
            pass
        else:
            print('no inventory map!')
            pass
        self.item_keys=list(self.item_json.keys())
        self.item_values=list(self.item_json.values())
    pass
if __name__=='__main__':
    pass
