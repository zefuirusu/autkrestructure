#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.brother.xlbk import XlBook
from autk.gentk.quick import MGL

class Wksh(XlBook): # WorkSheet
    def __init__(
        self,
        workpath,
    ):
        super().__init__(workpath)
        pass
    def fill_balance(self):
        pass
    def fill_profit_loss(self):
        pass
    def fill_rand_sample(
        self,
        method:dict,
        key_cols:list,
        gl:MGL,
        paste_mode=False
    ):
        '''
        This method does not work well with multi-entity MGL,
        in that `Wksh` cannot distinguish entities from MGL;
        method:dict
            {
                'sheet1':[start_index,sample_size,'accid_regex_str','drcr_type']
            }
        key_cols:list
            table structure of the destination to paste;
            `cols` are members from `gl.xlmap.columns`;
        '''
        print(
            '[{}]start working with paper:{}'.format(
                self.__class__.__name__,
                self.file_path,
            )
        )
        for shtna in list(method.keys()):
            start_index=method[shtna][0]
            sample_size=method[shtna][1]
            accid_regex_str=method[shtna][2]
            drcr_type=method[shtna][3]
            if drcr_type=='dr':
                num_col=gl.xlmap.drcrdesc[0]
                condition={
                    "string":[
                        [accid_regex_str,gl.xlmap.accid_col,True,False]
                    ],
                    "number":[
                        [num_col,">",0]
                    ]
                }
            elif drcr_type=='cr':
                num_col=gl.xlmap.drcrdesc[1]
                condition={
                    "string":[
                        [accid_regex_str,gl.xlmap.accid_col,True,False]
                    ],
                    "number":[
                        [num_col,">",0]
                    ]
                }
            else:
                condition={
                    "string":[
                        [accid_regex_str,gl.xlmap.accid_col,True,False]
                    ],
                    "number":[]
                }
                pass
            population=gl.filter(
                condition,
                filter_type='adv',
                over_write=False,
                type_xl=False
            )
            rand_sample=population.sample(
                n=min(sample_size,population.shape[0])
            )
            data=rand_sample[key_cols].values
            if paste_mode==True:
                self.paste_matrix(
                    data,
                    start_index,
                    shtna
                )
            else:
                self.insert_matrix(
                    data,
                    start_index,
                    shtna,
                    col_ins=False
                )
            continue
        pass
    pass
