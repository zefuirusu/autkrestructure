#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import DataFrame

from autk.gentk.funcs import relative_path
from autk.meta.handf.findfile import find_regex
from autk.calculation.base.xlsht import XlSheet
from autk.calculation.mortal.mortalgl import MGL
from autk.mapper.smpmap import SampleMap

# MSP: Mortal Sample
class MSP(XlSheet):
    def __init__(self,gl:MGL):
        self.gl=gl
        self.xlmap=SampleMap().from_list([
            'id',
            'type',
            'entity',
            self.gl.xlmap.key_name,
            'item_name',
            'if_collected',
            'comment',
            'ref_link',
            'location',
            'attachment'
        ])
        super().__init__(xlmap=self.xlmap,shmeta=None)
        pass
    @property
    def show(self):
        show={
            "sample":{
                self.__class__.__name__:
                self.name
            },
            "data_size":(
                self.data.shape 
                if isinstance(self.data,DataFrame) else None
            ),
            "xlmap":{
                str(type(self.xlmap)):
                None if self.xlmap is None else self.xlmap.show
            },
            "shmeta":{
                self.shmeta.__class__.__name__:
                None if self.shmeta is None else self.shmeta.data
            }
        }
        return show
    def update_location(
        self,
        by_func,
        search_dir='.',
    ):
        def __simple_parse(row_series):
            fdresu=find_regex(
                by_func(row_series[self.gl.xlmap.key_name]),
                search_dir=search_dir,
                match=False
            )
            if fdresu[0]==[] and fdresu[1]==[]:
                location_str=None
            elif fdresu[0] !=[] and fdresu[1] ==[]:
                location_li=[0]
                if len(location_li)==1:
                    location_str=location_li[0]
                else:
                    location_str=';'.join(location_li)
            elif fdresu[0] ==[] and fdresu[1] !=[]:
                location_li=fdresu[1]
                if len(location_li)==1:
                    location_str=location_li[0]
                else:
                    location_str=';'.join(location_li)
            else:
                location_str=fdresu
            return location_str
        self.apply_df_func(
            __simple_parse,
            'location'
            #  self.xlmap.location
        )
        pass
    def generate_link(
        self,
        ref_path
    ):
        def __xl_func_hyperlink(row_series):
            if isinstance(row_series['location'],str):
                func_str='='+''.join([
                    'hyperlink("',
                    relative_path(
                        row_series['location'],
                        ref_path
                    ),
                    '","open:',
                    row_series['glid'],
                    '")'
                ])
            else:
                func_str=None
            return func_str
        self.apply_df_func(
            __xl_func_hyperlink,
            'ref_link'
        )
        pass
    pass

if __name__=='__main__':
    pass
