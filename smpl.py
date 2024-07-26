#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import DataFrame

from autk.calculation.base.xlsht import XlSheet
from autk.calculation.mortal.mortalgl import MGL
from autk.mapper.smpmap import SampleMap

# MSP:Mortal Sample
class MSP(XlSheet):
    def __init__(self,gl:MGL):
        self.xlmap=SampleMap()
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
                #  str(type(self.shmeta)):
                None if self.shmeta is None else self.shmeta.data
            }
        }
        return show
    pass

if __name__=='__main__':
    pass
