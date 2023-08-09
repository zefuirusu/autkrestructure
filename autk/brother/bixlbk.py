#!/usr/bin/env python
# -*- coding: utf-8 -*-

from xlrd import open_workbook
from openpyxl import load_workbook
from pandas import DataFrame
from threading import Thread

from autk.parser.funcs import start_thread_list
from autk.reader.base.xlbk import XlBook

def map_copy(matrix):
    '''
    matrix is like:
    |from_bk|from_sheet|from_row|from_col|to_bk|to_sheet|to_row|to_col|
    '''
    pass
class BiXl:
    def __init__(self,from_path,to_path):
        self.from_bk=XlBook(from_path)
        self.to_bk=XlBook(to_path)
        pass
    def cp_bydf(self,matrix):
        '''
        columns of matrix:
            |to_sht|to_row|to_col|from_sht|from_row|from_col|
        '''
        def __single_cp(matrix_row):
            value=self.from_bk.get_value(
                matrix_row[3],
                (matrix_row[4],matrix_row[5])
            )
            self.to_bk.fill_value(
                matrix_row[0],
                (matrix_row[1],matrix_row[2]),
                value
            )
            pass
        #  thread_list=[]
        for row in matrix.iterrows():
            __single_cp(row[1])
            #  thread_list.append(
                #  Thread(
                    #  target=__single_cp,
                    #  args=(row[1],)
                #  )
            #  )
            continue
        #  start_thread_list(thread_list)
        pass
    pass
if __name__=='__main__':
    pass
