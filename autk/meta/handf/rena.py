#!/usr/bin/env python
# coding=utf-8
import re
import os
import shutil
from pandas import read_excel
# from autk.parser.funcs import f2list
def add_suffix(suffix_str,filedir):
    path_list=[]
    for filename in os.listdir(filedir):
        file_path=os.path.abspath(
            os.path.join(
                filedir,
                filename))
        path_list.append(
            file_path
        )
        continue
    print(path_list)
    def single_rename(file_path):
        filename=file_path.split(os.sep)[-1]
        cur_pure_name=re.sub(r'\..*$','',filename)
        extension_str=re.sub(r'^.*\.','',filename)
        new_path=os.path.join(
            os.sep.join(
                file_path.split(os.sep)[0:-1]
            ),
            ''.join([
                cur_pure_name,
                '-',suffix_str,
                '.',extension_str
            ])
        )
        print('current name:',file_path)
        print('new name',new_path)
        shutil.move(file_path,new_path)
        pass
    for file in path_list:
        single_rename(file)
        continue
    pass
def change_suffix(
    new_suffix,
    file_type_str,
    file_dir,
    preview=True
):
    '''
    Note:
        previous suffix must starts with `-`;
        for different file types, call this function separately;
    parameters:
        new_suffix:
            need not include the `-`;
        file_type_str:
            xls,xlsx,xlsm,etc.
        file_dir:
            where's your files ?
    returns:
        None
    '''
    import shutil
    from autk.handf.findfile import Walker
    def __change_file_name(file_path):
        new_file_path_list=file_path.split(os.sep)
        new_file_path_list[-1]=re.sub(
            r'-.*\.'+file_type_str+'$',
            r'-'+new_suffix+r'.'+file_type_str,
            file_path.split(os.sep)[-1]
        )
        new_file_path=os.sep.join(
            new_file_path_list
        )
        print('before:')
        print('\t',file_path.split(os.sep)[-1])
        print('after:')
        print('\t',new_file_path.split(os.sep)[-1])
        if preview==True:
            pass
        else:
            shutil.move(file_path,new_file_path)
        pass
    w1=Walker(r'\.'+file_type_str+r'$',file_dir,match=False)
    # map(__change_file_name,w1.get_files())
    for f in w1.get_files():
        __change_file_name(f)
        continue
    pass
class Rename:
    def __init__(self,target_dir,meta_path,shtna):
        '''
        Parameters
        ----------
        target_dir : str
            the directory where the files to be renamed locates.
        meta_path : str
            Excel_path.
        shtna : str
            sheet_name, telling you the map of old_name and new_name.

        Returns
        -------
        None.

        '''
        self.meta_path=meta_path
        self.shtna=shtna
        self.target_dir=target_dir
        pass
    def scan(self):
        print(os.listdir(self.target_dir))
        meta=read_excel(self.meta_path,sheet_name=self.shtna,engine='openpyxl')
        print(meta)
        print(meta.dtypes)
        return
    def start_rename(self):
        meta=read_excel(self.meta_path,sheet_name=self.shtna,engine='openpyxl')
        for i in meta.iterrows():
            row_data=i[1]
            old_name=row_data['old_name']
            new_name=row_data['new_name']
            old_file=os.path.abspath(os.path.join(self.target_dir,old_name))
            new_file=os.path.abspath(os.path.join(self.target_dir,new_name))
            # print(old_file)
            # print(new_file)
            try:
                shutil.move(old_file,new_file)
                print('ok!',old_name,'to',new_name)
            except:
                print('rename failed:\n',old_file)
            continue
        return
    pass
if __name__=='__main__':
    pass
