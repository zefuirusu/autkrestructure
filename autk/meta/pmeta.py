#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
json config file is in the format of:
{
    "Excel path 1":[
        ["sheet 1",0],
        ["sheet 2",1],
        ...
    ],
    "Excel path 2":[
        ["sheet 3",4],
        ["sheet 4",12]
    ],
    ...
}
'''

from os.path import isfile,isdir
from threading import Thread

from autk.gentk.funcs import f2dict,start_thread_list
from autk.meta.handf.findfile import find_regex

class JsonMeta:
    '''
    JsonMeta.data is like:
    {
        "path 1":[
            ["sheet 1",0],
            ["sheet 2",3]
        ],
        "path 2":[
            ["sheet 1",0],
            ["sheet 3",4]
        ]
    }
    '''
    def __init__(self,json_str):
        self.keep_additional=True
        self.path='json'
        self.data={}
        if isinstance(json_str,dict):
            self.data=json_str
        elif isfile(json_str):
            self.data=f2dict(json_str)
        else:
            self.data={}
        pass
    def __add__(self,other):
        join_meta=JsonMeta('')
        join_meta.data={**self.data,**other.data}
        return join_meta
    def split_to_shmeta(self):
        metali=[]
        for path in self.data.keys():
            sht_info_li=self.data[path]
            for sht_info in sht_info_li:
                metali.append(
                    JsonMeta({
                        path:[
                            [sht_info[0],sht_info[1]]
                        ]
                    })
                )
        return metali
    def check_split(self)->bool:
        metali=self.split_to_shmeta()
        sheet_count=0
        for path in self.data.keys():
            sheet_count+=len(
                self.data[path]
            )
            continue
        return len(metali)==sheet_count
    def save(self,savepath):
        from json import dumps
        with open(savepath,'w',encoding='utf-8') as f:
            f.write(
                dumps(
                    self.data,
                    ensure_ascii=False,
                    indent=4
                )
            )
        print(
            '[Note]{} data saved to: {}'.format(
                self.__class__.__name__,
                savepath
            )
        )
        pass
    pass
class PathMeta(JsonMeta):
    def __init__(
            self,
            xlpath,
            shtli=[],
            common_title=0,
            keep_additional=True
    ):
        self.keep_additional=keep_additional
        self.path=xlpath
        self.data={}
        if isfile(xlpath):
            ## `xlpath` is a file, load all sheets of it;
            ## or part of them, according to `shtli`;
            from autk.brother.xlbk import XlBook
            bk=XlBook(self.path)
            if shtli==[]:
                self.data.update({bk.file_path:[[sht,common_title] for sht in bk.shtli]})
            else:
                self.data.update({bk.file_path:[[sht,common_title] for sht in shtli]})
            pass
        elif isdir(xlpath):
            ## `xlpath` is a directory, load all sheets in all files;
            ## only when `shtli`=['SomeSheet'], load all the sheets with
            ## the same name from all files in that directory;
            from os import listdir
            from os.path import join as pjoin
            from autk.brother.xlbk import XlBook
            if len(shtli)==1:
                # laod all sheets with the same name
                # from all books in the directory.
                for p in listdir(xlpath):
                    fp=pjoin(xlpath,p)
                    bk=XlBook(fp)
                    self.data.update({
                        bk.file_path:[
                            [shtli[0],common_title]
                        ]
                    })
                    continue
            elif len(shtli)==0:
                # load all sheets
                # from all books in the directory.
                collect={}
                def __get_shtli(bk):
                    collect.update({
                        bk.file_path:[
                            [sht,common_title] for sht in bk.shtli
                        ]
                    })
                    pass
                thli=[]
                for p in listdir(xlpath):
                    fp=pjoin(xlpath,p)
                    bk=XlBook(fp)
                    thli.append(
                        Thread(
                            target=__get_shtli,
                            args=(bk,)
                        )
                    )
                    continue
                start_thread_list(thli)
                self.data.update(collect)
                pass
            else:
                # load specific sheets
                # from all books in the directory.
                for p in listdir(xlpath):
                    fp=pjoin(xlpath,p)
                    bk=XlBook(fp)
                    self.data.update({
                        bk.file_path:[
                            [sht,common_title] for sht in shtli
                        ]
                    })
                    continue
            pass
        else: 
            ## xlpath is neither a path nor a directory;
            print("[Error:] `xlpath` is neither a path nor a directory!")
        pass
    pass
class DirMeta(JsonMeta):
    def __init__(self,basedir,common_title=0):
        from autk.brother.xlbk import XlBook
        self.keep_additional=True
        self.path='json'
        self.data={}
        fli=find_regex(r'\.xlsx?$',search_dir=basedir,match=False)[0]
        for f in fli:
            self.data.update(
                {f:[[shtna,common_title] for shtna in XlBook(f).shtli]}
            )
        pass
    pass
if __name__=='__main__':
    pass
