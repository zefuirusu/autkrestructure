#!/usr/bin/env python
#

import re
import os
from threading import Thread,Lock
from docx import Document

from autk.gentk.funcs import regex_filter,start_thread_list
from autk.meta.handf.findfile import find_regex

class WordSum:
    def __init__(
        self,
        path:str,
    ):
        self.path=path,
        self.title=self.path.split(os.sep)[-1],
        self.matched_paragraphs=[]
        pass
    def single_search(self,item):
        for matched_paragraph in regex_filter(
            item,
            map(
                lambda p:p.text,
                Document(self.path).paragraphs
            )
        ):
            if matched_paragraph is not None:
                self.matched_paragraph.append(
                    matched_paragraph
                )
        pass
    def save(path):
        if len(self.matched_paragraphs)==0:
            return
        else:
            target_docx=Document(path)
            target_docx.add_heading(
                'Summary from:{}'.format(
                    self.title.join(['《','》'])
                ),
                level=2
            )
            target_docx.add_heading(
                'source:{}'.format(self.path),
                level=3
            )
            for p in self.matched_paragraphs:
                target_docx.add_paragraph(self.p)
        pass
    pass
def docxSum(item:str,sdir:str,save_path:str):
    file_list=find_regex(
        r'^[^\~]\.docx$',
        sdir,
    )[0]
    def __single_search(docx_path):
        ws=WordSum(docx_path)
        ws.single_search(item)
        docx_search_lock=Lock()
        docx_search_lock.acquire()
        ws.save(save_path)
        docx_search_lock.release()
        pass
    thli=[]
    for docx_path in file_list:
        thli.append(
            Thread(
                target=__single_search,
                args=(docx_path),
                name=self.__class__.__name__+'-'+__single_search.__name__+p
            )
        )
        continue
    start_thread_list(thli)
    pass
if __name__=='__main__':
    pass
