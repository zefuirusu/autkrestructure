#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os ## os.system('ls -hl')
import re
from threading import Thread
def pure_fname(path):
    return re.sub(
        '\.pdf$',
        '',
        path.split(os.sep)[-1]
    )
def split_type(pure_fname):
    item=re.search(r"合并",pure_fname)
    if item is None:
        return ['1-6','7']
    else:
        return ['1-12','13']
def split_cmd(path,fs=True,outdir='.'):
    if fs==True:
        resu_name=os.path.join(
            outdir,
            pure_fname(path)+'-fs.pdf'
        )
        cmd=' '.join([
            'pdftk',
            path,
            'cat',
            split_type(pure_fname(path))[0],
            #  split_type(pure_fname(path))[1],
            'output',
            resu_name
        ])
    else:
        resu_name=os.path.join(
            outdir,
            pure_fname(path)+'-note.pdf'
        )
        cmd=' '.join([
            'pdftk',
            path,
            'cat',
            #  split_type(pure_fname(path))[0],
            split_type(pure_fname(path))[1],
            'output',
            resu_name
        ])
    return cmd
def test(pdf_dir,outdir):
    for f in os.listdir(pdf_dir):
        path=os.path.join(pdf_dir,f)
        print('-'*12)
        print(split_cmd(path,fs=False,outdir=outdir))
        print(split_cmd(path,fs=True,outdir=outdir))
        continue
    pass
def multi_split(pdf_dir,outdir):
    '''
    This function requires `pdftk`.
    Run `sudo pacman -S pdftk` to install `pdftk`;
    see:
        https://www.pdflabs.com/tools/pdftk-the-pdf-toolkit/;
        https://gitlab.com/pdftk-java/pdftk;
    '''
    thread_list=[]
    for f in os.listdir(pdf_dir):
        path=os.path.join(pdf_dir,f)
        thread_list.append(
            Thread(
                target=os.system,
                args=(split_cmd(
                    path,
                    fs=True,
                    outdir=outdir),),
                name=pure_fname(path)+'_fs'
            )
        )
        thread_list.append(
            Thread(
                target=os.system,
                args=(split_cmd(
                    path,
                    fs=False,
                    outdir=outdir),),
                name=pure_fname(path)+'_note'
            )
        )
        continue
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()
    pass
if __name__=='__main__':
    pass
