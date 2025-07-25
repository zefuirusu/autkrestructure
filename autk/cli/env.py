#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.gentk.start import startprj,PRJ_JSON

def get_script_dir(home_dir):
    '''
    locate and return the directory of `script`;
    '''
    pli=[]
    for sub in home_dir['subdirs']:
        pli.append(sub['name'])
        if 'script' in [subsub['name'] for subsub in sub['subdirs']]:
            pli.append('script')
            return '/'.join(pli)
        else:
            get_script_dir(sub)
def config_xlmeta(args):
    from autk.brother.xlbk import XlBook
    from autk.meta.pmeta import DirMeta
    ## user-input title is 1-based index,
    ## while the `title` to create XlSheet is 0-based index;
    DirMeta(args.base,common_title=args.title-1).save(args.save)
    pass
def config_xlmap(args):
    '''
        This function has not been finished yet.
    '''
    if args.type=='list':
        # TODO
        from autk.mapper.base import XlMap
        XlMap.from_list(args.col).save(args.save)
        pass
    elif args.type=='dict':
        pass
    else:
        print("check arg:{}".format(args.type))
    pass

PRJ_CONFIG=[
    {
        "name":"config",
        "help":"project configuration.",
        "args":[],
        "func":None,
        "subcmd":[
            {
                "name":"new",
                "help":"create new project at target directory.",
                "args":[
                    ("--name",{"type":str,"help":"create new project in current directory."}),
                    ("--home",{"type":str,"default":".","help":"home directory to place your project, default to current directory."}),
                ],
                "func":lambda args:startprj(args.name,args.home),
                "subcmd":[],
            },
            {
                "name":"meta",
                "help":"save shape info of Excel file into JSON.",
                "args":[
                    ("base",{"type":str,"help":"directory to search Excel file."}),
                    ("save",{"type":str,"help":"save path for the JSON output."}),
                    ("--title",{"type":int,"help":"common title for those Excel files"}),
                ],
                "func":config_xlmeta,
                "subcmd":[],
            },
            {
                "name":"map",
                "help":"save map info of Excel file into JSON.",
                "args":[
                    ("--type",{"type":str,"help":"possible values:{list|dict}"}),
                    ("--col",{"type":list,"help":"columns to generate map."}),
                    ("--save",{"type":str,"help":"save path for the JSON output."}),
                ],
                "func":config_xlmap,
                "subcmd":[],
            },
        ],
    },
]
if __name__=='__main__':
  # print(get_script_dir(PRJ_JSON))
  pass
