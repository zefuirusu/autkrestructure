#!/usr/bin/env python
# -*- coding: utf-8 -*-


from autk.gentk.start import startprj
from autk.brother.xlbk import XlBook
from autk.meta.handf.findfile import file_link

# if subcmd` is {}, then `args` cannot be []; if `args` is [], `subcmd` cannot be{};
CMD=[
    {
        "name":"new",
        "help":"create new project.",
        "args":[
            ("--name",{"type":str,"help":"create new project in current directory."}),
            ("--home",{"type":str,"default":".","help":"home directory to place your project, default to current directory."}),
        ],
        "func":lambda args:startprj(args.name,args.home),
        "subcmd":[],
    },
    { # lv1_cmd
        "name":"show",
        "help":"show some info.",
        "args":[],
        "func":None,
        "subcmd":[# lv2_cmd
            {
                "name":"shape",
                "help":"get the shape of an Excel Workbook.",
                "args":[
                    ("ifp",{"type":str,"help":"Input File Path"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).shape_df),
                "subcmd":[],
            },
            {
                "name":"shtli",
                "help":"list all sheets of the Excel Workbook.",
                "args":[
                    ("ifp",{"type":str,"help":"Input File Path"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).shtli),
                "subcmd":[],
            },
            {
                "name":"sht",
                "help":"get the table of a sheet.",
                "args":[
                    ("shtna",{"type":str,"help":"sheet name."}),
                    ("ifp",{"type":str,"help":"Input File Path"}),
                    ("--ifdf",{"type":bool,"default":True,"help":"if is shown as DataFrame"}),
                ],
                "func":lambda
                args:print(XlBook(args.ifp).select_all(args.shtna,bool(args.ifdf))),
                "subcmd":[],
            },
            {
                "name":"matrix",
                "help":"get matrix of a selection.",
                "args":[
                    ("--start",{"type":int,"nargs":2,"help":"start index"}),
                    ("--end",{"type":int,"nargs":2,"help":"end index"}),
                    ("--shtna",{"type":str,"help":"sheet name."}),
                    ("--ifp",{"type":str,"help":"Input File Path"}),
                    ("--ifdf",{"type":bool,"default":True,"help":"if DataFrame format is needed."}),
                    ("--hastitle",{"type":bool,"default":True,"help":"if assign top row as title of DataFrame."}),
                ],
                "func":lambda args:print(XlBook(args.ifp).select_matrix(args.shtna,tuple(args.start),tuple(args.end),args.ifdf,args.hastitle)),
                "subcmd":[],
            },
            {
                "name":"value",
                "help":"get value of a spacific cell.",
                "args":[
                    ("--index",{"type":int,"nargs":2,"help":"index of the cell."}),
                    ("--shtna",{"type":str,"help":"sheet name."}),
                    ("--ifp",{"type":str,"help":"Input File Path"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).get_value(args.shtna,tuple(args.index))),
                "subcmd":[],
            },
            { 
                "name":"xlsch",
                "help":"search item from Excel file.",
                "args":[
                    ("--regex",{"type":str,"help":"Regular Expression"}),
                    ("--ifp",{"type":str,"help":"Input File Path"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).search(args.regex)),
                "subcmd":[],
            },
        ],
    },
    {
        "name":"analysis",
        "help":"Analysis,TODO.",
        "args":[],
        "func":None,
        "subcmd":[# lv2_cmd
            {
                "name":"side",
                "help":"side split",
                "args":[
                    ("--config",{"type":str,"help":""}),
                ],
                "func":None,
                "subcmd":[]
            },
        ],
    },
]
