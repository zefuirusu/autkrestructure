#!/usr/bin/env python
# -*- coding: utf-8 -*-


from autk.gentk.start import startprj
from autk.brother.xlbk import XlBook
from autk.meta.handf.findfile import file_link

# if subcmd` is {}, then `args` cannot be []; if `args` is [], `subcmd` cannot be{};
CMD_CONFIG=[
    {
        "name":"new",
        "help":"create new project.",
        "args":[
            ("--name",{"type":str,"help":"create new project in current directory."}),
        ],
        "func":lambda args:startprj(args.name),
        "subcmd":[],
    },
    { # lv1_cmd
        "name":"show",
        "help":"show some info.",
        "args":[],
        "func":None,
        "subcmd":[# lv2_cmd
            { 
                "name":"xlsch",
                "help":"search item from Excel file.",
                "args":[
                    ("--regex",{"type":str,"help":""}),
                    ("--ifp",{"type":str,"help":""}),
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
