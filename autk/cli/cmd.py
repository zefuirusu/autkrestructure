#!/usr/bin/env python
# -*- coding: utf-8 -*-

# NOTE: If subcmd` is {}, then `args` cannot be []; and if `args` is [], `subcmd` cannot be{};

from userfunc import *

from autk.gentk.start import startprj
from autk.gentk.quick import gl_from_json
from autk.brother.xlbk import XlBook
from autk.meta.handf.findfile import file_link

CMD=[
    { # lv1 cmd
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
            {
                "name":"mgl",
                "help":"save MGL config info into JSON.",
                "args":[
                    ("--keyna",{"type":str,"help":""}),
                    ("--keyidx",{"type":str,"help":""}),
                    ("--drcr",{"type":str,"help":""}),
                    ("--accid-col",{"type":str,"help":""}),
                    ("--accna-col",{"type":str,"help":""}),
                    ("--top-accid",{"type":str,"help":""}),
                    ("--top-accna",{"type":str,"help":""}),
                    ("--date-col",{"type":str,"help":""}),
                    ("--top-accid-len",{"type":int,"help":""}),
                    ("--accna-split-by",{"type":str,"help":""}),
                    ("--date-split-by",{"type":str,"help":""}),
                ],
                "func":config_gl,
                "subcmd":[],
            },
        ],
    },
    {# lv1 cmd
        "name":"ftk",
        "help":"File-handle Toolkit.",
        "args":[],
        "func":None,
        "subcmd":[
            {
                "name":"flink",
                "help":"generage file link.",
                "args":[
                    ("topic",{"type":str,"help":"sheet name in the output file."}),
                    ("regex",{"type":str,"help":"Regular Expression to match file names."}),
                    ("ofp",{"type":str,"help":"Output File Path"}),
                    ("--depth",{"type":int,"default":0,"help":"show the structure of results, default to show all(depth=0)."}),
                    ("--sdir",{"type":str,"default":".","help":"where to search/match, default to current directory."}),
                    ("--type",{"type":str,"default":"flatten","help":"file|dir|flatten;default to `flatten`."}),
                    ("--towin",{"type":str,"default":None,"nargs":2,"help":"transform `linux` path into `windows` path."}),
                ],
                "func":lambda args:
                    file_link(
                        args.topic,
                        args.regex,
                        args.sdir,
                        args.ofp,
                        depth=args.depth,
                        resu_type=args.type,
                        trans2win=args.towin
                    ),
                "subcmd":[]
            },
            {
                "name":"joinxl",
                "help":"join Excel file into ONE.",
                "args":[
                    ("--meta",{"nargs":"+","action":"append","help":"input <path,sheet,title_row,bottom_row,start_col,end_col>, which indicates the structure of files to join."}),
                    ("--save",{"nargs":2,"help":"input<path,sheet>, save the joined data."}),
                ],
                "func":joinxl,
                "subcmd":[],
            },
        ]
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
                "func":show_shtli,
                "subcmd":[],
            },
            {
                "name":"sht",
                "help":"get the table of a sheet.",
                "args":[
                    ("shtna",{"type":str,"help":"sheet name."}),
                    ("ifp",{"type":str,"help":"Input File Path"}),
                    ("--dftype",{"type":str,"default":"yes","help":"if is shown as DataFrame"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).select_all(args.shtna,yesno(args.dftype))),
                "subcmd":[],
            },
            {
                "name":"row",
                "help":"get row data",
                "args":[
                    ("n",{"type":int,"help":"row number."}),
                    ("shtna",{"type":str,"help":"sheet name."}),
                    ("ifp",{"type":str,"help":"Input File Path"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).get_row(args.shtna,args.n)),
                "subcmd":[]
            },
            {
                "name":"col",
                "help":"get column data",
                "args":[
                    ("n",{"type":int,"help":"column number."}),
                    ("shtna",{"type":str,"help":"sheet name."}),
                    ("ifp",{"type":str,"help":"Input File Path"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).get_col(args.shtna,args.n)),
                "subcmd":[]
            },
            {
                "name":"matrix",
                "help":"get matrix of a selection.",
                "args":[
                    ("--start",{"type":int,"nargs":2,"help":"start index"}),
                    ("--end",{"type":int,"nargs":2,"help":"end index"}),
                    ("--shtna",{"type":str,"help":"sheet name."}),
                    ("--ifp",{"type":str,"help":"Input File Path"}),
                    ("--dftype",{"type":str,"default":"no","help":"if DataFrame format is needed."}),
                    ("--hastitle",{"type":str,"default":"yes","help":"if assign top row as title of DataFrame."}),
                ],
                "func":lambda args:
                    print(
                          XlBook(args.ifp).select_matrix(
                             args.shtna,
                             tuple(args.start),
                             tuple(args.end),
                             yesno(args.dftype),
                             yesno(args.hastitle)
                         )
                    ),
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
        "name":"table",
        "help":"Analysis, through Immortal Table.",
        "args":[],
        "func":None,
        "subcmd":[
            {
                "name":"df",
                "help":"show DataFrame of the Table.",
                "args":[
                    ("--meta",{"type":str,"default":None,"help":"Json path of `meta` info for ImmortalTable."}),
                    ("--map",{"type":str,"default":None,"help":"Json path of `map` info for ImmortalTable."}),                   
                    ("--save",{"type":str,"default":None,"help":"save path for the output DataFrame."}),
                ],
                "func":table_df,
                "subcmd":[],
            },
            {
                "name":"search",
                "help":"search by keywords and get rows",
                "args":[
                    ("regex",{"type":str,"help":"Regular Expression."}),
                    ("col",{"type":str,"help":"column to apply the regex."}),
                    ("--config",{"type":dict,"default":None,"nargs":1,"help":"meta"}),
                    ("--ifp",{"type":str,"help":"path of the json config file for MGL"}),
                ],
                "func":table_search,
                "subcmd":[]
            },
            {
                "name":"call",
                "help":"call a function",
                "args":[
                    ("func_name",{"type":str,"help":"function of table."}),
                    ("func_args",{"nargs":"*","help":"arguments passed to the function."}),
                    ("--ifp",{}),
                ],
                "func":table_call,
                "subcmd":[]
            },
            {
                "name":"pca",
                "help":"perform Principal Component Analysis.",
                "args":[
                    ("--save",{"type":str,"help":"save path for result."}),
                    ("--ifp",{"type":str,"help":"Input File Path."}),
                    ("--shtna",{"type":str,"help":"Sheet Name."}),
                    ("--start",{"type":int,"nargs":2,"help":"start cell index"}),
                    ("--end",{"type":int,"nargs":2,"help":"end cell index"}),
                    ("--title",{"type":bool,"default":True,"help":"whether the input data has a title row; default True"}),
                    ("--ifstd",{"type":bool,"default":True,"help":"whether perform standardizing process; default True"}),
                    ("--method",{"type":str,"default":"average","help":"method for clustering, default to `average`;"}),
                    ("--metric",{"type":str,"default":"minkowski","help":"metric when calculating distance, default to `minkowski`;"}),
                ],
                "func":pca_analysis,
                "subcmd":[],
                "description":"To perform Principal Compoent Analysis.\nmethod for clustering:\n\taverage(default)：平均距离法（UPGMA算法）\n\tsingle：最短距离法（近邻点算法）\n\tcomplete：最长距离法（远邻点算法）\n\tcentroid：质心距离法（UPGMC算法）\n\tward：离差平方和法（Ward方差最小化算法）;\nmetric for distance:\n\tminkowski(defualt):明可夫斯基距离\n\teuclidean:欧氏距离\n\tmanhattan:曼哈顿距离\n\tcosine:余弦距离\n\tmahalanobis:马哈拉诺比斯距离;"
            },
        ]
    },
    {
        "name":"mgl",
        "help":"Analysis, through Mortal General Ledgers,TODO.",
        "args":[
        ],
        "subcmd":[# lv2_cmd
            {
                "name":"df",
                "help":"show DataFrame of MGL.",
                "args":[
                    ("config",{"type":str,"help":"Json path of the configuration file."}),
                    ("--save",{"type":str,"default":None,"help":"save path for the output DataFrame."}),
                ],
                "func":mgl_df,
                "subcmd":[],
            },
            {
                "name":"showcols",
                "help":"show all columns of the current mgl.",
                "args":[
                    ("--config",{"type":str,"help":"path of the json config file for MGL"}),
                ],
                "func":lambda args:print(gl_from_json(args.config).xlmap.columns),
                "subcmd":[],
            },
            {
                "name":"search",
                "help":"search by keywords and get rows",
                "args":[
                    ("regex",{"type":str,"help":"Regular Expression."}),
                    ("col",{"type":str,"help":"column to apply the regex."}),
                    ("--config",{"type":str,"help":"path of the json config file for MGL"}),
                    ("--dftype",{"type":str,"default":"yes","help":""}),
                ],
                "func":mgl_search,
                "subcmd":[]
            },
            {
                "name":"call",
                "help":"call a function of mgl.",
                "args":[
                    ("func_name",{"type":str,"help":"function of mgl."}),
                    ("func_args",{"nargs":"*","help":"arguments passed to the function."}),
                    ("--config",{"type":str,"help":"path of the json config file for MGL"}),
                ],
                "func":lambda
                    args:print(
                        getattr(
                            gl_from_json(args.config),
                            args.func_name
                        )(*args.func_args)
                    ),
                "subcmd":[]
            },
        ],
    },
]
