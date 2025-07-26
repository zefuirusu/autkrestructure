#!/usr/bin/env python
# -*- coding: utf-8 -*-


from autk.gentk.quick import gl_from_json
from autk.brother.xlbk import XlBook
from autk.meta.handf.findfile import file_link

# if subcmd` is {}, then `args` cannot be []; if `args` is [], `subcmd` cannot be{};

def yesno(yn_str):
    if yn_str=='yes' or yn_str=='y':
        return True
    elif yn_str=='no' or yn_str=='n':
        return False
    else:
        print('check argument:yes/no')
        return True
def __show_shtli(args):
    shtli=[]
    for sht in XlBook(args.ifp).shtli:
        print(sht)
        shtli.append(sht)
        continue
    return shtli
def __table_df(args):
    '''
        This function returns Table, not DataFrame
    '''
    from autk.gentk.funcs import f2dict
    from autk.calculation.base.table import ImmortalTable
    from autk.mapper.base import XlMap
    from autk.meta.pmeta import JsonMeta
    t=ImmortalTable(
        xlmap=XlMap.from_dict(f2dict(args.map)) if args.map is not None else None,
        xlmeta=JsonMeta(f2dict(args.meta)) if args.meta is not None else None,
    )
    t.load_raw_data()
    # for xl in t.xlset:
    #     print(xl)
    print(t)
    print(t.data)
    if args.save is not None:
        from autk.gentk.funcs import save_df
        save_df(t.data,'data',args.save)
    else:
        pass
    return t
def __table_search(args):
    pass
def __table_call(args):
    pass
def __mgl_df(args):
    mgl=gl_from_json(args.config)
    mgl.load_raw_data()
    print(mgl.data)
    return mgl
def __mgl_search(args):
    from pandas import DataFrame
    mgl=gl_from_json(args.config)
    print('get MGL:',mgl)
    resu=mgl.search(
        args.regex,
        args.col,
        type_xl=False
    )
    if yesno(args.ifdf)==True:
        print(resu)
        #  for r in resu.iterrows():
            #  print(DataFrame(r[1]).T)
    elif yesno(args.ifdf)==False:
        resu=resu.values
        for r in resu:
            print(r)
    else:
        print("check argument:--ifdf")
    return resu

CMD=[
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
                    ("meta",{"type":str,"help":"the JSON path for meta data."}),
                    ("save",{"type":str,"help":"save path."}),
                    ("--source",{"type":str,"help":"column to indicate the source file."}),
                ],
                "func":None,
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
                "func":__show_shtli,
                "subcmd":[],
            },
            {
                "name":"sht",
                "help":"get the table of a sheet.",
                "args":[
                    ("shtna",{"type":str,"help":"sheet name."}),
                    ("ifp",{"type":str,"help":"Input File Path"}),
                    ("--ifdf",{"type":str,"default":"yes","help":"if is shown as DataFrame"}),
                ],
                "func":lambda args:print(XlBook(args.ifp).select_all(args.shtna,yesno(args.ifdf))),
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
                    ("--ifdf",{"type":str,"default":"no","help":"if DataFrame format is needed."}),
                    ("--hastitle",{"type":str,"default":"yes","help":"if assign top row as title of DataFrame."}),
                ],
                "func":lambda args:
                    print(
                          XlBook(args.ifp).select_matrix(
                             args.shtna,
                             tuple(args.start),
                             tuple(args.end),
                             yesno(args.ifdf),
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
                "func":__table_df,
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
                "func":__table_search,
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
                "func":__table_call,
                "subcmd":[]
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
                "help":"TODO: show DataFrame of MGL.",
                "args":[
                    ("config",{"type":str,"help":"Json path of the configuration file."}),
                    ("--save",{"type":str,"default":None,"help":"save path for the output DataFrame."}),
                ],
                "func":__mgl_df,
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
                    ("--ifdf",{"type":str,"default":"yes","help":""}),
                ],
                "func":__mgl_search,
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
