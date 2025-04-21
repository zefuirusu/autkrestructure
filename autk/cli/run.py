#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.cli.cmd import CMD_CONFIG
from argparse import ArgumentParser
top_parser=ArgumentParser(description="Auditors' Toolkit",prog="autk")
subparsers=top_parser.add_subparsers(help="subcommand for autk")

def __set_parser(base_parser,cmd):
    #  print('setting:',cmd['name'])
    current_lv_parser=base_parser.add_parser(cmd['name'],help=cmd['help'])
    if len(cmd['args']) >0:
        for arg in cmd['args']:
            current_lv_parser.add_argument(arg[0],**arg[1])
            continue
        current_lv_parser.set_defaults(func=cmd['func'])
    if len(cmd['subcmd']) >0:
        lower_parser=current_lv_parser.add_subparsers(help='details for ...')
        for subcmd in cmd['subcmd']:
            __set_parser(lower_parser,subcmd)
            continue
    #  print('ok:',cmd['name'])
    pass
for cmd in CMD_CONFIG:
    __set_parser(subparsers,cmd)
    continue

#  for lv1_cmd_name,lv1_cmd in CMD_CONFIG.items():
    #  lv1_parser=top_parser.add_subparsers(help=lv1_cmd['help'])
    #  if len(lv1_cmd['args']) !=0:
        #  for arg in lv1_cmd['args']:
            #  lv1_parser.add_argument(arg[0],**arg[1])
            #  continue
        #  lv1_parser.set_defaults(func=lv1_cmd['func'])
    #  if lv1_cmd['subcmd'] !={}:
        #  for lv2_cmd_name,lv2_cmd in lv1_cmd.items():
            #  lv2_parser=lv1_parser.add_subparsers(help=lv2_cmd['help'])
            #  if len(lv2_cmd['args']) !=0:
                #  for arg in lv2_cmd['args']:
                    #  lv2_parser.add_argument(arg[0],**arg[1])
                    #  continue
                #  lv2_parser.set_defaults(func=lv2_cmd['func'])
            #  continue
            #  if lv2_cmd['subcmd'] !={}:
                #  pass
                #  continue
            #  continue

#  xlsch_parser=subparsers.add_parser("xlsch",help="Search item from Excel file.")
#  xlsch_parser.add_argument("--regex",type=str,help="regular expression, string to search")
#  xlsch_parser.add_argument("--ifp",type=str,help="Input File Path")
#  xlsch_parser.set_defaults(func=lambda args:print(XlBook(args.ifp).search(args.regex)))
#
#  df_p=subparsers.add_parser("dfshow",help="Show DataFrame of a selection.")
#  df_p.add_argument("--shtna",type=str,help="")
#  df_p.add_argument("--start",type=set,help="Index of the start cell.")
#  df_p.add_argument("--end",type=set,help="Index of the end cell.")
#  df_p.add_argument("--ifp",type=str,help="")
#  df_p.set_defaults(func=lambda args:XlBook(args.ifp).select_matrix(args.shtna,args.start,args.end,True,True))
#
#  shtli_p=subparsers.add_parser("shtli",help="")
#
#  fl_p=subparsers.add_parser("flink",help="Generate file link, and save to Excel.")
#  fl_p.add_argument('--topic',type=str,help='sheet name')
#  fl_p.add_argument('--regex',type=str,help='regular expression')
#  fl_p.add_argument('--sdir',type=str,help='search directory')
#  fl_p.add_argument('--ofp',type=str,help='Output File Path')
#  fl_p.add_argument('--type',type=str,help='type of result,can be `file`, `dir` or `flatten`;')
#  fl_p.set_defaults(func=lambda args:file_link(args.topic,args.regex,args.sdir,args.ofp,args.type))

## get acct df
## autk mgl acct --accid [accid]  --ofp [output path] --config [json_path]
## autk mgl search --regex [regex] --config [json_path]


if __name__=='__main__':
    args=top_parser.parse_args()
    args.func(args)
    pass
