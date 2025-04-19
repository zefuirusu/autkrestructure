#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
top_parser=ArgumentParser(description="AUTK",prog="autk")
subparsers=top_parser.add_subparsers(help="subcommand")

from autk.brother.xlbk import XlBook
xlsch_parser=subparsers.add_parser("xlsch",help="Search item from Excel file.")
xlsch_parser.add_argument("--regex",type=str,help="regular expression, string to search")
xlsch_parser.add_argument("--ifp",type=str,help="Input File Path")
xlsch_parser.set_defaults(func=lambda args:print(XlBook(args.ifp).search(args.regex)))

from autk.meta.handf.findfile import file_link
fl_p=subparsers.add_parser("flink",help="Generate file link, and save to Excel.")
fl_p.add_argument('--topic',type=str,help='sheet name')
fl_p.add_argument('--regex',type=str,help='regular expression')
fl_p.add_argument('--sdir',type=str,help='search directory')
fl_p.add_argument('--ofp',type=str,help='Output File Path')
fl_p.add_argument('--type',type=str,help='type of result,can be `file`, `dir` or `flatten`;')
fl_p.set_defaults(func=lambda args:file_link(args.topic,args.regex,args.sdir,args.ofp,args.type))

## get acct df
## autk mgl acct --accid [accid]  --ofp [output path] --config [json_path]
## autk mgl search --regex [regex] --config [json_path]


if __name__=='__main__':
    args=top_parser.parse_args()
    args.func(args)
    pass
