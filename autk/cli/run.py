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

if __name__=='__main__':
    args=top_parser.parse_args()
    args.func(args)
    pass
