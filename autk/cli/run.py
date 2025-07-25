#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from autk.cli.env import PRJ_CONFIG
from autk.cli.cmd import CMD

'''
see:https://docs.python.org/3/library/argparse.html#the-namespace-object
'''

TOP_PARSER=ArgumentParser(description="Auditors' Toolkit; version 4.0.1",prog="autk")
SUBPARSERS=TOP_PARSER.add_subparsers(help="subcommand for autk")

def __set_parser(base_parser,cmd):
    current_lv_parser=base_parser.add_parser(cmd['name'],help=cmd['help'])
    if len(cmd['args']) >0:
        for arg in cmd['args']:
            current_lv_parser.add_argument(arg[0],**arg[1])
            continue
        current_lv_parser.set_defaults(func=cmd['func'])
    if len(cmd['subcmd']) >0:
        lower_parser=current_lv_parser.add_subparsers(help='')
        for subcmd in cmd['subcmd']:
            __set_parser(lower_parser,subcmd)
            continue
    pass
def get_cmd(cmd_json):
    for cmd in cmd_json:
        __set_parser(SUBPARSERS,cmd)
        continue
    return

get_cmd(PRJ_CONFIG)
get_cmd(CMD)

if __name__=='__main__':
    USER_INPUT_ARGS=TOP_PARSER.parse_args()
    USER_INPUT_ARGS.func(USER_INPUT_ARGS)
    pass
