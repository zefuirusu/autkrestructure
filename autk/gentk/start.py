#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from json import load,dump

PRJ_JSON={
    "name":"newProject",
    "subdirs":[
        {
            "name":"100-push",
            "subdirs":[
                {"name":"1000-控制测试","subdirs":[]},
                {"name":"3000-盘点","subdirs":[]},
                {"name":"4000-询证函","subdirs":[
                    {"name":"100-函证抽样","subdirs":[]},
                    {"name":"200-核对与反馈","subdirs":[]},
                    {"name":"300-生成待盖章","subdirs":[]},
                    {"name":"400-回函及解释","subdirs":[]}
                ]},
                {"name":"5000-拍凭证","subdirs":[]},
                {"name":"7000-报告和附注","subdirs":[
                    {"name":"关联往来及交易","subdirs":[]}
                ]},
                {"name":"8000-归档","subdirs":[]},
                {"name":"9000-其他","subdirs":[]},
                {"name":"script",
                    "subdirs":[
                        {"name":"src","subdirs":[]},
                        {"name":"data","subdirs":[]},
                        {"name":"output","subdirs":[]},
                        {"name":"config","subdirs":[]},
                        {"name":"logfiles","subdirs":[]}
                ]}
            ]
        },
        {
            "name":"200-collection",
            "subdirs":[
                {"name":"100-综合资料","subdirs":[
                    {"name":"账套数据","subdirs":[]},
                    {"name":"未审报表","subdirs":[]}
                ]},
                {"name":"200-本部资料","subdirs":[]},
                {"name":"850-其他主体","subdirs":[]},
                {"name":"900-zipbackup","subdirs":[]},
                {"name":"glandtb","subdirs":[]}
            ]
        },
        {"name":"300-historical","subdirs":[]},
        {"name":"700-notes","subdirs":[]},
        {"name":"800-expense","subdirs":[]},
        {"name":"900-others","subdirs":[]}
    ]
}
def process_dict(cur_path,prj_json):
    cur_name=prj_json['name']
    cur_path=os.path.join(cur_path,cur_name)
    sub_dirs=prj_json['subdirs']
    os.mkdir(cur_path)
    print('mkdir:',cur_path)
    if len(sub_dirs)>0:
        for sub_prj_json in sub_dirs:
            process_dict(cur_path,sub_prj_json)
    else:
        pass
    return sub_dirs
def start_by_conf(
        json_file_path,
        home_path=os.path.abspath(os.curdir)):
    '''
    Configuration file must be json;
    '''
    with open(json_file_path) as f:
        prj_home_json=load(f)
    process_dict(home_path,prj_home_json)
    pass
def startprj(
        prj_name,
        home_path=os.path.abspath(os.curdir)):
    from copy import deepcopy
    prj_home_json=deepcopy(PRJ_JSON)
    prj_home_json['name']=prj_name
    process_dict(home_path,prj_home_json)
    pass
if __name__=='__main__':
    pass
