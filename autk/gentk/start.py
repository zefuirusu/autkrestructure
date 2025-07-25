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
                {"name":"4000-tofrom","subdirs":[]},
                {"name":"5100-盘点","subdirs":[]},
                {"name":"5200-询证函","subdirs":[
                    {"name":"100-函证抽样","subdirs":[]},
                    {"name":"200-核对与反馈","subdirs":[]},
                    {"name":"300-生成待盖章","subdirs":[]},
                    {"name":"400-回函解释","subdirs":[]},
                    {"name":"500-回函统计","subdirs":[]},
                ]},
                {"name":"5300-凭证附件","subdirs":[]},
                {"name":"6000-提交质控与底稿汇总","subdirs":[]},
                {"name":"7000-附注与报告撰稿","subdirs":[
                    {"name":"关联往来及交易","subdirs":[]}
                ]},
                {"name":"8100-出具报告","subdirs":[
                    {"name":"报表与附注落款页","subdirs":[]},
                    {"name":"签字页","subdirs":[]},
                    {"name":"排版校对下载","subdirs":[]},
                    {"name":"赋码报告扫描件","subdirs":[]}
                ]},
                {"name":"8200-资料归档","subdirs":[
                    {"name":"管理层声明与报告确认函","subdirs":[]},
                    {"name":"归档索引","subdirs":[]},
                    {"name":"业务流转单","subdirs":[]}
                ]},
                {"name":"script",
                    "subdirs":[
                        {"name":"src","subdirs":[]},
                        {"name":"data","subdirs":[]},
                        {"name":"output","subdirs":[]},
                        {"name":"config","subdirs":[]},
                        {"name":"logfiles","subdirs":[]},
                        {"name":"download","subdirs":[]},
                ]}
            ]
        },
        {
            "name":"200-collection",
            "subdirs":[
                {"name":"100-综合资料","subdirs":[
                    {"name":"账套数据","subdirs":[]},
                ]},
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
def __process_dict(cur_path,prj_json):
    cur_name=prj_json['name']
    cur_path=os.path.join(cur_path,cur_name)
    sub_dirs=prj_json['subdirs']
    os.mkdir(cur_path)
    print('mkdir:',cur_path)
    if len(sub_dirs)>0:
        for sub_prj_json in sub_dirs:
            __process_dict(cur_path,sub_prj_json)
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
    __process_dict(home_path,prj_home_json)
    pass
def startprj(
    prj_name,
    home_path=os.path.abspath(os.curdir)
):
    from copy import deepcopy
    prj_home_json=deepcopy(PRJ_JSON)
    prj_home_json['name']=prj_name
    __process_dict(home_path,prj_home_json)
    pass
if __name__=='__main__':
    pass
