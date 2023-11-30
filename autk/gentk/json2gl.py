#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.gentk.funcs import f2dict
from autk.mapper.glmap import MglMap
from autk.meta.pmeta import JsonMeta
from autk.calculation.mortal.mortalgl import MGL


def gl_from_json(json_path):
    gl_conf=f2dict(json_path)
    map_conf=gl_conf['map_conf']
    meta_conf=gl_conf['meta_conf']
    cols=map_conf['cols']
    class InstMap(MglMap):
        @property
        def key_name(self):
            return map_conf['key_name']
        @property
        def key_index(self):
            return map_conf['key_index']
        @property
        def drcrdesc(self):
            return map_conf['drcrdesc']
        @property
        def accid_col(self):
            return map_conf['accid_col']
        @property
        def accna_col(self):
            return map_conf['accna_col']
        @property
        def top_accid_col(self):
            return map_conf['top_accid_col']
        @property
        def top_accna_col(self):
            return map_conf['top_accna_col']
        @property
        def date_col(self):
            return map_conf['date_col']
        @property
        def top_accid_len(self):
            return map_conf['top_accid_len']
        @property
        def accna_split_by(self):
            return map_conf['accna_split_by']
        @property
        def date_split_by(self):
            return map_conf['date_split_by']
    xlmap=InstMap.from_dict(cols)
    xlmeta=JsonMeta(meta_conf)
    mgl=MGL(xlmap=xlmap,xlmeta=xlmeta)
    return mgl
if __name__=='__main__':
    pass
