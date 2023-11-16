#!/usr/bin/env python
# -*- coding: utf-8 -*-


from autk.gentk.funcs import f2dict
from autk.calculation.mortal.mortalgl import MGL
from autk.calculation.mortal.mortalchart import MCA

def gl(
    xlmap_path:str,
    xlmeta_json:str,
):
    return MGL(
        xlmap=xlmap_path,
        xlmeta=f2dict(xlmeta_path)
    )
def chart(
    xlmap_path:str,
    xlmeta_json:str,
):
    return MCA(
        xlmap=xlmap_path,
        xlmeta=f2dict(xlmeta_path)
    )
    pass

if __name__=='__main__':
    pass
