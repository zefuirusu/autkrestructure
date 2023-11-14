#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
## `Cycle Approach` to Segmenting an Audit 
A common way to divide an audit is 
to keep closely related types (or classes) 
of transactions and account balances in the same segment. 
This is called the cycle approach.
## `Basic Audit Procedures`
    Inquiry,
    Confirmation,
    Observation,
    Inspection,
    Recalculation,
    Reperformance,
    AnalyticalProcess;
'''


from autk.brother.xlbk import XlBook
from autk.calculation.mortal.mortalgl import MGL
from autk.calculation.mortal.mortalchart import MCA

class BasicProcedure:
    def __init__(
        self,
        workpath,
        mgl,
        mca,
    ):
        self.workpath=workpath
        pass
class SegmentPaper:
    def __init__(
        self,
        workpath,
        mgl,
        mca,
    ):
        self.workpath=workpath
        self.mgl=mgl
        self.mca=mca
        pass
    def conclusion(self):
        pass
    def detail(self):
        pass
    def confirm(self):
        pass
    def doc_inspect(self):
        pass
    def physical_inspect(self):
        pass
    pass
class CostSegPaper(SegmentPaper):
    def cost_analysis(self):
        pass
    pass
