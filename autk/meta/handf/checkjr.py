#!/usr/bin/env python
# -*- coding: utf-8 -*-


from autk.parser.funcs import f2list,save_df,relative_path
from autk.handf.findfile import find_regex

def locate_by_func(
    by_func, # to transform `target string` into regex_item; one parameter;
    jrli_path, # file path which contains a list of `target string` to be found; list can be passed as well;
    sdir, # search directory to search each `target string`;
    relative=False, # whether to export relative path;
    ref_path=None, # to what base path does the output relative path relative to?
):
    '''
    This function aims to figure out whether all the
    required `Audit_Evidence` are captured;
    Input `target strings` to search by file at `jrli_path`;
    Tansform `target strings` into regular expression(regex_item) through `by_func`;
    Search `regex_item` in `sdir`;
    `relative:bool` determines whether results are relative
    from `ref_path`, if true, you must provide `ref_path`;
    Output data will be sort by column 'target', and looks like:
    ___________________________________________
    |target|regex|file|directory|fcount|dcount|
    |------|-----|----|---------|-----|-------|
    -------------------------------------------
    '''
    from os.path import isfile,isdir
    from pandas import DataFrame,Series,concat
    if relative==True and ref_path is None:
        print('check argument: ref_path')
        return
    if isinstance(jrli_path,list):
        jrli=jrli_path
    elif isfile(jrli_path):
        jrli=f2list(jrli_path)
    else:
        print('check argument:jrli_path')
        return
    resu_df=DataFrame(
        [],
        index=jrli,
        columns=[
            'target',
            'regex',
            'file',#'location',
            'directory',
            'fcount',#'count',
            'dcount'
        ]
    )
    def rel_path_list(path_list):
        # list of relative_path;
        return [
            relative_path(p,ref_path) for p in path_list
        ]
    def parse_result(count,results):
        if count==0:
            location=''
        elif count==1:
            location=results[0] if relative==False else relative_path(results[0],ref_path)
        elif count>1:
            location=';'.join(results) if relative==False else ';'.join(rel_path_list(results))
        else:
            location='impossible'
        return location
    for raw_jr in resu_df.index:
        jr=by_func(raw_jr) # regex for raw_jr
        f_resu=find_regex(jr,sdir)[0]
        d_resu=find_regex(jr,sdir)[1]
        fcount=len(f_resu)
        dcount=len(d_resu)
        resu_df.loc[raw_jr,:]=Series(
            [
                raw_jr,
                jr,
                parse_result(fcount,f_resu),
                parse_result(dcount,d_resu),
                fcount,
                dcount
            ],
            index=resu_df.columns
        )
        continue
    resu_df.sort_values(
        'target',
        ascending=True,
        inplace=True,
        ignore_index=False
    )
    return resu_df
if __name__=='__main__':
    pass
