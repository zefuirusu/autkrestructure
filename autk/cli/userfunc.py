#!/usr/bin/env python
# -*- coding: utf-8 -*-

from autk.gentk.quick import gl_from_json

def yesno(yn_str):
    if yn_str=='yes' or yn_str=='y':
        return True
    elif yn_str=='no' or yn_str=='n':
        return False
    else:
        print('check argument:yes/no')
        return True
def show_shtli(args):
    shtli=[]
    for sht in XlBook(args.ifp).shtli:
        print(sht)
        shtli.append(sht)
        continue
    return shtli
def table_df(args):
    '''
        This function returns Table, not DataFrame
    '''
    from autk.gentk.funcs import f2dict
    from autk.calculation.base.table import ImmortalTable
    from autk.mapper.base import XlMap
    from autk.meta.pmeta import JsonMeta
    t=ImmortalTable(
        xlmap=XlMap.from_dict(f2dict(args.map)) if args.map is not None else None,
        xlmeta=JsonMeta(f2dict(args.meta)) if args.meta is not None else None,
    )
    t.load_raw_data()
    # for xl in t.xlset:
    #     print(xl)
    print(t)
    print(t.data)
    if args.save is not None:
        from autk.gentk.funcs import save_df
        save_df(t.data,'data',args.save)
    else:
        pass
    return t
def table_search(args):
    pass
def table_call(args):
    pass
def mgl_df(args):
    mgl=gl_from_json(args.config)
    mgl.load_raw_data()
    print(mgl.data)
    return mgl
def mgl_search(args):
    from pandas import DataFrame
    mgl=gl_from_json(args.config)
    print('get MGL:',mgl)
    resu=mgl.search(
        args.regex,
        args.col,
        type_xl=False
    )
    if yesno(args.dftype)==True:
        print(resu)
        #  for r in resu.iterrows():
            #  print(DataFrame(r[1]).T)
    elif yesno(args.dftype)==False:
        resu=resu.values
        for r in resu:
            print(r)
    else:
        print("check argument:--dftype")
    return resu
def get_script_dir(home_dir):
    '''
    locate and return the directory of `script`;
    '''
    pli=[]
    for sub in home_dir['subdirs']:
        pli.append(sub['name'])
        if 'script' in [subsub['name'] for subsub in sub['subdirs']]:
            pli.append('script')
            return '/'.join(pli)
        else:
            get_script_dir(sub)
def config_xlmeta(args):
    from autk.brother.xlbk import XlBook
    from autk.meta.pmeta import DirMeta
    ## user-input title is 1-based index,
    ## the `title` to create XlSheet is 1-based index currently;
    DirMeta(args.base,common_title=args.title).save(args.save)
    pass
def config_xlmap(args):
    '''
        This function has not been finished yet.
    '''
    if args.type=='list':
        # TODO
        from autk.mapper.base import XlMap
        XlMap.from_list(args.col).save(args.save)
        pass
    elif args.type=='dict':
        pass
    else:
        print("check arg:{}".format(args.type))
    pass
def config_gl(args):
    '''
        This function has not been finished yet.
    '''
    # TODO
    return

