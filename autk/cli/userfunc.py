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
    ## user-input title is 1-based index,
    ## the `title` to create XlSheet is 1-based index currently;
    from autk.brother.xlbk import XlBook
    from autk.meta.pmeta import DirMeta
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
def joinxl(args):
    from pandas import concat
    from threading import Thread,Lock
    from autk.gentk.funcs import start_thread_list
    from autk.brother.xlbk import XlBook
    print('user input:',args.meta)
    # TODO this function needs to be upgraded.
    '''
    An example of `join_meta`:
        {
            path1:{
                sheet1:{
                    title_row:1,
                    bottom_row:100,
                    start_col:2,
                    end_col:12,
                },
                sheet2:{...},
                ...
            },
            path2:{
                sheet_x:{...},
                sheet_y:{...}
            },
            ...
        }
    '''
    join_meta={}
    for arg_row in args.meta:
        for item in arg_row:
            parts=item.split(',')
            if len(parts) !=3:
              continue ## ignore invalid input argument;
            path=parts[0]
            shtna=parts[1]
            title=int(parts[2])
            if path in list(join_meta.keys()):
                if [shtna,title] in join_meta[path]:
                    pass
                else:
                    join_meta[path].append([shtna,title])
            else:
                join_meta.update({path:[[shtna,title]]})
            continue
    print('Join Excel data from:',join_meta)
    def __df_collect(path,dfli):
        collect_lock=Lock()
        collect_lock.acquire()
        xl=XlBook(path)
        for shmeta in join_meta[path]:
            shtna=shmeta[0]
            title=shmeta[1]
            df=xl.get_df(shtna,title=title)
            df['from_path']=path
            df['from_sheet']=shtna
            df['title']=title
            dfli.append(
                df
            )
        collect_lock.release()
        return
    dfli=[]
    thli=[]
    for path in list(join_meta.keys()):
        thli.append(
            Thread(
                target=__df_collect,
                args=(path,dfli,)
            )
        )
        continue
    start_thread_list(thli)
    resu=concat(dfli,axis=0,join='outer')
    print(resu)
    if args.save is not None:
        save_path=args.save[0]
        save_shtna=args.save[1]
        from autk.gentk.funcs import save_df
        save_df(resu,save_shtna,save_path)
    return resu
def pca_analysis(args):
    from autk.gentk.pca import ClusterPca
    from autk.brother.xlbk import XlBook
    if args.save is not None:
        from autk.gentk.funcs import save_df
        from numpy import ndarray
        from pandas import DataFrame,Series
    df=XlBook(args.ifp).select_matrix(
        args.shtna,
        args.start,
        args.end,
        type_df=True,
        has_title=True
    )
    pca=ClusterPca()
    print(
        "Load matrix from file:{},sheet:{},range start:{},range end:{},get:\n\t {}".format(
            args.ifp,
            args.shtna,
            args.start,
            args.end,
            df,
        )
    )
    pca.load_matrix(df)
    if args.ifstd == True:
        print("Standardize input data...")
        pca.standardize()
    print("Perform `Hierarchical Cluster Analysis` process...")
    pca.hie_cluster(
        method=args.method,
        metric=args.metric,
        no_plot=False
    )
    pca.cov_pca()
    pca.check()
    print('=='*6,'PCA Result','=='*6)
    for k in list(pca.__dict__.keys()):
        d=getattr(pca,k)
        if d is not None:
            print('[Note] {}:'.format(k))
            if args.save is not None:
                if isinstance(d,DataFrame):
                    save_df(d,k,args.save)
                elif isinstance(d,ndarray) or isinstance(d,Series):
                    save_df(
                        DataFrame(d),
                        k,
                        args.save
                    )
                else:
                    print('[Warning] unsaved data: {}'.format(k))
            print(d)
        continue
    return pca
