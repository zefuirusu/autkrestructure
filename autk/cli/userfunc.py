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
def save_by_shmeta(df,shmeta:str):
    from autk.gentk.funcs import save_df
    save_df(
        df,
        shmeta.split(',')[1], # sheet name
        shmeta.split(',')[0], # path
    )
    pass
def show_shtli(args):
    shtli=[]
    for sht in XlBook(args.ifp).shtli:
        print(sht)
        shtli.append(sht)
        continue
    return shtli
def __parse_range_meta_row(range_meta_str:str):
    '''
    input meta structure:
        <ifp,shtna,start_rdx,start_cdx,end_rdx,end_cdx>
    parse ouput:
        {
            ifp:{
                shtna:[
                    ((start_rdx,start_cdx),(end_rdx,end_cdx)),
                ]
            }
        }
    '''
    range_meta=range_meta_str.split(',')
    ifp=range_meta[0]
    shtna=range_meta[1]
    start_rdx=int(range_meta[2]) # start-row index
    start_cdx=int(range_meta[3]) # start-column index
    end_rdx=int(range_meta[4]) # end-row index
    end_cdx=int(range_meta[5]) # end-column index
    single={
        ifp:{
            shtna:[
                ((start_rdx,start_cdx),(end_rdx,end_cdx)),# matrix range
            ]
        }
    }
    return single
def __parse_range_meta_set(range_meta_list:list,range_meta_json:dict):
    for range_meta_row in range_meta_list:
        range_meta=__parse_range_meta_row(range_meta_row)
        ifp=list(range_meta.keys())[0]
        if ifp in range_meta_json.keys():
            if range_meta.get(ifp)==range_meta_json.get(ifp):
                pass
            else:
                for shtna in range_meta[ifp].keys():
                    if shtna in range_meta_json.get(ifp).keys():
                        if range_meta.get(ifp).get(shtna)==range_meta_json.get(ifp).get(shtna):
                            pass
                        else:
                            range_meta_json[ifp][shtna].extend(
                                range_meta_row.get(ifp).get(shtna)
                            )
                    range_meta_json[ifp].update(
                        range_meta_row.get(ifp).get(shtna)
                    )
                    continue
            pass
        else:
            range_meta_json.update(range_meta)
    return range_meta_json
def show_matrix(args):
    from threading import Thread,Lock
    from pandas import concat
    from autk.gentk.funcs import start_thread_list
    from autk.brother.xlbk import XlBook
    print('get input:',args.meta)
    thli=[]
    xlmeta={}
    for arg_set in args.meta:
        thli.append(
            Thread(
                target=__parse_range_meta_set,
                args=(arg_set,xlmeta)
            )
        )
        continue
    start_thread_list(thli)
    print('xlmeta:',xlmeta)
    def __append_df(
        dfli:list,
        path:str,
        range_meta:(str,(int,int),(int,int))
    ):
        bklock=Lock()
        xl=XlBook(path)
        bklock.acquire()
        df=xl.select_matrix(
            range_meta[0],
            range_meta[1],
            range_meta[2],
            type_df=True,
            has_title=True
        )
        df['from_path']=path
        df['from_sheet']=range_meta[0]
        df['from_range_start']=str(range_meta[1])
        df['from_range_end']=str(range_meta[2])
        dfli.append(df)
        bklock.release()
        pass
    thli=[]
    dfli=[]
    for path in xlmeta.keys():
        for shtna in xlmeta[path].keys():
            for range in xlmeta.get(path).get(shtna):
                thli.append(
                        Thread(
                            target=__append_df,
                            args=(
                                dfli,
                                path,
                                (shtna,range[0],range[1])
                            ),
                        )
                    )
                continue
            continue
        continue
    start_thread_list(thli)
    resu=concat(dfli,axis=0,join='outer')
    print(resu)
    if args.save is not None:
        save_by_shmeta(resu,args.save)
    return resu
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
        save_by_shmeta(t.data,args.save)
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
    from autk.mapper.base import XlMap
    if args.type=='list':
        map_cols=[]
        for item in args.col:
            if isinstance(item,str):
                map_cols.append(item)
            elif isinstance(item,list):
                map_cols.extend(item)
            else:
                pass
            continue
        xlmap=XlMap.from_list(map_cols)
        print("map cols to save:",xlmap.show)
        xlmap.save(args.save)
    elif args.type=='dict':
        map_cols={}
        for item in args.col:
            if isinstance(item,str):
                map_cols.update(
                    {item.split(',')[0]:item.split(',')[1]}
                )
            elif isinstance(item,list):
                for __item in item:
                    map_cols.update(
                        {__item.split(',')[0]:int(__item.split(',')[1])}
                    )
            else:
                print("check arg:{}".format(args.type))
            continue
        xlmap=XlMap.from_dict(map_cols)
        print("map cols to save:",xlmap.show)
        xlmap.save(args.save)
    else:
        print("check arg:{}".format(args.type))
    pass
def config_gl(args):
    '''
        This function has not been finished yet.
    '''
    from os.path import isfile
    from autk.gentk.funcs import f2dict,dict2json
    GL_CONFIG_DEFAULT={
        "key_name":"glid",
        "key_index":[],
        "drcrdesc":["dr","cr"],
        "accid_col":"accid",
        "accna_col":"accna",
        "top_accid_col":"top_accid",
        "top_accna_col":"top_accna",
        "date_col":"date",
        "top_accid_len":4,
        "accna_split_by":"/",
        "date_split_by":"-"
    }
    to_update={
        "key_name":args.keyna,
        "key_index":args.keyidx,
        "drcrdesc":args.drcr,
        "accid_col":args.accid,
        "accna_col":args.accna,
        "top_accid_col":args.topidc,
        "top_accna_col":args.topnac,
        "date_col":args.date,
        "top_accid_len":args.topidlen,
        "accna_split_by":args.namesplit,
        "date_split_by":args.datesplit
    }
    current_json=f2dict(args.save)
    def __update(p,k):
        if to_update[k] is None:
            v=current_json[k]
            if v !=GL_CONFIG_DEFAULT[k]:
                pass
            else:
                v=GL_CONFIG_DEFAULT[k]
        else:
            v=to_update[k]
        if isfile(p):
            temp=f2dict(p)
            temp.update({k:v})
            dict2json(temp,p)
        else:
            dict2json({k:v},p)
        pass
    for k in to_update.keys():
        __update(args.save,k)
    return to_update
def joinxl(args):
    '''
        This function aims to join sheets from Excel workbooks.
        If you need to join ranges(matrice) from sheets of Excel workbooks,
        `autk show matrix --save <path,sheet> --meta <ifp,shtna,srdx,scdx,eidx,ecdx>`
        maybe a better choice;
    '''
    from pandas import concat
    from threading import Thread,Lock
    from autk.gentk.funcs import start_thread_list
    from autk.brother.xlbk import XlBook
    print('user input:',args.meta)
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
        save_by_shmeta(resu,args.save)
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
