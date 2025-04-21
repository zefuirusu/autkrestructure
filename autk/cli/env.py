#!/usr/bin/env python
# -*- coding: utf-8 -*-


from autk.gentk.start import PRJ_JSON
def get_script_dir(home_dir):
  pli=[]
  for sub in home_dir['subdirs']:
    pli.append(sub['name'])
    if 'script' in [subsub['name'] for subsub in sub['subdirs']]:
      pli.append('script')
      return '/'.join(pli)
    else:
      get_script_dir(sub)

PRJ_CONFIG=[
  {
    "name":"config",
    "help":"project configuration.",
    "args":[
        ("--comment",{"type":str,"help":""}),
    ],
    "func":lambda args:'TODO',
    "subcmd":[
      {
        "name":"new",
        "help":"",
        "args":[
          ("--type",{"type":str,"help":"to assign what type of config file."}),
          ("--savename",{"type":str,"help":"name of the json file."}),
        ],
        "func":lambda args:"TODO",
        "subcmd":[],
      },
    ],
  },
]
if __name__=='__main__':
  print(get_script_dir(PRJ_JSON))
  pass
