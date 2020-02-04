from os.path import normpath, split, splitext, join
from os import listdir
from arcnah import arcno
from new_primarykeys import pk_add, gap_pk, pg_send, drop_one, bsne_pk
from datetime import datetime
import pandas as pd

arc = arcno()
"""
Directory with network dimas to ingest!

"""
path = r"INSERT_PATH_HERE"
networkdims = listdir(path)
normpath(join(path, networkdims))
"""
Main ingester
"""
count = 0
for i in networkdims:
    dimapath = normpath(join(path,i))
    print(dimapath,count)
    count+=1
    for table in maintablelist:
        pg_send(f'{table}',dimapath)
"""
utilities:
- table drop loop
"""
for table in arc.maintablelist:
    drop_one(table)

for table in arc.newtables:
    drop_one(table)
