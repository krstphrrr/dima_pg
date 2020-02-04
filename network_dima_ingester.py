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
path = r"C:\Users\kbonefont\Desktop\newdimas\New"
networkdims = listdir(path)
#normpath(join(path, networkdims))
"""
Main ingester
"""
count = 0
for i in networkdims:
    dimapath = normpath(join(path,i))
    print(dimapath,count)
    count+=1
    for table in arc.maintablelist:
        pg_send(f'{table}',dimapath)
"""
utilities:
- table drop loop
"""
for table in arc.maintablelist:
    drop_one(table)

for table in arc.newtables:
    drop_one(table)
"""
check how many rows in table are in a table
where itemtype == T or M
(mwac or dust deposition)
"""
dustcounter = 0
suspectRows = {}
for index, row, in bsne_pk(normpath(join(path,networkdims[2]))).iterrows():
    if (row['ItemType']=='t'):

        dustcounter+=1
        suspectRows.update({dustcounter:row})
print(dustcounter)
"""
check number of tables with data inside dima,
just to see if there actually any network tables etc.
"""
arc = arcno()
completepath = normpath(join(path,networkdims[0]))
for table in arc.tablelist:
    if arc.MakeTableView(table,completepath).shape[0]>2:
        print(table, arc.MakeTableView(table,completepath).shape[0])
