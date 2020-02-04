from os.path import normpath, split, splitext, join
from os import listdir
from arcnah import arcno
from new_primarykeys import pk_add, gap_pk, pg_send, drop_one, bsne_pk
from datetime import datetime
import pandas as pd
from utils import db

arc = arcno()
"""
Directory with network dimas to ingest!

"""
path = r"C:\Users\kbonefont\Desktop\newdimas\Network_DIMAs_20190826"
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
"""
Fixing HorizontalFlux table:
 - two dateestablished fields (from join)
 - two notes! one is empty (drop)
"""

for index, row, in bsne_pk(normpath(join(path,networkdims[0]))).iterrows():
    if (row['DateEstablished_x']==null):

        fieldCounter+=1
        suspectRows.update({fieldCounter:row})
print(fieldCounter)
"""
Fixing fields

"""
try:
    con = db.str
    cur = db.str.cursor()

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    DROP COLUMN  "Notes";
    """)

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    RENAME COLUMN "DateEstablished_x" TO "DateEstablished";
    """)

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    RENAME COLUMN "DateModified_x" TO "DateModified";
    """)

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    RENAME COLUMN "Notes_x" TO "Notes";
    """)

    ### DROPS

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    DROP COLUMN "DateEstablished_y";
    """)

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    DROP COLUMN "DateModified_y";
    """)

    cur.execute("""
    ALTER TABLE public."tblHorizontalFlux"
    DROP COLUMN "Notes_y";
    """)
    con.commit()
except Exception as e:
    con = db.str
    cur = db.str.cursor()

    print(e)
