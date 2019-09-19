import os, time, pandas as pd

import csv, pyodbc

def dconfig(filename='database.ini', section='dima'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def msconfig(filename='database.ini', section='msaccess'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

# set up some constants
MDB = 'C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb'
DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

# connect to db
con = pyodbc.connect(r"DRIVER={};DBQ={};".format(DRV,MDB))
con = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb;')
cur = con.cursor()
con.setencoding(encoding='utf-8')


from configparser import ConfigParser
import psycopg2
pgparams = dconfig()
conn = psycopg2.connect(**pgparams)
pgcur = conn.cursor()

postgresql_fields = {
    'COUNTER': 'serial',
    'VARCHAR': 'text',
    'LONGCHAR': 'text',
    'BYTE': 'integer',
    'SMALLINT': 'integer',
    'INTEGER': 'bigint',
    'REAL': 'real',
    'DOUBLE': 'double precision',
    'DATETIME': 'timestamp',
    'CURRENCY': 'money',
    'BIT':  'boolean',
}
pgcur.execute('CREATE TABLE db."One"(field1 text, field2 text);')
conn.commit()


for col in cur.columns(table='tblSites'):
    if col.type_name in postgresql_fields:
        print(col.type_name+"is")

fun1 =

sql = """something "{word}(""".format(word="word2")
sql+= "something"
sql+=""" )"""

print(cur.columns(new_tbllist[1]))

def get_cols():
    col_count = {}
    for table in []


new_tbllist=[]

just_three = new_tbllist[0:3]
just_three
for tbl in cur.tables():
    if "tbl" in tbl[2]:
        cur1 = con.cursor()
        if tbl.table_type == "TABLE":
            new_tbllist += [tbl.table_name,]

for tbl in new_tbllist:
    sql="""
    CREATE TABLE db."{table}"
    (""".format(table=tbl)
    sql+=0
nostr ="NOSTRING"

string = """stringstringstring"{str}",(
   """.format(str=nostr)
string+="ANOTHERSTRING"
string+=""")"""
print(string)

[(cur.columns(table='tblTempPlots')

field_list = []
for col in cur.columns(table='tblTempPlots'):
    if col.type_name in postgresql_fields:
        field_list+=['"'+col.column_name+'"'+" "+postgresql_fields[col.type_name],]
    elif column.type_name == "DECIMAL":
        field_list += ['"' + column.column_name + '"' +
                       " numeric(" + str(column.column_size) + "," +
                       str(column.decimal_digits) + ")", ]
    else:
        print("column " + table + "." + column.column_name +
        " has uncatered for type: " + column.type_name)

def create_fields(table):
    postgresql_fields = {
        'COUNTER': 'serial',  # autoincrement
        'VARCHAR': 'text',  # text
        'LONGCHAR': 'text',  # memo
        'BYTE': 'integer',  # byte
        'SMALLINT': 'integer',  # integer
        'INTEGER': 'bigint',  # long integer
        'REAL': 'real',  # single
        'DOUBLE': 'double precision',  # double
        'DATETIME': 'timestamp',  # date/time
        'CURRENCY': 'money',  # currency
        'BIT':  'boolean',  # yes/no
    }
    SQL = ""
    field_list = list()
    for col in cur.columns(table=table):
        if col.type_name in postgresql_fields:
            field_list += ['"' + col.column_name + '"' +
                           " " + postgresql_fields[col.type_name], ]
        elif col.type_name == "DECIMAL":
            field_list += ['"' + col.column_name + '"' +
                           " numeric(" + str(col.column_size) + "," +
                           str(col.decimal_digits) + ")", ]
        else:
            print("column " + table + "." + col.column_name +
            " has uncatered for type: " + col.type_name)
    return ",\n ".join(field_list)
create_fields('tblGISDatums')
''
for table in just_three:
    import pandas as pd
    # first_round = 'DROP TABLE IF EXISTS '
    # SQL ='CREATE TABLE db."{table}"('.format(table=table)
    # SQL += create_fields(table)
    # SQL +=")"
    query = '\'SELECT * FROM db."{table}"\''.format(table=table)
    # pgcur.execute(SQL)
    dflist = {'a':'df1','b':'df2','c':'df3'}
    dflist2 =[]
    for df in dflist:
        exec('{df} = pd.read_sql({query},con)'.format(df=table,query=query))
        dflist2.append(table)
    for df in dflist2:
        print('RecKey' in eval(df).columns)
        elif 'daysExposed' in eval(dfs).columns:
            print("yes")


    for unit in dflist2:
        print(eval(unit).columns)
        print('RecKey' in eval(df).columns)
        if 'RecKey' in eval(df).columns:
            print("no")
        elif 'daysExposed' in eval(df).columns:
            print("yes")

tblBSNE_BoxCollection.columns
'RecKey' in eval('tblBSNE_BoxCollection').columns
if 'RecKey' in df1.columns:
    print("CHECK")
elif 'DBKey' in df1.columns:
    print("don't")
df1
df2
df3
for df in dflist:
    print(dflist[df])
for tbl in cur.tables():
    if "tbl" in tbl[2]:
        print(tbl)
        # for row in cur.columns(table=tbl):
        #     print(row)

cur.getTypeInfo(table='tblLICDetail')
cur.nextset()

for tbl in cur.tables():
    print(tbl.table_name)

if cur.tables(table='tblTempPlots').fetchone():
    print('mhm')
new_list = []

def tbl_(tab):
    tbl = "{}".format(tab)
    for row in cur.columns(table=tbl):

        print(row)
cur.description




row = cur.fetchone()
print(row[0])

def tbl2(tab):
    # tbl = "{}".format(tab)
    print(cur.execute('SELECT * FROM db."{tb}"'.format(tb=tab)).fetchall())

tbl2('tblGapHeader')
tbl_('tblLICDetail')

startTime = time.time()
print ("Start time is " + time.asctime())

# dima directory
dirDIMAs = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Desktop\Some_data"

# directly to postgis
outDB = r" "

# text file that held tables inside dimas
tableFile = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Downloads\py_txt\inputtablesForNewSchema - Copy.txt"

# ingest year
dateLoadedField = "DateLoadedInDb"
loadDate = '"2018-09-01"'

# some tables are not replaced by Key but instead replace by DBkey // exceptions
appendTables = ["tblEcolSites", "tblPeople", "tblSpecies", "tblSpecRichDetail", "tblSpeciesGeneric", "tblSites"]

# used for creating XY layer from DIMA tblPlots - assuming NAD 83
wkidCode = 4269


tableErrors = []
tableDups = []

tableRead = open(tableFile,"r", encoding = 'utf-8')
lineList = tableRead.readlines()
tableList = {}

line = 'tblGapDetail|RecKey,SeqNo|N\n'
line.split("|")[1]
# .split(",")[0]
df = pd.DataFrame(None)

for line in lineList:
    # update changes format from: [field1|field2,field3|field4] to {field1:field2 }
    tableList.update({line.split("|")[0] : line.split("|")[1].split(",")[0]})

listDIMAs = []
for file in os.listdir(dirDIMAs):
    if file.endswith(".mdb"):
        listDIMAs.append(dirDIMAs + "\\" + file)


con = pyodbc.connect(r"DRIVER={};DBQ={};".format(DRV,MDB))
con = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb;')
cur = con.cursor()

import json,sys

ms_config = {'driver':'{Microsoft Access Driver (*.mdb, *.accdb)}',
'dqb':'C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb'}
with open('config.json','w') as f:
    json.dump(ms_config,f)

with open('config.json', 'r') as f:
    msconfig = json.load(f)

config_path = os.path.abspath()
msconfig
config_data = dconfig()
pg_con_string = dconfig()
pyodbc.connect(**access_con_string)
access_con_string = msconfig()


config_data = json.load(open(config_path))
if __name__ == "__main__":
    ms_pg = all_pg(access_con_string,pg_con_string,print_SQL)
    ms_pg.create_schema()
    ms_pg.create_tables()
