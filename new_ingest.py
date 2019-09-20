import os, time, pandas as pd
from configparser import ConfigParser
import csv, pyodbc

startTime = time.time()
print ("Start time is " + time.asctime())

print("tempFL", "NEW_SELECTION", '"DBKey" LIKE ' + "'" + os.path.basename(inDIMA)[:-6] + "%'")
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


# access_con_string = msconfig()

MDB = 'C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb'
DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

# connect to db
con = pyodbc.connect(r"DRIVER={};DBQ={};".format(DRV,MDB))
cur = con.cursor()

dirDIMAs = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Documents\GitHub\dima_pg"
# outDB = pd.DataFrame(" ")

tableFile = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Downloads\py_txt\inputtablesForNewSchema - Copy.txt"

dateLoadedField = "DateLoadedInDb"
loadDate = '"2019-09-20"'

# some tables are not replaced by Key but instead replace by DBey
appendTables = ["tblEcolSites", "tblPeople", "tblSpecies", "tblSpecRichDetail", "tblSpeciesGeneric", "tblSites"]

# used for creating XY layer from DIMA tblPlots - assuming NAD 83
wkidCode = 4269

tableErrors = []
tableDups = []

tableRead = open(tableFile,"r")
lineList = tableRead.readlines()
tableList = {}

full_tbl_list = []
for tbl in cur.tables():
    if "tbl" in tbl[2]:
        cur1 = con.cursor()
        if tbl.table_type == "TABLE":
            full_tbl_list += [tbl.table_name,]

for line in lineList:
    tableList.update({line.split("|")[0] : line.split("|")[1].split(",")[0]})

# do for all access files in the dir given
dimaCount = 0

# INPUT LAYER/TABLE tempfl = table to which to join -> table
# INPUT FIELD tableList[table] = field to use for join
# TABLE TO BE JOINED = table to be joined -> inDIMA
tableList['tblGapDetail']

listDIMAs = []
seen = None
dflist=[]
for file in os.listdir(dirDIMAs):
    if file.endswith(".mdb"):
        listDIMAs.append(dirDIMAs + "\\" + file)

for inDIMA in listDIMAs:
    # if one DIMA fails go on to the next one
    stopFlag = 0
    print ("\nStarting DIMA - " + inDIMA)
    print ("   Starting Dups check")

    for table in tableList:
        seen = set(dflist)
        if stopFlag == 0 and table not in seen:
            seen.add(table)
            query = '\'SELECT * FROM db."{table}"\''.format(table=table)
            exec('{df} = pd.read_sql({query},con)'.format(df=table,query=query))
            dflist.append(table)
    for table in dflist:
        if "DBKey" in eval(table).columns:
            print(table, "has DBKey")
            dimaCount = len(dflist)+1
        if dimaCount > 0:
            print("      " + table + " - ERROR Records with the same or similar DBKey found!")
            print("   ** Skipping DIMA " + inDIMA + " - failed Dups check! **")
            stopFlag = 1
            tableErrors.append("Table " + table + " in " + inDIMA + " failed on import due to Dups in DBKeys.")
            # bail out of loop - hate to do it but make smore sense
            continue
        else:
            print("      " + table + " - No Dups found")
######################### join
        if not table in appendTables:




            # First check all for dups DBKey then actually ingest only if ALL tables pass the check
            arcpy.MakeTableView_management(outDB + "\\" + table, "tempFL")


            # Need to check to see if somehow other DIMAs are using the Key field for this table - so compare all records to a DBKey select
            # Need to check using a Like since name may now be 02 for example
            arcpy.SelectLayerByAttribute_management ("tempFL", "NEW_SELECTION", '"DBKey" LIKE ' + "'" + os.path.basename(inDIMA)[:-6] + "%'")
            dimaCount = int(arcpy.GetCount_management("tempFL").getOutput(0))
