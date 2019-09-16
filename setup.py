import os, time, pandas as pd

import csv, pyodbc

sources = pyodbc.dataSources()
keys = sources.keys()
for key in keys:
   print(key)


# set up some constants
MDB = 'C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb'
DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

# connect to db
con = pyodbc.connect(r"DRIVER={};DBQ={};".format(DRV,MDB))
con = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb;')
cur = con.cursor()
list(cur.columns())


for tbl in list(cur.tables()):
    if "tbl" in tbl[2]:
        print(tbl[2])




# you could change the mode from 'w' to 'a' (append) for any subsequent queries
with open('mytable.csv', 'wb') as fou:
    csv_writer = csv.writer(fou) # default field-delimiter is ","
    csv_writer.writerows(rows)



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




for inDIMA in listDIMAs:
    # if one DIMA fails go on to the next one
    import pandas as pd
    stopFlag = 0
    print ("\nStarting DIMA - " + inDIMA)
    print ("   Starting Dups check")

    for table in tableList:
        if stopFlag == 0:
            # First check all for dups DBKey then actually ingest only if ALL tables pass the check
                # pandas or geopandas table ("dir/PGtable", "temptablename")
            arcpy.MakeTableView_management(outDB + "\\" + table, "tempFL")

            # Need to check to see if somehow other DIMAs are using the Key field for this table - so compare all records to a DBKey select
            # Need to check using a Like since name may now be 02 for example
                # on temptable, select, WHERE CLAUSE, in this case dbkey that's LIKE dimaname %
            arcpy.SelectLayerByAttribute_management("tempFL", "NEW_SELECTION", '"DBKey" LIKE ' + "'" + os.path.basename(inDIMA)[:-6] + "%'")

            dimaCount = int(arcpy.GetCount_management("tempFL").getOutput(0))
            if dimaCount > 0:
                print "      " + table + " - ERROR Records with the same or similar DBKey found!"
                print "   ** Skipping DIMA " + inDIMA + " - failed Dups check! **"
                stopFlag = 1
                tableErrors.append("Table " + table + " in " + inDIMA + " failed on import due to Dups in DBKeys.")
                # bail out of loop - hate to do it but make smore sense
                continue
            else:
                print "      " + table + " - No Dups found"
            arcpy.SelectLayerByAttribute_management ("tempFL", "CLEAR_SELECTION")

            # check if records already exsist by their main key as set in the table file - if so stop the script
            # some tables the keys should not be checked
            if not table in appendTables:
                arcpy.AddJoin_management("tempFL", tableList[table], inDIMA + "\\" + table, tableList[table], "KEEP_COMMON")
                # Need to exempt 999999999 and 888888888 from the dup check
                arcpy.SelectLayerByAttribute_management ("tempFL", "NEW_SELECTION", table + "." + tableList[table] + " <> '999999999' And " + table + "." + tableList[table] + " <> '888888888'")
                joinedCount = int(arcpy.GetCount_management("tempFL").getOutput(0))
                if joinedCount > 0:
                    print "      " + table + " - ERROR Records with the same Keys " + tableList[table] + " as DIMA found!"
                    print "   ** Skipping DIMA " + inDIMA + " - failed Dups check! **"
                    stopFlag = 1
                    tableErrors.append("Table " + table + " in " + inDIMA + " has Dups in Key field.  DIMA was skipped!")
                    # bail out of loop
                    continue

                arcpy.RemoveJoin_management("tempFL")
            arcpy.Delete_management("tempFL")
