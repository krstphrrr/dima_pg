import pandas as pd
import os
from arcnah import arcno

def fun1(arg):
    print(f'{arg}')

def fun2(arg):
    print(arg)

type(fun1('something'))

type(fun2('something'))

"""
directories
"""
dirDIMAs = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Desktop\Some_data"
# outDB <= not needed, df's are not in an external directory
tableFile = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Downloads\py_txt\inputtablesForNewSchema - Copy.txt"
appendTables = ["tblEcolSites", "tblPeople", "tblSpecies", "tblSpecRichDetail", "tblSpeciesGeneric", "tblSites"]
wkidCode = 4269

"""
lists + dictionaries
"""
tableDups = []
tableErrors = []
# tblLPIDetail

tableRead = open(tableFile,"r", encoding="utf-8")
lineList = tableRead.readlines()
tableList = {}
for line in lineList:
    tableList.update({line.split("|")[0] : line.split("|")[1].split(",")[0]})

listDIMAs = []
# getOutput
# import pandas as pd
# pd.merge(df,dff,on='RecKey',how='outer')
# pd
#
from arcnah import arcno
arcno = arcno()

arcno.MakeTableView_management('tblGapDetail')
# df = arcno.temp
# dff = arcno.temp
# df1 = df[0:9]
# df2 = df[10:21]
# pd.concat([df1,df2])
# number =180712125342201
# print(df['RecKey']==f'{number}')
# arcno.GetCount_management()
# arcno.AddJoin_management(df,'RecKey',dff,'RecKey')
# arcno.temp_table
arcno.SelectLayerByAttribute_management(arcno.temp, "RecKey",1809200916516982,operator="<>")
arcno.temp.shape

# arcno.GetCount_management()
# for table in tableList:
#     print(table)
#     if stopFlag == 0:
#         print(arcno.MakeTableView_management(self=arcno,in_table=table))


"""
loop
"""
for file in os.listdir(dirDIMAs):
    if file.endswith(".mdb"):
        listDIMAs.append(dirDIMAs + "\\" + file)

for inDIMA in listDIMAs:
    # if one DIMA fails go on to the next one
    stopFlag = 0
    print ("\nStarting DIMA - " + inDIMA)
    print ("   Starting Dups check")

    for table in tableList:
        if stopFlag == 0:
            from arcnah import arcno
            arcno = arcno()
            arcno.MakeTableView_management(table)
            tempFL = arcno.temp
            arcno.SelectLayerByAttribute_management (tempFL, "RecKey")
            tempFL = arcno.temp
            dimaCount =  arcno.GetCount_management()
            if dimaCount > 0:
                print("      " + table + " - ERROR Records with the same or similar DBKey found!")
                print("   ** Skipping DIMA " + inDIMA + " - failed Dups check! **")
                stopFlag = 1
                tableErrors.append("Table " + table + " in " + inDIMA + " failed on import due to Dups in DBKeys.")
                continue
            else:
                print("      " + table + " - No Dups found")
            arcno.MakeTableView_management(table)
            tempFL = arcno.temp
            if not table in appendTables:
                tempfield = tableList[table]
                arcno.MakeTableView_management(table)
                temp_table = pd.merge(tempFL,arcno.temp,left_on=tempfield,right_on=tempfield)
                print(temp_table.shape)
                arcno.SelectLayerByAttribute_management(temp_table, tempfield,)

                # arcno.AddJoin_management(tempFL,
                # tableList[table], table,
                # tableList[table])
                # arcno.temp_table





                arcpy.SelectLayerByAttribute_management ("tempFL", "NEW_SELECTION", table + "." + tableList[table] + " <> '999999999' And " + table + "." + tableList[table] + " <> '888888888'")
                joinedCount = int(arcpy.GetCount_management("tempFL").getOutput(0))
                if joinedCount > 0:
                    print "      " + table + " - ERROR Records with the same Keys " + tableList[table] + " as DIMA found!"
                    print "   ** Skipping DIMA " + inDIMA + " - failed Dups check! **"
                    stopFlag = 1
                    tableErrors.append("Table " + table + " in " + inDIMA + " has Dups in Key field.  DIMA was skipped!")
                    continue
