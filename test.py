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
# 1807121252394813
#
from arcnah import arcno
arcno = arcno()
# ind=[1807121252394813,180712125342201]
arcno.MakeTableView_management('tblGapDetail')
df1 = pd.DataFrame({'employee': ['Bob', 'Jake', 'Lisa', 'Sue'],
                    'group': ['Accounting', 'Engineering', 'Engineering', 'HR']})
df2 = pd.DataFrame({'employee': ['Lisa', 'Bob', 'Jake', 'Sue'],
                    'hire_date': [2004, 2008, 2012, 2014]})
display('df1', 'df2')
df = arcno.temp
dff = arcno.temp
df1 = df[0:9]
df2 = df[10:21]
pd.concat([df1,df2])
number =180712125342201
print(df['RecKey']==f'{number}')
arcno.GetCount_management()
arcno.AddJoin_management(df3,df4,'group','group')
df3 = arcno.temp_table

df4 = pd.DataFrame({'group': ['Accounting', 'Engineering', 'HR'],
                    'supervisor': ['Carly', 'Guido', 'Steve']})


# arcno.temp_table
# arcno.SelectLayerByAttribute_management(arcno.temp, "RecKey",1807121252394813 and 180712125342201)
# for val in arcno.temp:
#
# ind in arcno.temp['RecKey']
# [arcno.temp[i] for i in ind]
# [ind not in row[1] for row in arcno.temp]
# arcno.uniq
# arcno.temp.shape[0]
# arcno.GetCount_management()
# for table in tableList:
#     print(table)
#     if stopFlag == 0:
#         print(arcno.MakeTableView_management(self=arcno,in_table=table))

for i in ind:
    arcno.temp[i]
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
            # tempFL = arcno.temp
            if not table in appendTables:
                # tempfield = tableList[table]
                # arcno.MakeTableView_management(table)
                # temp_table = pd.merge(tempFL,arcno.temp,left_on=tempfield,right_on=tempfield)
                arcno.AddJoin_management(tempFL,arcno.temp,left_on=tableList[table],right_on=tableList[table])

                print(arcno.temp_table.shape)
                arcno.
                if temp_table.shape[0]!=0:
                    arcno.SelectLayerByAttribute_management(temp_table, tempfield,999999999,op='<>')
                    tmp1 = arcno.temp
                    print(tmp1.shape)
                    joinedCount =
                arcno.SelectLayerByAttribute_management(temp_table, tempfield,888888888,op='<>')
                tmp2 = arcno.temp
                try:
                    tmp1_tmp2 = pd.merge(tmp1,tmp2,on=tempfield,how='inner')
                    arcno.temp = tmp1_tmp2
                    arcno.GetCount_management()



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
