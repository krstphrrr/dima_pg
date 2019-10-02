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
import pandas as pd
from arcnah import arcno
arcno = arcno()
# ind=[1807121252394813,180712125342201]
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
arcno.AddJoin_management(df1,df2,'groups','')
arcno.temp_table


df3 = arcno.temp_table

df4 = pd.DataFrame({'group': ['Accounting', 'Engineering', 'HR'],
                    'supervisor': ['Carly', 'Guido', 'Steve']})

from arcnah import arcno
arcno = arcno()
arcno.MakeTableView_management('tblGapDetail')
df = arcno.temp
type(arcno.temp)
arcno.temp.columns
arcno.temp
arcno.temp2
arcno.SelectLayerByAttribute_management(df, "RecKey", 1807121252394813, 180712125342201)
arcno.temp.shape[0]
arcno.temp
arcno.

arcno.uniq
fulldf = arcno.temp

# 1807121252394813
firstdf = arcno.temp

# 180712125342201

seconddf = arcno.temp

index1 = fulldf['RecKey'].str.contains('1807121252394813')
temp = fulldf[index1]

index2 = fulldf['RecKey'].str.contains('180712125342201')
temp2 = fulldf[index2]

import pandas as pd
frames = [temp,temp2]
temp = pd.concat(frames)







str = f'{os.path.basename(listDIMAs[1])[:-6]}'
str





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
            str = f'{os.path.basename(listDIMAs[1])[:-6]}'
            tempFL = arcno.temp
            arcno.SelectLayerByAttribute_management (arcno.temp, "RecKey", 1807121252394813)
            print("Diff: ",tempFL.shape[0]-arcno.temp.shape[0])
            dimaCount =  arcno.GetCount_management()
            print(dimaCount)
            if dimaCount > 0:
                print("      " + table + " - ERROR Records with the same or similar DBKey found!")
                print("   ** Skipping DIMA " + inDIMA + " - failed Dups check! **")
                stopFlag = 1
                tableErrors.append("Table " + table + " in " + inDIMA + " failed on import due to Dups in DBKeys.")
                continue
            else:
                print("      " + table + " - No Dups found")
            tempFL = arcno.MakeTableView_management(table)
            # tempFL = arcno.temp
            if not table in appendTables:

                arcno.AddJoin_management(tempFL,arcno.temp,left_on=tableList[table],right_on=tableList[table])

                print(arcno.temp_table.shape) # first join tempfl + temp

                if arcno.temp_table.shape[0]!=0:
                    arcno.SelectLayerByAttribute_management(arcno.temp_table, tableList[table],999999999,888888888)

                    print(arcno.temp.shape)
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
