import pandas as pd
import os
from arcnah import arcno

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

from arcnah import arcno
arcno = arcno()
arcno.MakeTableView_management('tblLines','C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\DIMA 5.2 as of 2017-07-18.mdb')

arcno.temp
str = f'{os.path.basename(listDIMAs[1])[:-6]}'
fulldf = arcno.temp.copy(deep=True)

'ok' in fulldf['RecKey']
'ok' in fulldf.RecKey
arcno.SelectLayerByAttribute_management(fulldf, 'RecKey',4)
arcno.temp.RecKey
arcno.SelectLayerByAttribute_management(arcno.temp, 'RecKey',1707260956495950)
print(arcno.uniq)
spgen = arcno.temp.copy(deep=True)
spgen.columns

for file in os.listdir(dirDIMAs):
    if file.endswith(".mdb"):
        listDIMAs.append(dirDIMAs + "\\" + file)
mdbs = listDIMAs

arcno.temp
from arcnah import arcno
arcno = arcno()
arcno.MakeTableView_management('tblGapHeader', mdbs[1])
df1 = arcno.temp.copy(deep=True)
arcno.MakeTableView_management('tblGapDetail', mdbs[1])
df2 = arcno.temp.copy(deep=True)

df1.shape # 28 rows, 41 cols

df2.shape


arcno.AddJoin_management(df1, df2, left_on = 'RecKey', right_on = 'RecKey')
arcno.temp_table





arcno.MakeTableView_management('tblGapDetail', mdbs[2])
df3 = arcno.temp.copy(deep=True)
arcno.MakeTableView_management('tblPlotNotes', mdbs[3])
df4 = arcno.temp.copy(deep=True)
arcno.SelectLayerByAttribute_management(arcno.temp, 'DBKey')
df1.columns
df2.columns
tempFL = arcno.temp.copy(deep=True)


for each in df3.RecKey:
    print(each in df2.RecKey)
[print(each in df2.RecKey) for each in df3.RecKey if each is not None]



arcno.temp_table.shape

arcno.SelectLayerByAttribute_management(arcno.temp_table, 'RecKey')
arcno.uniq

arcno.temp




df = arcno.temp.copy(deep=True)
import pandas as pd
df2 = pd.concat([pd.DataFrame({k:[] for k in df.columns}), None, None])
df2.shape




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
    print ("\n1. Starting DIMA - " + inDIMA)
    print ("   Starting Dups check")

    for table in tableList:
        if stopFlag == 0:
            from arcnah import arcno
            import os
            arcno = arcno()
            arcno.MakeTableView_management(table,inDIMA)

            str = f'{os.path.basename(listDIMAs[1])[:-6]}'
            tempFL = arcno.temp.copy(deep=True)
            arcno.SelectLayerByAttribute_management(tempFL, "DBKey", str)
            # print("Diff: ",tempFL.shape[0]-arcno.temp.shape[0])


            dimaCount =  arcno.GetCount_management(arcno.temp)


            if dimaCount > 0:
                print("      " + table + " - ERROR Records with the same or similar DBKey found!")
                print("   ** Skipping DIMA " + inDIMA + " - failed Dups check! **")
                stopFlag = 1
                tableErrors.append("Table " + table + " in " + inDIMA + " failed on import due to Dups in DBKeys.")
                continue
            else:
                print("      " + table + " - No Dups found")

            # tempFL = arcno.temp
            if not table in appendTables:
                for dims in listDIMAs:
                    arcno.MakeTableView_management(table,dims)
                    print("\n**** CHECK PER TABLE, SIZE:",arcno.temp.shape,"****\n")
                    print("**** TEMPFL's SIZE:",tempFL.shape,"****\n")

                    arcno.AddJoin_management(tempFL,arcno.temp,left_on=tableList[table],right_on=tableList[table])

                    print("! FINAL JOIN SIZE:",arcno.temp_table.shape,"! \n \n") # first join tempfl + temp

                if arcno.temp_table.shape[0]!=0:
                    arcno.SelectLayerByAttribute_management(arcno.temp_table, tableList[table],999999999,888888888)

                    print("REAL FINAL SIZE:",arcno.temp.shape,"\n \n \n \n")
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
