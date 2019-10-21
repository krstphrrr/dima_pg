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
arcno.MakeTableView_management('tblLines','C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\NM_TaosFO_LUP_2018_5-3b_01.mdb')

any(arcno.temp.where(arcno.temp.iloc[2]==888888888))
arcno.SelectLayerByAttribute_management(arcno.temp,'LineKey',999999999,888888888)

for row in range(0,arcno.temp.shape[0]):
    print(row)

for i,row in arcno.temp.iteritems():
    print(i,row)

import numpy as np
np.where(arcno.temp.applymap(lambda x:x==999999999))
arcno.temp[( arcno.temp['LineKey']=='999999999')|(arcno.temp['LineKey']=='888888888')]

arcno.temp.iterrows
for i in arcno.temp.iterrows:
    print(i)

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
