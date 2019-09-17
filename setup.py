import os, time, pandas as pd

import csv, pyodbc




# set up some constants
MDB = 'C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb'
DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

# connect to db
con = pyodbc.connect(r"DRIVER={};DBQ={};".format(DRV,MDB))
con = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb;')
cur = con.cursor()
con.setencoding(encoding='utf-8')





cur = con.cursor()


print(cur.columns(new_tbllist[1]))

def get_cols():
    col_count = {}
    for table in []

cur.columns(table=tbls[0])



for row in cur.tables():
    print(row.table_name)


row.table_name






new_tbllist=[]
for tbl in list(cur.tables()):
    if "tbl" in tbl[2]:
        if tbl.table_type == "TABLE":
            new_tbllist += [tbl.table_name,]

for tbl in cur.tables():
    if "tbl" in tbl[2]:
        for row in cur.columns(table=tbl):
            print(row.column_name)




for tbl in cur.tables():
    print(tbl.table_name)

if cur.tables(table='tblTempPlots').fetchone():
    print('mhm')
new_list = []

def tbl_(table):
    tbl = "{}".format(table)
    for row in cur.columns(table=tbl):
        # new_list.append(row.column_name)
        print(row.column_name)

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


# def create_tables(self):
#
#     # Generate list of tables in schema
#     table_list = list()
#     for table in self.access_cur.tables():
#         if table.table_type == "TABLE":
#             table_list += [table.table_name, ]
#
#     for table in table_list:
#         SQL = """
#         CREATE TABLE "{schema}"."{table}"
#         (
#         """.format(schema=self.schema_name, table=table)
#
#         SQL += self.create_fields(table)
#
#         SQL += """
#         ) """
#
#         if self.print_SQL:
#             print(SQL)
#
#         self.pg_cur.execute(SQL)
#         self.pg_con.commit()
#
# def create_fields(self, table):
#
#     postgresql_fields = {
#         'COUNTER': 'serial',  # autoincrement
#         'VARCHAR': 'text',  # text
#         'LONGCHAR': 'text',  # memo
#         'BYTE': 'integer',  # byte
#         'SMALLINT': 'integer',  # integer
#         'INTEGER': 'bigint',  # long integer
#         'REAL': 'real',  # single
#         'DOUBLE': 'double precision',  # double
#         'DATETIME': 'timestamp',  # date/time
#         'CURRENCY': 'money',  # currency
#         'BIT':  'boolean',  # yes/no
#     }
#
#     SQL = ""
#     field_list = list()
#     for column in self.access_cur.columns(table=table):
#         if column.type_name in postgresql_fields:
#             field_list += ['"' + column.column_name + '"' +
#                            " " + postgresql_fields[column.type_name], ]
#         elif column.type_name == "DECIMAL":
#             field_list += ['"' + column.column_name + '"' +
#                            " numeric(" + str(column.column_size) + "," +
#                            str(column.decimal_digits) + ")", ]
#         else:
#             print("column " + table + "." + column.column_name +
#             " has uncatered for type: " + column.type_name)
#
#     return ",\n ".join(field_list)
