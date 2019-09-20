import pandas as pd


# replacing arcpys gdb methods with pandas alternatives
# if arcpy creates a temporary view in gdb, arcno creates dataframe within its temp class attribute
# if arcpy selects certain columns from temp view, arcno selects certain columns from df and returns them
# if arcpy counts rows of temp view filtered with select, arcno counts rows of dataframe filtered through if statement dependent on a methods argument

class arcno():
    temp = None
    string = None
    # in_table = None
    def __init__(self, in_table = None, in_df = None):
        self.in_table = in_table
        self.in_df = in_df

    def MakeTableView_management(self,in_table):
        import pyodbc
        # self.in_table = in_table
        self.in_table = in_table
        from temp_tools import msconfig
        param = msconfig()
        con = pyodbc.connect(**param)
        cur = con.cursor()
        query = 'SELECT * FROM db."{table}"'.format(table=self.in_table)
        print(query)
        self.temp = pd.read_sql(query,con)

    def SelectLayerByAttribute_management(self, in_df, string):
        self.in_df = in_df
        self.string = "{str}".format(str=string)
        # print(in_df)
        print(self.string)
        for cols in self.in_df.columns:
            if self.string in cols:
                self.temp = cols
                print(cols)

    def GetCount_management(self,in_df):
        print(len(self.in_df))


cur
tbl_list = []
for tbl in cur.tables():
    if "tbl" in tbl[2]:
        cur1 = con.cursor()
        if tbl.table_type == "TABLE":
            tbl_list += [tbl.table_name,]

tableFile = r"C:\Users\kbonefont.JER-PC-CLIMATE4\Downloads\py_txt\inputtablesForNewSchema - Copy.txt"
tableRead = open(tableFile,"r")
lineList = tableRead.readlines()
tableList = {}
for line in lineList:
    tableList.update({line.split("|")[0] : line.split("|")[1].split(",")[0]})
arcno = arcno()
arcno.MakeTableView_management('tblGapDetail')
df1=arcno.temp
df1.columns


arcno.SelectLayerByAttribute_management(df1,'RecKey')
arcno.temp
arcno.GetCount_management(arcno.temp)
for table in tableList:
    arcno.MakeTableView_management(table)
class pr():
    word = None
    # def __init__(self, word=None):
    #     self.word = word
    # def get_string(self):
    #     self.word = input()

    def inter(self,word):
        self.word = word
        print(self.word)

pr = pr()
pr.inter('aqw')

pr.get_string()
pr.inter()
