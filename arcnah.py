import pandas as pd


# replacing ap's gdb methods with pandas alternatives
# if arcpy creates a temporary view in gdb, arcno creates dataframe within its temp class attribute
##
# if arcpy selects certain columns from temp view, arcno selects certain columns from df and returns them
# if arcpy counts rows of temp view filtered with select, arcno counts rows of dataframe filtered through if statement dependent on a methods argument
import pyodbc
from temp_tools import msconfig
# iris = pd.read_csv(r'C:\Users\kbonefont.JER-PC-CLIMATE4\Desktop\iris.csv')

#
# param = msconfig()
# con = pyodbc.connect(**param)
# cur = con.cursor()
# full_tbl_list = []
# for tbl in cur.tables():
#     if "tbl" in tbl[2]:
#         cur1 = con.cursor()
#         if tbl.table_type == "TABLE":
#             full_tbl_list += [tbl.table_name,]
#
type(4)==str

class arcno():
    temp = None
    string = None
    uniq = None
    single_field = None
    exist=None
    temp_join=None
    how = None
    # in_table = None
    def __init__(self, in_table = None, in_df = None):
        self.in_table = in_table
        self.in_df = in_df

    def MakeTableView_management(self,in_table): # <= create df from an ms table
        import pyodbc
        self.in_table = in_table
        from temp_tools import msconfig
        param = msconfig()
        con = pyodbc.connect(**param)
        cur = con.cursor()
        query = 'SELECT * FROM "{table}"'.format(table=self.in_table)
        print("used query:"+query)
        self.temp = pd.read_sql(query,con)

    def SelectLayerByAttribute_management(self, in_df, field = None, val=None, op = None):
        self.in_df = in_df
        self.field = field
        self.val = val

        if op==None:
            # if there's a value to look up, index by it
            if val != None and type(val) == str:
                index= self.in_df[f'{self.field}'].str.contains(f'{self.val}$')
                self.temp= self.temp[index]
                self.exist=True

            elif val != None and type(val) == int:
                # self.val
                index= self.in_df[f'{self.field}']==f'{self.val}'
                self.temp= self.in_df[index]
                self.exist=True

            # else if theres not value to look up and field does not exist within     table, return warning
            elif val == None and self.field not in self.in_df.columns:
                print('field does not exist')
                self.exist=False

            # else if there's no value to look up, print unique values in supplied     field
            else:
                exec(f'self.uniq = self.temp.{self.field}.unique()')
                #self.uniq = pd.DataFrame(self.uniq)
                # print(self.uniq)
                # print("this attr.     has",len(self.temp.f'{self.field}'.unique()),"unique values, please     specify")
                self.exist=False
        elif op=="<>":
            if val != None and type(val) == str:
                index = self.in_df[f'{self.field}'].str.contains(f'{self.val}$')
                self.temp = self.temp[~index]
                self.exist = True
            elif val != None and type(val) == int:
                # self.val
                index = self.in_df[f'{self.field}']==f'{self.val}'
                self.temp = self.in_df[~index]
                self.exist = True

    def GetCount_management(self):
        if self.exist==False:

            return int(0)
        elif self.exist==False and self.uniq!=None:

            return len(self.temp)
        elif self.exist==True:

            return len(self.temp)
        elif self.exist==None:
            print("use SelectLayerByAttribute first")
            return(len(self.temp))

    def AddJoin_management(self, in_df,jointable,**kwargs):
        options = {
                'op1': None,
                'op2': None,}

        self.in_df = in_df
        self.field = kwargs.get('op1')
        self.jointable = jointable
        self.join_field = kwargs.get('op2')

        options.update(kwargs)
        self.temp_table=pd.merge(self.in_df,self.jointable, left_on=self.field, right_on=self.join_field)


        # self.temp_table = [self.in_df,self.field]




        # self.how = how ### this could be used to choose type of merge but unneeded rn

        # if self.field == self.join_field:
        #     self.temp_table = pd.merge(self.in_df,
        #     self.jointable,on=self.field,how='outer')
        #
        # if self.field != self.join_field:
        #     self.temp_table = pd.merge(self.in_df,
        #     self.jointable, left_on=self.field,right_on=self.join_field)

        # # second_df=
        #




# arcno.AddJoin_management(self=arcno,in_df=i1,field='species',jointable=i2,join_field='sp2')
# arcno.AddJoin_management(self=arcno,in_df=i1,jointable=i2,op1='species',op2='sp2')
# arcno.temp_table
# arcno.temp_table

# arcno = arcno()
# arcno.MakeTableView_management('tblGapDetail')
# df1=arcno.temp
# df1


# arcno.SelectLayerByAttribute_management(df1,'RecKey')
# arcno.single_field
# arcno.temp
# arcno.GetCount_management()
# for table in tableList:
#     arcno.MakeTableView_management(table)
# class pr():
#     word = None
#     # def __init__(self, word=None):
#     #     self.word = word
#     # def get_string(self):
#     #     self.word = input()
#
#     def inter(self,word):
#         self.word = word
#         print(self.word)
#
# pr = pr()
# pr.inter('aqw')
#
# pr.get_string()
# pr.inter()
# import pandas as pd
# ir = pd.read_csv(r'C:\Users\kbonefont.JER-PC-CLIMATE4\Desktop\iris.csv')
#
# ir.columns
# pr
# irr = ir['species'].str.contains('osa$')
# ir[irr]
#
# ir['species'].str.contains('osa$')



# class iris():
#     df = None
#     word=None
#     val = None
#
#     def ind(self,word,val = None):
#         self.word = word
#         self.val = val
#         if self.val != None:
#             index = self.df["{word}".format(word=self.word)] =='{val}'.format(val=self.val)
#             self.df = self.df[index]
#
#         else:
#             pass
#
# iris = iris()
# iris.ind()
# #
# ind1=ir['species']=='setosa'
# i1=iris[ind1][0:15]
# i2=iris[ind1][16:30]
# #
# # iris_s=iris[ind1]
# # i1=iris_s[0:15]
# # i2=iris_s[16:30]
# #
# # i2.columns
# i2.rename(columns={'species':'sp2'},inplace=True)

# iris = iris()
# iris.df = ir
# iris.ind('species','setosa')
# iris.df
# ir[species == 'setosa']
