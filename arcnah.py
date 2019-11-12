import pandas as pd
from os import listdir,getcwd, chdir
from os.path import normpath, join
from methods.make_table import Table
# replacing ap's gdb methods with pandas alternatives
# if arcpy creates a temporary view in gdb, arcno creates dataframe within its temp class attribute
##
# if arcpy selects certain columns from temp view, arcno selects certain columns from df and returns them
# if arcpy counts rows of temp view filtered with select, arcno counts rows of dataframe filtered through if statement dependent on a methods argument

# iris = pd.read_csv(r'C:\Users\kbonefont.JER-PC-CLIMATE4\Desktop\iris.csv')
#
# set(pd.Series(iris.columns))
#
# class df_sets:
#     _df = None
#     _cols = None
#     def __init__(self,input):
#         self.input = input
#         self._cols = set(pd.Series(input.columns))
#


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
# path = normpath(r"C:\Users\kbonefont\Desktop\Some_data\NM_TaosFO_LUP_2018_5-3b_01.mdb")
#
#
# arcno.MakeTableView_management('tblLines',path)
# arcno.temp
# arcno.SelectLayerByAttribute_management(arcno.temp,'1808051039437030','1808050843012839',field='LineKey')
# arcno.temp_table


class arcNo():
    temp = None
    string = None
    uniq = None
    frames = None
    single_field = None
    exist=None
    temp_join=None
    how = None
    temp2 = None
    temp_table = None

    t1 = None
    t2 = None
    # in_table = None
    def __init__(self, in_table = None, in_df = None, right_on=None, left_on = None, whichdima = None):
        self.in_table = in_table
        self.in_df = in_df
        self.right_on = right_on
        self.left_on = left_on
        self.whichdima = whichdima
    def __call__(self):
        return arcno.__call__()


    def MakeTableView_management(self,in_table,whichdima):
        """ connects to ms access mdb, selects a table
        and copies it to a df
        """
        self.in_table = in_table
        self.whichdima = whichdima

        try:
            self.temp = Table(self.in_table, self.whichdima).temp
        except Exception as e:
            print(e)

    def SelectLayerByAttribute_management(self, in_df,*vals, field = None):

        import pandas as pd
        self.in_df = in_df
        self.field = field
        self.vals = vals

        dfset = []
        def name(arg1,arg2):
            self.arg1 = arg1
            self.arg2 = arg2
            import os
            joined= os.path.join(self.arg1+self.arg2)
            return joined

        if all(self.in_df):
            print("dataframe exists")
            try:
                for val in self.vals:
                    index = self.in_df[f'{self.field}']==f'{val}'
                    exec("%s = self.in_df[index]" % name(f'{self.field}',f'{val}'))
                    dfset.append(eval(name(f'{self.field}',f'{val}')))

                self.temp_table = pd.concat(dfset)
            except Exception as e:
                print(e)
        else:
            print("error")
    def GetCount_management(self,in_df):
        self.in_df = in_df
        return self.in_df.shape[0]

    def AddJoin_management(self,
    in_df,df2,right_on=None,left_on=None):
        """ inner join on two dataframes on 1 or 2 fields

        """
        import pandas as pd
        # self.temp_table = None
        d={}

        d[self.right_on] = right_on
        d[self.left_on] = left_on

        self.in_df = in_df
        self.df2 = df2

        if self.right_on==self.left_on and len(self.in_df.columns)==len(self.df2.columns):
            try:
                frames = [self.in_df, self.df2]
                self.temp_table = pd.concat(frames)
            except Exception as e:
                print(e)
                print('1. field or fields invalid' )
        elif self.right_on==self.left_on and len(self.in_df.columns)!=len(self.df2.columns):
            try:
                # frames = [self.in_df, self.df2]
                self.temp_table = self.in_df.merge(self.df2, on = d[self.right_on], how='inner')
            except Exception as e:
                print(e)
                print('2. field or fields invalid')
        else:
            try:
                self.temp_table=self.in_df.merge(self.df2,right_on=d[self.right_on], left_on=d[self.left_on])
            except Exception as e:
                print(e)
                print('3. field or fields invalid')

    def AddField_management(self, in_df, newfield):
        """ adds empty field within 'in_df' with fieldname
        supplied in the argument
        """
        self.in_df = in_df
        self.newfield = newfield

        self.in_df[f'{self.newfield}'] = pd.Series()

    def RemoveJoin_management(self):
        """ creates deep copy of original dataset
        and joins any new fields product of previous
        right hand join
        """
        pass

    def MakeFeatureLayer_management(self):
        """ may be similar to screate table view
        """
        pass

    def GetParameterAsText(self,string):
        """ return stringified element

        """

        self.string = f'{string}'
        return self.string



"""
Starting DIMA - C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb
   Starting Dups check
used query:SELECT * FROM "tblGapDetail"
df detected
starting try..
field not found
use SelectLayerByAttribute first
      tblGapDetail - No Dups found"""
