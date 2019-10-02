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


class arcno():
    temp = None
    string = None
    uniq = None
    frames = None
    single_field = None
    exist=None
    temp_join=None
    how = None
    temp2 = None

    t1 = None
    t2 = None
    # in_table = None
    def __init__(self, in_table = None, in_df = None, right_on=None, left_on = None):
        self.in_table = in_table
        self.in_df = in_df
        self.right_on = right_on
        self.left_on = left_on

    def MakeTableView_management(self,in_table): # <= create df from ms table , make table view
        import pyodbc
        self.in_table = in_table
        from temp_tools import msconfig
        param = msconfig()
        con = pyodbc.connect(**param)
        cur = con.cursor()
        query = 'SELECT * FROM "{table}"'.format(table=self.in_table)
        print("used query:"+query)
        self.temp = pd.read_sql(query,con)

    def SelectLayerByAttribute_management(self,
    in_df, field = None, val1=None, val2 = None,
    op = None):
    """ creates a table indexed by supplied field and
    1 or 2 attributes for that field.
    """
    # <= select layer by att
        import pandas as pd
        self.in_df = in_df
        self.field = field
        self.val1 = val1
        self.val2 = val2

        if all(self.in_df):
            try:
                if self.field in self.in_df.columns:
                # if there's a value to look up, index by it
                    if val1 is not None and type(val1) == str and val2 is None:
                        if op is not None:
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.temp= self.in_df[~index]
                            self.exist=True

                        else:
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.temp= self.in_df[index]
                            self.exist=True

                # if there's a val2, join to df indexed by val1 (first filter)


                    elif val1 is not None and type(val1) == str and val2 is not None:
                        if op is not None:
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.t1= self.in_df[~index]
                            index2 = self.in_df[f'{self.field}'].str.contains(f'{self.val2}')
                            self.t2= self.in_df[~index2]
                            frames = [self.t1, self.t2]
                            self.temp = pd.concat(frames) # <- join     of filtered df's
                            self.exist=True

                        else:
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.t1= self.in_df[index]
                            index2 = self.in_df[f'{self.field}'].str.contains(f'{self.val2}')
                            self.t2 = self.in_df[index2]
                            frames = [self.t1, self.t2]
                            self.temp = pd.concat(frames)
                            self.exist=True

                    elif val1 is not None and type(val1) == int and val2 is None:
                        if op is not None:
                            index= self.in_df[f'{self.field}']==f'{self.val1}'
                            self.temp= self.in_df[~index]
                            self.exist=True

                        else:
                            index= self.in_df[f'{self.field}']==f'{self.val1}'
                            self.temp= self.in_df[index]
                            self.exist=True

                    elif val1 is not None and type(val1) == int and val2 is not None:
                         if op is not None:
                             index = self.in_df[f'{self.field}']==f'{self.val1}'
                             self.t1 = self.in_df[~index]
                             index2 = self.in_df[f'{self.field}']==f'{self.val2}'
                             self.t2 = self.in_df[~index2]
                             frames = [self.t1, self.t2]
                             self.temp = pd.concat(frames)
                             self.exist=True

                         else:
                             index = self.in_df[f'{self.field}']==f'{self.val1}'
                             self.t1 = self.in_df[index]
                             index2 = self.in_df[f'{self.field}']==f'{self.val2}'
                             self.t2 = self.in_df[index2]
                             frames = [self.t1, self.t2]
                             self.temp = pd.concat(frames)
                             self.exist=True

                # else if there's no value to look up, print unique values in supplied     field
                elif val1 is None:
                        exec(f'self.uniq = self.temp.{self.field}.unique()')
                        self.exist=False

            # else if theres not value to look up and field does not exist
            # within table, return warning
            except:
                print('field does not exist')
                self.exist=False
        else:
            print('input df')

    def GetCount_management(self):  # needs work
    """ counts rows of supplied object?
    or count rows of internal table (class attribute?)
    """


        if self.exist==False:

            return int(0)
        elif self.exist==False and self.uniq!=None:

            return len(self.temp)
        elif self.exist==True:

            return len(self.temp)
        elif self.exist==None:
            print("use SelectLayerByAttribute first")
            return(len(self.temp))

    def AddJoin_management(self,
    in_df,df2,right_on=None,left_on=None):
        import pandas as pd
        d={}

        d[self.right_on] = right_on
        d[self.left_on] = left_on

        self.in_df = in_df
        self.df2 = df2
        if self.right_on==self.left_on:
            try:
                self.temp_table = self.in_df.merge(self.df2, on = d[self.right_on])
            except:
                print('field or fields invalid')

        else:
            try:
                self.temp_table=self.in_df.merge(self.df2,right_on=d[self.right_on], left_on=d[self.left_on])
            except:
                print('field or fields invalid')

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
        """ may be similar to create table view
        """


arcno = arcno()
arcno.MakeTableView_management('tblGapHeader')
gapheader=arcno.temp.copy(deep=True)

arcno.MakeTableView_management('tblLines')
tbllines = arcno.temp.copy(deep=True)

arcno.AddJoin_management(gapheader,tbllines, left_on="LineKey", right_on="LineKey")
arcno.temp_table
gapheaderview = arcno.temp_table.copy(deep=True)

arcno.AddField_management(gapheaderview,'PlotID')


df = arcno.temp.copy(deep=True)

df.columns


arcno.AddField_management(df,'something')
df['something']
