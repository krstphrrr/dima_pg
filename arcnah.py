import pandas as pd


# replacing ap's gdb methods with pandas alternatives
# if arcpy creates a temporary view in gdb, arcno creates dataframe within its temp class attribute
##
# if arcpy selects certain columns from temp view, arcno selects certain columns from df and returns them
# if arcpy counts rows of temp view filtered with select, arcno counts rows of dataframe filtered through if statement dependent on a methods argument
import pyodbc
from temp_tools import msconfig


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

    def MakeTableView_management(self,in_table,whichdima):
        """ connects to ms access mdb, selects a table
        and copies it to a df
        """
        import pyodbc, pandas as pd
        self.in_table = in_table
        self.whichdima = whichdima

        try:
            MDB = self.whichdima
            DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
            mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)

            con = pyodbc.connect(mdb_string)
            cur = con.cursor()
            query = 'SELECT * FROM "{table}"'.format(table=self.in_table)
            print("2. used query:"+query)
            self.temp = pd.read_sql(query,con)
        except Exception as e:
            print(e)

    def SelectLayerByAttribute_management(self,
    in_df, field = None, val1=None, val2 = None,
    op = None):
        """ creates a table indexed by supplied field and
        1 or 2 attributes for that field.
        """
        import pandas as pd
        self.in_df = in_df
        self.field = field
        self.val1 = val1
        self.val2 = val2

        if all(self.in_df):
            print("3. df detected")
            try:
                print("4. starting try..")
                if self.field in self.in_df.columns:
                    print("5. field found in df columns")
                    # if there's a value to look up, index by it
                    if val1 is not None and type(val1) == str and val2 is None:

                        if op is not None:

                            print("6. val1 = 1, val2 = 0, op = 1, type = str")
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.temp_table = self.in_df[~index]
                            self.exist=True

                        else:
                            print("6. val1 = 1, val2 = 0, op = 0, type = str")
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.temp_table = self.in_df[index]
                            self.exist=True

                    # if there's a val2, join to df indexed by val1 (first filter)
                    elif val1 is not None and type(val1) == str and val2 is not None:
                        if op is not None:
                            print("6. val1 = 1, val2 = 1, op = 1, val1 = str")
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.t1= self.in_df[~index]
                            index2 = self.in_df[f'{self.field}'].str.contains(f'{self.val2}')
                            self.t2= self.in_df[~index2]
                            frames = [self.t1, self.t2]
                            self.temp_table = pd.concat(frames) # <- join     of filtered df's
                            self.exist=True

                        else:
                            print("6. val1 = 1, val2 = 1, op = 0, val1 = str")
                            index= self.in_df[f'{self.field}'].str.contains(f'{self.val1}')
                            self.t1= self.in_df[index]
                            index2 = self.in_df[f'{self.field}'].str.contains(f'{self.val2}')
                            self.t2 = self.in_df[index2]
                            frames = [self.t1, self.t2]
                            self.temp_table = pd.concat(frames)
                            self.exist=True

                    elif val1 is not None and type(val1) == int and val2 is None:
                        if op is not None:
                            print("6. val1 = 1, val2 = 0, op = 1, type = int")
                            index= self.in_df[f'{self.field}']==self.val1
                            self.temp_table = self.in_df[~index]
                            self.exist=True

                        else:
                            print("6. val1 = 1, val2 = 0, op = 0, type = int")
                            index= self.in_df[f'{self.field}']==f'{self.val1}'
                            self.temp_table = self.in_df[index]
                            self.exist=True

                    elif val1 is not None and type(val1) == int and val2 is not None:
                         if op is not None:
                             print("6. val1 = 1, val2 = 1, op = 1, type = int")
                             index = self.in_df[f'{self.field}']==self.val1
                             self.t1 = self.in_df[~index]
                             index2 = self.in_df[f'{self.field}']==self.val2
                             self.t2 = self.in_df[~index2]
                             frames = [self.t1, self.t2]
                             self.temp_table = pd.concat(frames)
                             self.exist=True

                         else:
                             print("6. val1 = 1, val2 = 1, op = 0, type = int")
                             index = self.in_df[f'{self.field}']==self.val1
                             self.t1 = self.in_df[index]
                             index2 = self.in_df[f'{self.field}']==self.val2
                             self.t2 = self.in_df[index2]
                             frames = [self.t1, self.t2]
                             self.temp_table = pd.concat(frames)
                             self.exist=True

                # else if there's no value to look up, print unique values in supplied     field
                    else:
                        print('5. field exists; input 1 or 2 values to index table by')
                        exec(f'self.uniq = self.temp.{self.field}.unique()')
                        self.exist=False

            # else if theres not value to look up and field does not exist
            # within table, return warning
                else:
                    print("5. field not found, returning empty table")
                    self.temp_table = pd.concat([pd.DataFrame({k:[]for k in self.temp.columns}), None, None])



            except Exception as e:
                print(e)
                self.exist=False
        else:
            print('3. input df, cannot create table')

    def GetCount_management(self, in_df):  # needs work
        """ counts rows of supplied object?
        or count rows of internal table (class attribute?)
        """
        try:
            if self.exist is not None:
                return self.in_df.shape[0]
        # if self.exist==False:
        #
        #     return int(0)
        # elif self.exist==False and self.uniq!=None:
        #
        #     return len(self.temp)
        # elif self.exist==True:
        #
        #     return len(self.temp)
            elif self.exist is None:
                print("use SelectLayerByAttribute first")
                return(len(self.temp))
        except Exception as e:
            print(e)

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
