import pandas as pd
from os import listdir,getcwd, chdir
from os.path import normpath, join
from methods.make_table import Table
from utils import Acc


"""
 replacing ap's gdb methods with pandas alternatives
 - if ap creates a temporary view in gdb, arcno creates dataframe within its
   temp class attribute
 - if ap selects certain columns from temp view, arcno selects certain columns
   from df and returns them
 - if ap counts rows of temp view filtered with select, arcno counts rows of
   dataframe filtered through if statement dependent on a methods argument
"""


class arcno():
    tablelist = []
    isolates = None

    def __init__(self, whichdima = None):
        """ Initializes a list of tables in dima accessible on tablelist.
        ex.
        arc = arcno(path_to_dima)
        arc.tablelist
        """
        self.whichdima = whichdima
        if self.whichdima is not None:
            cursor = Acc(self.whichdima).con.cursor()
            for t in cursor.tables():
                if t.table_name.startswith('tbl'):
                    self.tablelist.append(t.table_name)


    def MakeTableView(self,in_table,whichdima):
        """ connects to Microsoft Access .mdb file, selects a table
        and copies it to a dataframe.
        ex.
        arc = arcno()
        arc.MakeTableView('table_name', 'dima_path')
        """
        self.in_table = in_table
        self.whichdima = whichdima

        try:
            return Table(self.in_table, self.whichdima).temp
        except Exception as e:
            print(e)

    def SelectLayerByAttribute(self, in_df,*vals, field = None):

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

                return pd.concat(dfset)
            except Exception as e:
                print(e)
        else:
            print("error")
    def GetCount(self,in_df):
        """ Returns number of rows in dataframe
        """
        self.in_df = in_df
        return self.in_df.shape[0]

    def AddJoin(self,
    in_df,df2,right_on=None,left_on=None):
        """ inner join on two dataframes on 1 or 2 fields
        ex.
        arc = arcno()
        arc.AddJoin('dataframe_x', 'dataframe_y', 'field_a')
        """
        # self.temp_table = None
        d={}
        self.right_on = None
        self.left_on = None

        d[self.right_on] = right_on
        d[self.left_on] = left_on

        self.in_df = in_df
        self.df2 = df2

        if self.right_on==self.left_on and len(self.in_df.columns)==len(self.df2.columns):
            try:
                frames = [self.in_df, self.df2]
                return pd.concat(frames)
            except Exception as e:
                print(e)
                print('1. field or fields invalid' )
        elif self.right_on==self.left_on and len(self.in_df.columns)!=len(self.df2.columns):
            try:
                # frames = [self.in_df, self.df2]
                return self.in_df.merge(self.df2, on = d[self.right_on], how='inner')
            except Exception as e:
                print(e)
                print('2. field or fields invalid')
        else:
            try:
                return self.in_df.merge(self.df2,right_on=d[self.right_on], left_on=d[self.left_on])
            except Exception as e:
                print(e)
                print('3. field or fields invalid')

    def CalculateField(self,in_df,newfield,*fields):
        """ Creates a newfield by concatenating any number of existing fields
        ex.
        arc = arcno()
        arc.CalculateField('dataframe_x', 'name_of_new_field', 'field_x', 'field_y','field_z')
        field_x = 'red'
        field_y = 'blue'
        field_z = 'green'
        name_of_new_field = 'redbluegreen'
        """
        self.in_df = in_df
        self.newfield = newfield
        self.fields = fields

        self.in_df[f'{self.newfield}'] = (self.in_df[[f'{field}' for field in self.fields]].astype(str)).sum(axis=1)
        return self.in_df



    def AddField(self, in_df, newfield):
        """ adds empty field within 'in_df' with fieldname
        supplied in the argument
        """
        self.in_df = in_df
        self.newfield = newfield

        self.in_df[f'{self.newfield}'] = pd.Series()
        return self.in_df

    def RemoveJoin(self):
        """ creates deep copy of original dataset
        and joins any new fields product of previous
        right hand join
        """
        pass

    def isolateFields(self,in_df,*fields):
        """ creates a new dataframe with submitted
        fields.
        """
        self.in_df = in_df
        self.fields = fields
        self.isolates =self.in_df[[f'{field}' for field in self.fields]]
        return self.isolates


    def GetParameterAsText(self,string):
        """ return stringified element
        """
        self.string = f'{string}'
        return self.string
