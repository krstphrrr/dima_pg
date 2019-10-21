

arcno = arcno()
arcno.make('tblLines','C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\NM_TaosFO_LUP_2018_5-3b_01.mdb')
arcno.temp
arcno.select(arcno.temp,'1808051039437030','1808050843012839','1808051155204941',field='LineKey')
arcno.temp_table
"""
in_df = type dataframe

"""
var('er')
def var(name):
    exec("{0}=sum(1,2)".format(name))
    print(f'{name}')
name('joi','ned')
def name(arg1,arg2):
    import os
    joined= os.path.join(arg1+arg2)
    return joined

df = arcno.temp.copy(deep=True)
import pickle

pickle.dump(dflk)
dflk = df['LineKey']
prnt('LineKey','1808051039437030','1808050843012839')
def prnt(field,*args):
    empdict=dict()

    dfset = []
    def name(arg1,arg2):
        import os
        joined= os.path.join(arg1+arg2)
        return joined

    import pandas as pd
    df2 = pd.DataFrame()
    for arg in args:
        index = df['LineKey']==f'{arg}'
        exec("%s=df[index]" % name(f'{field}',f'{arg}'))
        dfset.append(eval(name(f'{field}',f'{arg}')))


    df2 = pd.concat(dfset)
    return df2



class test():
    temp = None

    def __init__(self,in_df=None):
        self.in_df = in_df

    def apply(self, op=None):
        if self.op=="invert":
            pass

    def print(self,field = None, val=None):
        self.field = field
        self.val = val
        index = self.in_df[f'{self.field}'] == self.val

        self.temp = self.in_df[index]
        return self.temp



    def __invert__(self):
        return self.apply("invert",self)

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


    def __init__(self, *args, **kwargs):
        self._values = args
        self._keyvals = dict(kwargs)

    def make(self,in_table,whichdima):
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
            print("dataframe made on arcno.temp")
            self.temp = pd.read_sql(query,con)
        except Exception as e:
            print(e)

    def select(self, in_df,*vals, field = None):
        """
        right now:
        - refactoring nested if statements
        later:
        - receiving multiple arguments for values instead of two
        - overloading operator instead of keyword argument
        """
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
