import os, pandas as pd
from os import listdir,getcwd, chdir
from os.path import normpath, join

from methods.make_table import Table
from methods.select import Select_tbl
from functools import reduce
getcwd()

df1 = pd.DataFrame({'a':['one','two'],'b':['three','four']})


df1.iloc[[0,1],[0]]

for i in [a for a in df1.iloc[[0,1],[1]].values]:
    print(i)
df1.values

for item in df1.values:
    for i in item:
        print(i)
[i for i in item for item in df1.values]

for a in df1.iloc[[0,1],[1]].values:
    for i in a:
        print(i)


lambda a,b: return join(a,b)
reduce(lambda a,b:a+b)

df1.iloc[[0,1],[0]].combine(df1.iloc[[0,1],[1]], lambda a,b:join(f'{a}',f'{b}'))

df1['comb'] = df1['a'].map(str)


df1['a'].map(str)+df1['b']



path = normpath(r"C:\Users\kbonefont\Desktop\Some_data\NM_TaosFO_LUP_2018_5-3b_01.mdb")
path
Table('tblLines',normpath(r"C:\Users\kbonefont\Desktop\Some_data\NM_TaosFO_LUP_2018_5-3b_01.mdb")).temp

arcno = arcno()

arcno.MakeTable('tblLines',path)
arcno.temp

arcno.Select(arcno.temp,field='LineKey')


arcno.select(arcno.temp,'1808051039437030','1808050843012839',field='LineKey')
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
index = df['LineKey']=='1808050843012839'
df[~index]
import operator
df[operator.invert(index)]
df
basicfilter=basicfilter()
basicfilter.select(df,'LineKey',~'1808050843012839')
basicfilter.tempdf
class basicfilter():
    tempdf = None

    def __init__(self, indf=None,field=None,*values):

        self.indf = indf
        self.field = field
        self.values = values
        self.index = None

    def select(self,indf,field,*values):

        self.indf = indf
        self.field = field
        self.values = values
        self.index = None
        dfset = []
        def name(arg1,arg2):

            self.arg1 = arg1
            self.arg2 = arg2

            joined= os.path.join(self.arg1+self.arg2)
            return joined

        for val in self.values:
            self.index = self.indf[f'{self.field}'] == f'{val}'
            exec("%s = self.indf[self.index]" % name(f'{self.field}',f'{val}'))
            dfset.append(eval(name(f'{self.field}',f'{val}')))
            self.tempdf = pd.concat(dfset)

    def __invert__(self):





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
        if self.in_df is not None:
            return ~self.in_df

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

    def __invert__(self):
        if self.in_df is not None:
            return ~self.in_df

    def MakeTable(self,in_table,whichdima):
        """ connects to ms access mdb, selects a table
        and copies it to a df
        """
        self.in_table = in_table
        self.whichdima = whichdima

        try:
            self.temp = Table(self.in_table, self.whichdima).temp
        except Exception as e:
            print(e)
    # def Select(self,in_df,*vals,field=None):
    #     self.in_df = in_df
    #     self.field = field
    #     self.vals = vals
    #     try:
    #         self.temp_table = Select_tbl(self.in_df, self.vals, self.field)
    #     except Exception as e:
    #         print(e)

    def select(self, in_df,*vals, field = None):
        """
        right now:
        - refactoring nested if statements _done
        later:
        - receiving multiple arguments for values instead of two _done
        - overloading operator instead of keyword argument
        --- unary opera
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
