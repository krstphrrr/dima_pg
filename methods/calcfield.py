
import pandas as pd
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
def add(list, *elements):
    newlist=None
    if len(elements)>1:
        for element in elements:
            list.append(element)
            newlist=list
    else:
        list.append(elements)
        newlist=list


list = []

add(list,'a')
add.newlist()
df1['dfdf']+= df1['a'].map(str)
df1.rem

calc.comb(df1,'ok','a','b')
calc.in_
calc.out_

calc = calcField()
calcField.in_
calcField.out_


len([1,2,3,4,5])
class calcField:
    in_ = None
    out_ = None

    def __init__(self,*arg,**args):
        self.arg = arg
        self.args = args

    def comb(self,in_df,newfield,*fields):
        self.in_df = in_df
        self.newfield = newfield
        self.fields = fields
        self.in_ = self.in_df

        for field in self.fields:
            try:
                self.in_[f'{self.newfield}'] += self.in_[field].map(str)
                return self.in_.columns

                # self.out_ = self.in_
            except Exception as e:
                print(e)

    #
    # def __call__(self):
    #     return calcField().__call__()
    #
    # def df(self):
    #     return self.out_
