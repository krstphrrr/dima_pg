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
path = path = normpath(r"C:\Users\kbonefont\Desktop\Some_data\NM_TaosFO_LUP_2018_5-3b_01.mdb")
arc = arcno(path)
tblLPIDetail, tblLPIHeader, tblLines, tblPlots, tblSites
header = arc.MakeTableView_management('tblLPIHeader', path)
lpi_detail = arc.MakeTableView_management('tblLPIDetail', path)
lines = arc.MakeTableView_management('tblLines', path)
plots = arc.MakeTableView_management('tblPlots', path)

plots

for i in plots.columns:
    if (i.find('date')!=-1) or (i.find('Date')!=-1):
        print(i)
header.shape[0]
lpi_detail.shape[0]
head_detail = arc.AddJoin_management(header, lpi_detail, 'RecKey')

head_detail.shape[0]

head_det_lines = arc.AddJoin_management(head_detail, lines,'LineKey','LineKey')

head_det_lines.shape[0]

head_det_lines_plot = arc.AddJoin_management(head_det_lines, plots, 'PlotKey')
head_det_lines_plot.shape

for i in head_det_lines_plot.columns:
    if (i.find('date')!=-1) or (i.find('Date')!=-1):
        print(i)

for i in head_det_lines_plot.FormDate:
    print(i)

head_det_lines_plot = arc.AddJoin_management(head_det_lines, plots, 'PlotKey', 'PlotKey')
head_det_lines_plot.shape

head_pk = arc.CalculateField_management(head_det_lines_plot, "PrimaryKey", "PlotKey", "FormDate")
len(head_pk.PrimaryKey.unique())

plt_pk = arc.CalculateField_management(plots, 'PrimaryKey', 'PlotKey','FormDate')
plot_line = arc.AddJoin_management(plots,lines, 'PlotKey','PlotKey')

plot_line_det = arc.AddJoin_management(plot_line, head_detail, 'LineKey', 'LineKey')

plot_line_det_head = arc.AddJoin_management(plot_line_det, header, 'RecKey')

for i in plot_line_det_head.FormDate:
    print(i)

plot_pk = arc.CalculateField_management(plot_line_det_head, "PrimaryKey", "PlotKey", "FormDate")
len(plot_pk.PrimaryKey.unique())

df = new_pk(path)
def new_pk(dimapath):
    header = arc.MakeTableView_management('tblLPIHeader', dimapath)
    lpi_detail = arc.MakeTableView_management('tblLPIDetail', dimapath)
    lines = arc.MakeTableView_management('tblLines', dimapath)
    plots = arc.MakeTableView_management('tblPlots', dimapath)

    plot_line = arc.AddJoin_management(plots,lines, 'PlotKey','PlotKey')
    plot_line_det = arc.AddJoin_management(plot_line, head_detail, 'LineKey', 'LineKey')
    plot_line_det_head = arc.AddJoin_management(plot_line_det, header, 'RecKey')
    plot_pk = arc.CalculateField_management(plot_line_det_head, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk


def to_plot(plottable, dimapath):
    fulldf = new_pk(dimapath)
    arc = arcno()
    arc.isolateFields(fulldf, 'PlotKey','PrimaryKey')
    plot_tmp = arc.isolates
    plt = plot_tmp.rename(columns={'PlotKey':'PlotKey2'}).copy(deep=True)
    # plot_pk = pd.concat([plottable,plot_tmp], axis=1, join='inner' )
    plt = plt.drop_duplicates(['PlotKey2'])
    print(plt.columns)
    print(plottable.columns)
    # plot_pk = plt.merge(plottable, on=[plt.PlotKey2,plottable.PlotKey])
    plot_pk = arc.AddJoin_management(plt, plottable,'PlotKey','PlotKey2')
    # plot_pk.rename(columns={plot_pk.columns[-2]:'PlotKey2'}, inplace=True)
    plot_pk.drop('PlotKey2', axis=1, inplace=True)
    return plot_pk

"""
NEED TO GET RID OF PESKY DUPLICATE COLUMN
"""
PLOT = to_plot(plots,path)
PLOT.PrimaryKey




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


    def MakeTableView_management(self,in_table,whichdima):
        """ connects to Microsoft Access .mdb file, selects a table
        and copies it to a dataframe.
        ex.
        arc = arcno()
        arc.MakeTableView_management('table_name', 'dima_path')
        """
        self.in_table = in_table
        self.whichdima = whichdima

        try:
            return Table(self.in_table, self.whichdima).temp
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

                return pd.concat(dfset)
            except Exception as e:
                print(e)
        else:
            print("error")
    def GetCount_management(self,in_df):
        """ Returns number of rows in dataframe
        """
        self.in_df = in_df
        return self.in_df.shape[0]

    def AddJoin_management(self,
    in_df,df2,right_on=None,left_on=None):
        """ inner join on two dataframes on 1 or 2 fields
        ex.
        arc = arcno()
        arc.AddJoin_management('dataframe_x', 'dataframe_y', 'field_a')
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

    def CalculateField_management(self,in_df,newfield,*fields):
        """ Creates a newfield by concatenating any number of existing fields
        ex.
        arc = arcno()
        arc.CalculateField_management('dataframe_x', 'name_of_new_field', 'field_x', 'field_y','field_z')

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



    def AddField_management(self, in_df, newfield):
        """ adds empty field within 'in_df' with fieldname
        supplied in the argument
        """
        self.in_df = in_df
        self.newfield = newfield

        self.in_df[f'{self.newfield}'] = pd.Series()
        return self.in_df

    def RemoveJoin_management(self):
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


    # def MakeFeatureLayer_management(self):
    #     """ may be similar to screate table view
    #     """
    #     pass

    def GetParameterAsText(self,string):
        """ return stringified element

        """
        self.string = f'{string}'
        return self.string
