from arcnah import arcno
from os.path import normpath, split, splitext, join
from utils import db
from sqlalchemy import create_engine
from utils import sql_str, config
from datetime import datetime
from psycopg2 import sql
import pandas as pd
"""
None-BSNE data: 'PlotKey' + 'FormDate' on most atomic level
BSNE data: 'PlotKey' + 'collectDate'

to-do:
- refactor all

"""


"""
creating dataframes that create a primary key at most atomic level And
propagate it outward:
1. LPI
2. Gap
3. SpeRick
4. PlantDen
5. BSNE
"""


def lpi_pk(dimapath):
    # tables
    arc = arcno()
    lpi_header = arc.MakeTableView('tblLPIHeader', dimapath)
    lpi_detail = arc.MakeTableView('tblLPIDetail', dimapath)
    lines = arc.MakeTableView('tblLines', dimapath)
    plots = arc.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arc.AddJoin(plots,lines, 'PlotKey','PlotKey')
    lpihead_detail = arc.AddJoin(lpi_header, lpi_detail, 'RecKey')
    plot_line_det = arc.AddJoin(plot_line, lpihead_detail, 'LineKey', 'LineKey')
    # plot_line_det_head = arc.AddJoin(plot_line_det, lpiheader, 'RecKey')
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def gap_pk(dimapath):
    # tables
    arc = arcno()
    gap_header = arc.MakeTableView('tblGapHeader', dimapath)
    gap_detail = arc.MakeTableView('tblGapDetail', dimapath)
    lines = arc.MakeTableView('tblLines', dimapath)
    plots = arc.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arc.AddJoin(plots,lines, 'PlotKey','PlotKey')
    gaphead_detail = arc.AddJoin(gap_header, gap_detail, 'RecKey')
    plot_line_det = arc.AddJoin(plot_line, gaphead_detail, 'LineKey', 'LineKey')
    # plot_line_det_head = arc.AddJoin(plot_line_det, header, 'RecKey')
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def sperich_pk(dimapath):
    # tables
    arc = arcno()
    spe_header = arc.MakeTableView('tblSpecRichHeader', dimapath)
    spe_detail = arc.MakeTableView('tblSpecRichDetail', dimapath)
    lines = arc.MakeTableView('tblLines', dimapath)
    plots = arc.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arc.AddJoin(plots,lines, 'PlotKey','PlotKey')
    spehead_detail = arc.AddJoin(spe_header, spe_detail, 'RecKey')
    plot_line_det = arc.AddJoin(plot_line, spehead_detail, 'LineKey', 'LineKey')
    # plot_line_det_head = arc.AddJoin(plot_line_det, header, 'RecKey')
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def plantden_pk(dimapath):
    # tables
    arc = arcno()
    pla_header = arc.MakeTableView('tblPlantDenHeader', dimapath)
    pla_detail = arc.MakeTableView('tblPlantDenDetail', dimapath)
    lines = arc.MakeTableView('tblLines', dimapath)
    plots = arc.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arc.AddJoin(plots,lines, 'PlotKey','PlotKey')
    plahead_detail = arc.AddJoin(pla_header, pla_detail, 'RecKey')
    plot_line_det = arc.AddJoin(plot_line, plahead_detail, 'LineKey', 'LineKey')
    # plot_line_det_head = arc.AddJoin(plot_line_det, header, 'RecKey')
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def bsne_pk(dimapath):
    box = arc.MakeTableView("tblBSNE_Box",dimapath)

    stack = arc.MakeTableView("tblBSNE_Stack", dimapath)
    boxcol = arc.MakeTableView('tblBSNE_BoxCollection', dimapath)

    df = arc.AddJoin(stack, box, "StackID", "StackID")
    df2 = arc.AddJoin(df,boxcol, "BoxID")
    df2 = arc.CalculateField(df2,"PrimaryKey","PlotKey","collectDate")
    return df2

"""
takes a dima tablename string and a dima path,
returns the table with the proper PrimaryKey field appended.
(works for the most part, depends on dima)
to-do:
- DONE implement sites table
- DONE detect calibration in dimapath and append to dbkey? or source
- keep unexpected columns
"""
arc = arcno()
# plottmp = arc.MakeTableView('tblBSNE_Stack', path)

def pk_add(tablename,dimapath):
    """
    adds primary key to the chosen table
    1. PlotKey pipe
    2. LineKey pipe
    3. reckey pipe
    4. pipe if its none of the above: only BSNE implemented
    """
    plot = None


    plotkeylist = ['tblPlots','tblLines','tblQualHeader','tblSoilStabHeader',
    'tblSoilPits','tblPlantProdHeader','tblPlotNotes', 'tblSoilPitHorizons']

    linekeylist = ['tblGapHeader','tblLPIHeader','tblSpecRichHeader',
    'tblPlantDenHeader']

    reckeylist = ['tblGapDetail','tblLPIDetail','tblQualDetail','tblSoilStabDetail',
    'tblSpecRichDetail','tblPlantProdDetail','tblPlantDenDetail']

    nonline = {'tblQualDetail':'tblQualHeader',
    'tblSoilStabDetail':'tblSoilStabHeader','tblPlantProdDetail':'tblPlantProdHeader'}

    arc = arcno()
    if tablename in plotkeylist:

        if tablename.find('Horizon')!=-1:
            fulldf = gap_pk(dimapath)
            if fulldf.shape[0]<1:
                fulldf = lpi_pk(dimapath)
            arc.isolateFields(fulldf, 'PlotKey','PrimaryKey')
            soil_tmp = arc.isolates
            plt = soil_tmp.rename(columns={'PlotKey':'PlotKey2'}).copy(deep=True)
            plt = plt.drop_duplicates(['PlotKey2'])
            plottable = arc.MakeTableView('tblSoilPits',dimapath)
            soil_pk = plt.merge(plottable, left_on=plt.PlotKey2, right_on=plottable.PlotKey)
            soil_pk.drop('PlotKey2', axis=1, inplace=True)
            soil_pk = soil_pk.copy(deep=True)
            soil_pk.drop('key_0', axis=1, inplace=True)
            hordf = arc.MakeTableView(f'{tablename}', dimapath)

            arc.isolateFields(soil_pk,'SoilKey','PrimaryKey')
            sk_tmp = arc.isolates
            plt = sk_tmp.rename(columns={'SoilKey':'SoilKey2'}).copy(deep=True)
            plt = plt.drop_duplicates(['SoilKey2'])
            soil_pk = plt.merge(hordf, left_on=plt.SoilKey2, right_on=hordf.SoilKey)
            soil_pk.drop('SoilKey2', axis=1, inplace=True)
            soil_pk = soil_pk.copy(deep=True)
            soil_pk.drop('key_0', axis=1, inplace=True)

            return soil_pk

        else:
            fulldf = gap_pk(dimapath)
            arc.isolateFields(fulldf, 'PlotKey','PrimaryKey')
            plot_tmp = arc.isolates
            plt = plot_tmp.rename(columns={'PlotKey':'PlotKey2'}).copy(deep=True)
            plt = plt.drop_duplicates(['PlotKey2'])
            plottable = arc.MakeTableView(f'{tablename}',dimapath)
            plot_pk = plt.merge(plottable, left_on=plt.PlotKey2, right_on=plottable.PlotKey)
            plot_pk.drop('PlotKey2', axis=1, inplace=True)
            plot_pk = plot_pk.copy(deep=True)
            plot_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] =plot_pk
            return plot_pk

    elif tablename in linekeylist:
        if tablename.find('Gap')!=-1:
            fulldf = gap_pk(dimapath)
            arc.isolateFields(fulldf, 'LineKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'LineKey':'LineKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['LineKey2'])
            linekeytable = arc.MakeTableView(f'{tablename}',dimapath)
            linekeytable_pk = lin.merge(linekeytable, left_on=lin.LineKey2, right_on=linekeytable.LineKey)
            linekeytable_pk.drop('LineKey2', axis=1, inplace=True)
            linekeytabler_pk = linekeytable_pk.copy(deep=True)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = linekeytable_pk
            return linekeytable_pk

        elif tablename.find('LPI')!=-1:
            fulldf = lpi_pk(dimapath)
            arc.isolateFields(fulldf, 'LineKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'LineKey':'LineKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['LineKey2'])
            linekeytable = arc.MakeTableView(f'{tablename}',dimapath)
            linekeytable_pk = lin.merge(linekeytable, left_on=lin.LineKey2, right_on=linekeytable.LineKey)
            linekeytable_pk.drop('LineKey2', axis=1, inplace=True)
            linekeytabler_pk = linekeytable_pk.copy(deep=True)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = linekeytable_pk
            return linekeytable_pk

        elif tablename.find('SpecRich')!=-1:
            fulldf = sperich_pk(dimapath)
            arc.isolateFields(fulldf, 'LineKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'LineKey':'LineKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['LineKey2'])
            linekeytable = arc.MakeTableView(f'{tablename}',dimapath)
            linekeytable_pk = lin.merge(linekeytable, left_on=lin.LineKey2, right_on=linekeytable.LineKey)
            linekeytable_pk.drop('LineKey2', axis=1, inplace=True)
            linekeytabler_pk = linekeytable_pk.copy(deep=True)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = linekeytable_pk
            return linekeytable_pk

        elif tablename.find('PlantDen')!=-1:
            fulldf = plantden_pk(dimapath)
            arc.isolateFields(fulldf, 'LineKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'LineKey':'LineKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['LineKey2'])
            linekeytable = arc.MakeTableView(f'{tablename}',dimapath)
            linekeytable_pk = lin.merge(linekeytable, left_on=lin.LineKey2, right_on=linekeytable.LineKey)
            linekeytable_pk.drop('LineKey2', axis=1, inplace=True)
            linekeytabler_pk = linekeytable_pk.copy(deep=True)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            return linekeytable_pk

    elif tablename in reckeylist:
        if tablename.find('Gap')!=-1:
            fulldf = gap_pk(dimapath)
            arc.isolateFields(fulldf, 'RecKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'RecKey':'RecKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['RecKey2'])
            reckeytable = arc.MakeTableView(f'{tablename}',dimapath)
            reckeytable_pk = lin.merge(reckeytable, left_on=lin.RecKey2, right_on=reckeytable.RecKey)
            reckeytable_pk.drop('RecKey2', axis=1, inplace=True)
            reckeytable_pk = reckeytable_pk.copy(deep=True)
            reckeytable_pk.drop('key_0', axis=1, inplace=True)
            return reckeytable_pk

        elif tablename.find('LPI')!=-1:
            fulldf = lpi_pk(dimapath)
            arc.isolateFields(fulldf, 'RecKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'RecKey':'RecKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['RecKey2'])
            reckeytable = arc.MakeTableView(f'{tablename}',dimapath)
            reckeytable_pk = lin.merge(reckeytable, left_on=lin.RecKey2, right_on=reckeytable.RecKey)
            reckeytable_pk.drop('RecKey2', axis=1, inplace=True)
            reckeytable_pk = reckeytable_pk.copy(deep=True)
            reckeytable_pk.drop('key_0', axis=1, inplace=True)
            return reckeytable_pk

        elif tablename.find('SpecRich')!=-1:
            fulldf = sperich_pk(dimapath)
            arc.isolateFields(fulldf, 'RecKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'RecKey':'RecKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['RecKey2'])
            reckeytable = arc.MakeTableView(f'{tablename}',dimapath)
            reckeytable_pk = lin.merge(reckeytable, left_on=lin.RecKey2, right_on=reckeytable.RecKey)
            reckeytable_pk.drop('RecKey2', axis=1, inplace=True)
            reckeytable_pk = reckeytable_pk.copy(deep=True)
            reckeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = reckeytable_pk
            return reckeytable_pk

        elif tablename.find('PlantDen')!=-1:
            fulldf = plantden_pk(dimapath)
            arc.isolateFields(fulldf, 'RecKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'RecKey':'RecKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['RecKey2'])
            reckeytable = arc.MakeTableView(f'{tablename}',dimapath)
            reckeytable_pk = lin.merge(reckeytable, left_on=lin.RecKey2, right_on=reckeytable.RecKey)
            reckeytable_pk.drop('RecKey2', axis=1, inplace=True)
            reckeytable_pk = reckeytable_pk.copy(deep=True)
            reckeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = reckeytable_pk
            return reckeytable_pk

        else:
            tempdf = arc.MakeTableView(nonline[f'{tablename}'], dimapath)
            fulldf = arc.CalculateField(tempdf, "PrimaryKey", "PlotKey", "FormDate")
            arc.isolateFields(fulldf, 'RecKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'RecKey':'RecKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['RecKey2'])
            reckeytable = arc.MakeTableView(f'{tablename}',dimapath)
            reckeytable_pk = lin.merge(reckeytable, left_on=lin.RecKey2, right_on=reckeytable.RecKey)
            reckeytable_pk.drop('RecKey2', axis=1, inplace=True)
            reckeytable_pk = reckeytable_pk.copy(deep=True)
            reckeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = reckeytable_pk
            return reckeytable_pk

    elif tablename.find("BSNE")!=-1:
        if tablename.find("Stack")!=-1:
            tempdf = arc.MakeTableView(f'{tablename}', dimapath)
            fulldf = bsne_pk(dimapath)
            arc.isolateFields(fulldf, 'PlotKey','PrimaryKey')
            plot_tmp = arc.isolates
            plt = plot_tmp.rename(columns={'PlotKey':'PlotKey2'}).copy(deep=True)
            plt = plt.drop_duplicates(['PlotKey2'])
            plottable = arc.MakeTableView(f'{tablename}',dimapath)
            plot_pk = plt.merge(plottable, left_on=plt.PlotKey2, right_on=plottable.PlotKey)
            plot_pk.drop('PlotKey2', axis=1, inplace=True)
            plot_pk = plot_pk.copy(deep=True)
            plot_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] =plot_pk
            return plot_pk

        if tablename.find("BoxCollection")!=-1:
            tempdf = arc.MakeTableView(f'{tablename}', dimapath)
            fulldf = bsne_pk(dimapath)
            arc.isolateFields(fulldf, 'BoxID','PrimaryKey')
            plot_tmp = arc.isolates
            plt = plot_tmp.rename(columns={'BoxID':'BoxID2'}).copy(deep=True)
            plt = plt.drop_duplicates(['BoxID2'])
            plottable = arc.MakeTableView(f'{tablename}',dimapath)
            plot_pk = plt.merge(plottable, left_on=plt.BoxID2, right_on=plottable.BoxID)
            plot_pk.drop('BoxID2', axis=1, inplace=True)
            plot_pk = plot_pk.copy(deep=True)
            plot_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] =plot_pk
            return plot_pk
        elif tablename.find("Trap")!=-1:
            tempdf = arc.MakeTableView(f'{tablename}', dimapath)
            return tempdf

        else:
            tempdf = arc.MakeTableView(f'{tablename}', dimapath)
            fulldf = bsne_pk(dimapath)
            arc.isolateFields(fulldf, 'BoxID','PrimaryKey')
            plot_tmp = arc.isolates
            plt = plot_tmp.rename(columns={'BoxID':'BoxID2'}).copy(deep=True)
            plt = plt.drop_duplicates(['BoxID2'])
            plottable = arc.MakeTableView(f'{tablename}',dimapath)
            plot_pk = plt.merge(plottable, left_on=plt.BoxID2, right_on=plottable.BoxID)
            plot_pk.drop('BoxID2', axis=1, inplace=True)
            plot_pk = plot_pk.copy(deep=True)
            plot_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] =plot_pk
            return plot_pk
    elif (tablename.find('tblSpecie')!=-1) or (tablename.find('tblSpeciesGeneric')!=-1) or (tablename.find('tblSites')!=-1):
        tempdf = arc.MakeTableView(f'{tablename}', dimapath)
        return tempdf

    else:
        # print('Supplied tablename does not exist in dima or a path to it has not been implemented.')

        tempdf = arc.MakeTableView(f'{tablename}', dimapath)
        return tempdf

def newcols(fdf,rdf):
    for item in fdf.columns.tolist():
        if item not in rdf.columns.tolist():
            df = rdf.copy(deep=True)
            df[f'{item}'] = fdf[f'{item}']
            return df




# dimapath3 = normpath(r"C:\Users\kbonefont\Desktop\Some_data\REPORT 3Aug16 - 27Oct17 Mandan DIMA 5.3 as of 2018-02-14.mdb")
# # db.str.close()
# # db.str.commit()
# # db.str.rollback()
# df = pk_add("tblGapDetail",dimapath3)
# dbcols = pd.read_sql(f'SELECT * FROM "tblGapDetail" LIMIT 1', db.str)
# cur = db.str.cursor()
# engine = create_engine(sql_str(config()))

# if len(df.columns.tolist())>1:
#     for item in df.columns.tolist():
#         if item not in dbcols.columns.tolist():
#             print(f'{item} is not in db')
#             data = pd.read_sql(f'SELECT * FROM "tblGapDetail"', db.str)
#             str1 = join(f'{item}'+'_inspect')
#             df1 = pd.concat([data,df[f'{item}']],sort=False, axis=1)
#             # cur.execute(f'DROP TABLE "tblGapDetail"')
#             # cur.commit()
#             df1.to_sql(name="tblGapDetail",con=engine, index=False, if_exists='replace', chunksize=500)
#             print(str1, 'column added to db')
#
# if len(df.columns.tolist())>1:
#     for item in dbcols.columns.tolist():
#         if item not in df.columns.tolist():
#             print(f'{item} is not in new df')
#             df1 = df.copy(deep=True)
#             str2 = join(f'{item}'+'_fromdf')
#             df1[f'{str2}'] = pd.Series()
#             # cur.execute(f'DROP TABLE "tblGapDetail"')
#             # cur.commit()
#             df1.to_sql(name="tblGapDetail",con=engine, index=False, if_exists='replace', chunksize=500)
#             print(str2, 'column added to df and ingested')



#
# str = 'lol'
# str.capitalize()
#
#
# from sqlalchemy import MetaData
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float
# Base = declarative_base()
#
# df = pd.DataFrame({
# 'a':[1,2],
# 'b':[3,4]
# })
#
# dimapath2 = normpath(r"C:\Users\kbonefont\Desktop\Some_data\NM_TaosFO_LUP_2018_5-3b_01.mdb")
# arc = arcno()
# df = arc.MakeTableView('tblGapHeader', dimapath2)
#
#
# for item in [print(df[col].dtype,col) for col in df.columns]:
#     print(item)
# typeconv = {
# 'int64':Integer,
# 'O':String,
# '<M8[ns]':DateTime,
# 'bool':Boolean,
# 'float64':Float
# }
# df.columns
# typeconv[f'{tp}']
# df['LineKey'].dtype
# df['DateModified'].dtype
# df['GapMin'].dtype
# df['PerennialsCanopy'].dtype
# tp=df['a'].dtype
#
# def tbl(tablename,dimapath):
#     arc = arcno()
#     df = arc.MakeTableView(f'{tablename}', dimapath).columns
#     Base = declarative_base()
#     class myclass(Base):
#         __tablename__ = f'{tablename.lower()}'
#
#         field1 = Column(Integer, primary_key=True)
#         field2 = Column(String)
#
#         #
#         # def __repr__(self):
#         #     return "<User(name='%s', fullname='%s', nickname='%s')>" % (self.name, self.fullname, self.nickname)
#
#     myclass.__name__ = tablename
#     return myclass
# stuff = tbl('Stuff','id','color')
# type(stuff)


def pg_send(tablename, dimapath):

    cursor = db.str.cursor()
    try:
        # adds primarykey to access table and returns dataframe with it
        df = pk_add(tablename,dimapath)
        # df = pk_add('tblGapDetail',dimapath3)
        # adds dateloaded and db key to dataframe
        df['DateLoadedInDB']= datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        if dimapath.find('calibration')!=-1:
            df['DBKey']=join('calibration_',split(splitext(dimapath)[0])[1].replace(" ",""))
            df['DBKey']=split(splitext(dimapath)[0])[1].replace(" ","")
        else:
            df['DBKey']=split(splitext(dimapath)[0])[1].replace(" ","")
        # if 'tempSeqNo' in df.columns:
        #     df=df.drop('tempSeqNo',1)

        # use pandas 'to_sql' to send altered dataframe to postgres db
        engine = create_engine(sql_str(config()))
        if df.shape[0]>0:
            # need to pull col list from db
            try:
            #if theres no difference, ingest and append, else do the magic
                df.to_sql(name=f'{tablename}',con=engine, index=False, if_exists='append')

            except Exception as e:
                print("mismatch between the columns in database table and supplied table..")

                dbcols = pd.read_sql(f'SELECT * FROM "{tablename}" LIMIT 1', db.str)

                # df.columns.tolist().sort()
                # dbcols.columns.tolist().sort()
                if len(df.columns.tolist())>1:
                    for item in df.columns.tolist():
                        if item not in dbcols.columns.tolist():
                            print(f'{item} is not in db')
                            data = pd.read_sql(f'SELECT * FROM "{tablename}"', db.str)
                            str1 = join(f'{item}'+'_CHK')
                            df1 = pd.concat([data,df[f'{item}']],sort=False, axis=1)
                            # to_sql: hangs on 'replace' operation, also, deletes previous db table data
                            # explore sqlalchemy bulk upload
                            df1.to_sql(name=f'{tablename}',con=engine, index=False, if_exists='replace')
                            # need:
                            # 1. pull data from db
                            # 2. delete table,
                            # 3. modify pulled table, add columns or rows etc.
                            # 4. reingest modified table

                            print(str1, 'column added to db')

                    for item in dbcols.columns.tolist():
                        if item not in df.columns.tolist():
                            print(f'column {item} missing from supplied df')
                            # df1 = df.copy(deep=True)
                            # df1[f'{item}'] = pd.Series()
                            # df1.to_sql(name=f'{tablename}',con=engine, index=False, if_exists='replace', chunksize=500)



            # else:
            #     df.to_sql(name=f'{tablename}',con=engine, index=False, if_exists='append', chunksize=500)
            #     db.str.commit()


            # return df
        else:
            print(f'Ingestion to postgresql DB aborted: {tablename} is empty')

    except Exception as e:
        print(e)
        # cursor.execute("ROLLBACK")
        # db.str.commit()

def drop_one(table):
    con = db.str
    cur = db.str.cursor()
    try:
        cur.execute(
        sql.SQL('DROP TABLE IF EXISTS postgres.public.{0}').format(
                 sql.Identifier(table))
    )
        con.commit()
        print(f'successfully dropped {table}')


    except Exception as e:
        print(e)
