from arcnah import arcno

"""
None-BSNE data: 'PlotKey' + 'FormDate' on most atomic level
BSNE data: 'PlotKey' + 'collectDate' on most atomic level
"""
"""
Check tables inside the dima
"""
for tbl in arc.tablelist:
    tempdf = arc.MakeTableView(f'{tbl}',path)
    if tempdf.shape[0]>10:
        tablewithdata[f'{tbl}']=tempdf

tablewithdata = {}
[key for key in tablewithdata.keys()]

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
- behavior with empty tables inside dima
"""
def plotlinerec(tablename,dimapath):
    """
    adds primary key to the chosen table
    1. PlotKey pipe
    2. LineKey pipe
    3. reckey pipe
    4. pipe if its none of the above: only BSNE implemented
    """


    plotkeylist = ['tblPlots','tblLines','tblQualHeader','tblSoilStabHeader',
    'tblSoilPits','tblPlantProdHeader','tblPlotNotes']

    linekeylist = ['tblGapHeader','tblLPIHeader','tblSpecRichHeader',
    'tblPlantDenHeader']

    reckeylist = ['tblGapDetail','tblLPIDetail','tblQualDetail','tblSoilStabDetail',
    'tblSpecRichDetail','tblPlantProdDetail','tblPlantDenDetail']

    nonline = {'tblQualDetail':'tblQualHeader',
    'tblSoilStabDetail':'tblSoilStabHeader','tblPlantProdDetail':'tblPlantProdHeader'}

    arc = arcno()
    if tablename in plotkeylist:
        tempdf = arc.MakeTableView(f'{tablename}', dimapath)
        fulldf = arc.CalculateField(tempdf, "PrimaryKey", "PlotKey", "FormDate")
        # fulldf = lpi_pk(dimapath)
        arc.isolateFields(fulldf, 'PlotKey','PrimaryKey')
        plot_tmp = arc.isolates
        plt = plot_tmp.rename(columns={'PlotKey':'PlotKey2'}).copy(deep=True)
        plt = plt.drop_duplicates(['PlotKey2'])
        plottable = arc.MakeTableView(f'{tablename}',dimapath)
        plot_pk = plt.merge(plottable, left_on=plt.PlotKey2, right_on=plottable.PlotKey)
        plot_pk.drop('PlotKey2', axis=1, inplace=True)
        plot_pk = plot_pk.copy(deep=True)
        plot_pk.drop('key_0', axis=1, inplace=True)
        mdb[f'{tablename}'] =plot_pk
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
            print(linekeytable_pk.columns)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            mdb[f'{tablename}'] = linekeytable_pk
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
            print(linekeytable_pk.columns)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            mdb[f'{tablename}'] = linekeytable_pk
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
            print(linekeytable_pk.columns)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            mdb[f'{tablename}'] = linekeytable_pk
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
            print(linekeytable_pk.columns)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            mdb[f'{tablename}'] = linekeytable_pk
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
            mdb[f'{tablename}'] = reckeytable_pk
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
            mdb[f'{tablename}'] = reckeytable_pk
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
            mdb[f'{tablename}'] = reckeytable_pk
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
            mdb[f'{tablename}'] = reckeytable_pk
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
            mdb[f'{tablename}'] = reckeytable_pk
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
            mdb[f'{tablename}'] =plot_pk
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
            mdb[f'{tablename}'] =plot_pk
            return plot_pk

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
            mdb[f'{tablename}'] =plot_pk
            return plot_pk










def to_plot(plottable, dimapath):
    fulldf = lpi_pk(dimapath)
    arc = arcno()
    arc.isolateFields(fulldf, 'PlotKey','PrimaryKey')
    plot_tmp = arc.isolates
    plt = plot_tmp.rename(columns={'PlotKey':'PlotKey2'}).copy(deep=True)
    plt = plt.drop_duplicates(['PlotKey2'])
    plot_pk = plt.merge(plottable, left_on=plt.PlotKey2, right_on=plottable.PlotKey)
    plot_pk.drop('PlotKey2', axis=1, inplace=True)
    plot_pk = plot_pk.copy(deep=True)
    plot_pk.drop('key_0', axis=1, inplace=True)
    mdb['tblPlots'] =plot_pk
    return plot_pk
