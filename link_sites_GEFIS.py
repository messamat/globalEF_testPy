from GEFIS_setup import *

#Input variables
gefis15s_gdb = os.path.join(resdir, 'GEFIS_15s.gdb')
EMCperc = os.path.join(gefis15s_gdb, 'EMC_10Variable_2') #GEFIS layer of Vorosmarty human pressure index at 15 arc-sec
px_grid = os.path.join(datdir, 'pixel_area_skm_15s.gdb', 'px_area_skm_15s') #HydroSHEDS pixel area grid
wsgdb = os.path.join(resdir, 'EFsites_watersheds.gdb')

#Output variables
gefisstats_gdb = os.path.join(resdir, 'GEFIS_stats.gdb')
pathcheckcreate(gefisstats_gdb)

#Layers to get percentages of
EMCinter = [0, .25, .5, .65, 1]
EMCs = ['A', 'B', 'C', 'D']
EMCdict = dict(zip(
    EMCs,
    [[l,u] for l, u in zip(EMCinter[:-1], EMCinter[1:])]
))
for c in EMCdict:
 EMCdict[c].append(os.path.join(gefis15s_gdb, 'EMCarea_{0}'.format(c)))

for emc in EMCdict:
    if not arcpy.Exists(EMCdict[emc][2]):
        print('Processing {0}'.format(emc))
        Con((Raster(EMCperc) > EMCdict[emc][0]) & (Raster(EMCperc) <= EMCdict[emc][1]),
            Raster(px_grid)).save(EMCdict[emc][2])

#Extract data
wslist = [os.path.join(wsgdb, x) for x in getfilelist(wsgdb)]


#Layers to sum and divide
sumdivras_list = [os.path.join(gefis15s_gdb, p) for p in
               ['MAR_A_v2', 'MAR_B_v2', 'MAR_C_v2','MAR_D_v2', 'MAR_EF_Probable_mcm', 'MAR_Natural_Annual_Runoff_v2']]

def zonalstats_indiv(zonelist, valuelist, statistics, rastemplate = None, IDcol = None):
    statsdict = defaultdict(list)
    for in_zone in zonelist:
        for in_value in valuelist:
            print('Zone: {0}, Layer: {1}'.format(in_zone, in_value))
            if rastemplate is not None:
                arcpy.env.extent = arcpy.env.snapRaster = rastemplate

            if not statistics == 'RATIO':
                outab = arcpy.da.TableToNumPyArray(
                    ZonalStatisticsAsTable(in_zone_data=in_zone, zone_field='Value',
                                           in_value_raster=in_value, out_table= os.path.join(gefisstats_gdb, 'temp'),
                                           ignore_nodata= 'DATA', statistics_type=statistics),
                    statistics)
            else:
                outab = arcpy.da.TableToNumPyArray(
                    ZonalStatisticsAsTable(in_zone_data=in_zone, zone_field='Value',
                                           in_value_raster=in_value, out_table= os.path.join(gefisstats_gdb, 'temp'),
                                           ignore_nodata= 'DATA', statistics_type='SUM'),
                    ['COUNT', 'SUM'])

            if outab.shape[0] > 0:
                if not statistics == 'RATIO':
                    statsdict[os.path.split(in_zone)[1]].append(outab[0][0])
                else:
                    statsdict[os.path.split(in_zone)[1]].append(outab[0][1]/float(outab[0][0]))
            else:
                statsdict[os.path.split(in_zone)[1]].append(np.nan)

    out_pd = pd.DataFrame.from_dict(statsdict, orient='index').reset_index()
    out_pd.columns = [IDcol]+[rasname(r) for r in valuelist]

    return(out_pd)

sumdiv_pd = zonalstats_indiv(zonelist = wslist, valuelist = sumdivras_list, statistics = 'SUM',
                             rastemplate = px_grid, IDcol = 'no')
sumdiv_pd.iloc[:,1:] = sumdiv_pd.iloc[:,1:].div(576.0)

sum_pd = zonalstats_indiv(zonelist = wslist, valuelist = getfilelist(gefis15s_gdb, 'EMCarea.*'), statistics = 'SUM',
                          rastemplate = px_grid, IDcol = 'no')
mean_pd = zonalstats_indiv(zonelist = wslist, valuelist = [os.path.join(gefis15s_gdb, 'EMC_10Variable_2')],
                           statistics = 'MEAN',
                           rastemplate = px_grid, IDcol = 'no')

#Assess coverage of layers by basin
ratio_pd = zonalstats_indiv(zonelist = wslist, valuelist = getfilelist(gefis15s_gdb, '.*_boolean$'),
                              statistics = 'RATIO', rastemplate = px_grid, IDcol = 'no')

#Join all stats
allstats_pd = pd.merge(
    sumdiv_pd, sum_pd.reindex(columns=sorted(sum_pd.columns)), on='no').merge(
    mean_pd, on= 'no').merge(
    ratio_pd, on='no')

#Write out to csv
allstats_pd.to_csv(os.path.join(resdir, 'efsites_gefisstats.csv'))