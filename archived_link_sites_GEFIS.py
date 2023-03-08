from globalEF_comparison_setup import *

#Input variables
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_QAQCed_riverjoin = os.path.join(process_gdb, 'Master_20211104_QAQCed_riverjoin')

gefis15s_gdb = os.path.join(resdir, 'GEFIS_15s.gdb')
EMCperc = os.path.join(gefis15s_gdb, 'EMC_10Variable_2') #GEFIS layer of Vorosmarty human pressure index at 15 arc-sec
EMC_GEFIS = os.path.join(resdir, 'GEFIS_15s.gdb', 'EMC_10Variable_2')
flowdir = os.path.join(datdir, 'flow_dir_15s_global.gdb', 'flow_dir_15s')
px_grid = os.path.join(datdir, 'pixel_area_skm_15s.gdb', 'px_area_skm_15s') #HydroSHEDS pixel area grid
up_grid = os.path.join(datdir, 'upstream_area_skm_15s.gdb', 'up_area_skm_15s')
wsgdb = os.path.join(resdir, 'EFsites_watersheds.gdb')

#Output variables
gefisstats_gdb = os.path.join(resdir, 'GEFIS_stats.gdb')
pathcheckcreate(gefisstats_gdb)

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
                                           in_value_raster=in_value,
                                           out_table= os.path.join(gefisstats_gdb, 'temp_{0}_{1}'.format(
                                               os.path.split(in_value)[1], statistics)),
                                           ignore_nodata= 'DATA', statistics_type=statistics),
                    statistics)
            else:
                outab = arcpy.da.TableToNumPyArray(
                    ZonalStatisticsAsTable(in_zone_data=in_zone, zone_field='Value',
                                           in_value_raster=in_value,
                                           out_table= os.path.join(gefisstats_gdb, 'temp_{0}_{1}'.format(
                                               os.path.split(in_value)[1], statistics)),
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

#----------------------------------------- Run flow accumulation -------------------------------------------------------
sumdivras_list = [os.path.join(gefis15s_gdb, p) for p in
                  ['MAR_A_v2', 'MAR_B_v2', 'MAR_C_v2', 'MAR_D_v2', 'MAR_EF_Probable_mcm',
                   'MAR_Natural_Annual_Runoff_v2']]
#sumlist = getfilelist(gefis15s_gdb, 'EMCarea.*')
meanlist = [os.path.join(gefis15s_gdb, 'EMC_10Variable_2')]
ratiolist = getfilelist(gefis15s_gdb, '.*_boolean$')

for ras in sumdivras_list:
    outacc = os.path.join(gefisstats_gdb, '{}_acc'.format(os.path.split(ras)[1]))
    if not arcpy.Exists(outacc):
        print('Generating {}'.format(outacc))
        FlowAccumulation(in_flow_direction_raster=flowdir,
                         in_weight_raster=ras,
                         data_type='FLOAT').save(outacc)

for ras in meanlist+ratiolist:
    outacc = os.path.join(gefisstats_gdb, '{}_wacc'.format(os.path.split(ras)[1]))
    if not arcpy.Exists(outacc):
        print('Generating {}'.format(outacc))
        FlowAccumulation(in_flow_direction_raster=flowdir,
                         in_weight_raster=(Raster(px_grid)*Raster(ras)),
                         data_type='FLOAT').save(outacc)

#Compute mean Incident Biodiversity Threat index upstream
marbool_wacc = os.path.join(gefisstats_gdb, 'MAR_boolean_wacc')
emcbool_wacc = os.path.join(gefisstats_gdb, 'EMC_boolean_wacc')
emcindex_wacc = os.path.join(gefisstats_gdb, 'EMC_10Variable_2_wacc')
emcindex_mean = os.path.join(gefisstats_gdb, 'EMC_10Variable_2_mean')
if not arcpy.Exists(emcindex_mean):
    (Raster(emcindex_wacc)/Raster(emcbool_wacc)).save(emcindex_mean)

#Compute percentage upstream masked out for MAR and EMC masks
marbool_ratio = os.path.join(gefisstats_gdb, 'MAR_boolean_ratio')
emcbool_ratio = os.path.join(gefisstats_gdb, 'EMC_boolean_ratio')
if not arcpy.Exists(marbool_ratio):
    (Raster(marbool_wacc)/Raster(up_grid)).save(marbool_ratio)
if not arcpy.Exists(emcbool_ratio):
    (Raster(emcbool_wacc)/Raster(up_grid)).save(emcbool_ratio)

#----------------------------------------- Extract by point ------------------------------------------------------------
#Extract GEFIS data at site point
oidfn = arcpy.Describe(EFpoints_QAQCed_riverjoin).OIDFieldName

for ras in ['MAR_boolean_ratio', 'EMC_boolean_ratio', 'EMC_10Variable_2_mean','MAR_A_v2_acc', 'MAR_B_v2_acc',
            'MAR_C_v2_acc', 'MAR_D_v2_acc', 'MAR_EF_Probable_mcm_acc', 'MAR_Natural_Annual_Runoff_v2_acc', EMC_GEFIS]:
    print('Processing {}'.format(ras))

    if os.path.split(ras)[0] != '':
        in_path = ras
        ras = os.path.split(ras)[1]
    else:
        in_path = os.path.join(gefisstats_gdb, ras)
    temptab = arcpy.da.TableToNumPyArray(
        Sample(in_rasters=in_path, in_location_data=EFpoints_QAQCed_riverjoin,
               out_table=os.path.join(process_gdb, 'temp'),
               resampling_type='NEAREST', unique_id_field=oidfn),
        field_names=['Master_20211104_QAQCed_riverjoin', '{}_Band_1'.format(ras)]
    )

    tempdict = {i:j for i,j in temptab}

    arcpy.management.AddField(EFpoints_QAQCed_riverjoin, ras, 'float')
    with arcpy.da.UpdateCursor(EFpoints_QAQCed_riverjoin, [oidfn, ras]) as cursor:
        for row in cursor:
            print(row[0])
            if row[0] in tempdict:
                if ras in ['MAR_A_v2_acc', 'MAR_B_v2_acc', 'MAR_C_v2_acc', 'MAR_D_v2_acc', 'MAR_EF_Probable_mcm_acc',
                   'MAR_Natural_Annual_Runoff_v2_acc']:
                    row[1] = tempdict[row[0]]/576.0
                else:
                    if not np.isnan(tempdict[row[0]]):
                        row[1] = tempdict[row[0]]
                cursor.updateRow(row)

arcpy.management.CopyRows(EFpoints_QAQCed_riverjoin, os.path.join(resdir, 'Master_20211104_QAQCed_riverjoin.csv'))


#######################################################################################################################
# #----------------------------------------- Extract by watershed --------------------------------------------------------
# #Layers to get percentages of
# EMCinter = [0, .25, .5, .65, 1]
# EMCs = ['A', 'B', 'C', 'D']
# EMCdict = dict(zip(
#     EMCs,
#     [[l,u] for l, u in zip(EMCinter[:-1], EMCinter[1:])]
# ))
# for c in EMCdict:
#  EMCdict[c].append(os.path.join(gefis15s_gdb, 'EMCarea_{0}'.format(c)))
#
# for emc in EMCdict:
#     if not arcpy.Exists(EMCdict[emc][2]):
#         print('Processing {0}'.format(emc))
#         Con((Raster(EMCperc) > EMCdict[emc][0]) & (Raster(EMCperc) <= EMCdict[emc][1]),
#             Raster(px_grid)).save(EMCdict[emc][2])
#
# #Extract data
# wslist = [os.path.join(wsgdb, x) for x in getfilelist(wsgdb)]
#
# #Layers to sum and divide
# sumdivras_list = [os.path.join(gefis15s_gdb, p) for p in
#                ['MAR_A_v2', 'MAR_B_v2', 'MAR_C_v2','MAR_D_v2', 'MAR_EF_Probable_mcm', 'MAR_Natural_Annual_Runoff_v2']]
#
# print('Processing rasters for divided sum')
# sumdiv_pd = zonalstats_indiv(zonelist = wslist, valuelist = sumdivras_list, statistics = 'SUM',
#                              rastemplate = px_grid, IDcol = 'no')
# sumdiv_pd.iloc[:,1:] = sumdiv_pd.iloc[:,1:].div(576.0)
# sumdiv_pd.to_csv(os.path.join(resdir, 'efsites_gefissumdiv.csv'))
#
# print('Processing rasters for sums')
# sum_pd = zonalstats_indiv(zonelist = wslist, valuelist = getfilelist(gefis15s_gdb, 'EMCarea.*'), statistics = 'SUM',
#                           rastemplate = px_grid, IDcol = 'no')
# sum_pd.to_csv(os.path.join(resdir, 'efsites_gefissum.csv'))
#
# print('Processing rasters for means')
# mean_pd = zonalstats_indiv(zonelist = wslist, valuelist = [os.path.join(gefis15s_gdb, 'EMC_10Variable_2')],
#                            statistics = 'MEAN',
#                            rastemplate = px_grid, IDcol = 'no')
# mean_pd.to_csv(os.path.join(resdir, 'efsites_gefismean.csv'))
#
# print('Processing coverage of layers by basin')
# ratio_pd = zonalstats_indiv(zonelist = wslist, valuelist = getfilelist(gefis15s_gdb, '.*_boolean$'),
#                               statistics = 'RATIO', rastemplate = px_grid, IDcol = 'no')
# ratio_pd.to_csv(os.path.join(resdir, 'efsites_gefisratio.csv'))
#
#
# #Join all stats
# allstats_pd = pd.merge(
#     sumdiv_pd, sum_pd.reindex(columns=sorted(sum_pd.columns)), on='no').merge(
#     mean_pd, on= 'no').merge(
#     ratio_pd, on='no')
#
# #Write out to csv
# allstats_pd.to_csv(os.path.join(resdir, 'efsites_gefisstats.csv'))