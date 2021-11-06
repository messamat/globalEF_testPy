from GEFIS_setup import *

#Intputs
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_QAQCed_p = os.path.join(process_gdb, 'Master_20211104_QAQCed')
riveratlas = os.path.join(datdir, 'RiverATLAS_v10.gdb', 'RiverATLAS_v10')

DA_grid = os.path.join(datdir, 'upstream_area_skm_15s.gdb', 'up_area_skm_15s') #HydroSHEDS upstream area grid
MAF_nat_grid = os.path.join(datdir, 'discharge_wg22_1971_2000.gdb', 'dis_nat_wg22_ls_year') #WaterGAP naturalized mean annual discharge grid downscaled to 15s
MAF_ant_grid = os.path.join(datdir, 'discharge_wg22_1971_2000.gdb', 'dis_ant_wg22_ls_year') #WaterGAP naturalized mean annual discharge grid downscaled to 15s
#monthlyF_net = os.path.join(datdir, 'HS_discharge_monthly.gdb', 'Hydrosheds_discharge_monthly') #WaterGAP natural mean monthly discharge associated with HydroRIVERS

#Outputs
riveratlas_csv = os.path.join(resdir, 'RiverATLAS_v10tab.csv')
EFpoints_QAQCed_riverjoin = os.path.join(process_gdb, 'Master_20211104_QAQCed_riverjoin')

#------------------------------- Analysis ------------------------------------------------------------------------------
#Join to RiverATLAS
arcpy.SpatialJoin_analysis(EFpoints_QAQCed_p, riveratlas, EFpoints_QAQCed_riverjoin, join_operation='JOIN_ONE_TO_ONE',
                           join_type="KEEP_COMMON",
                           match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                           distance_field_name='station_river_distance')

#Extract HydroSHEDS and WaterGAP layers
ExtractMultiValuesToPoints(in_point_features=EFpoints_QAQCed_riverjoin, in_rasters=[DA_grid, MAF_nat_grid, MAF_ant_grid],
                           bilinear_interpolate_values='NONE')

#Export riveratlas attributes to csv file
if not arcpy.Exists(riveratlas_csv):
    print('Exporting CSV table of RiverATLAS v1.0 attributes')
    arcpy.CopyRows_management(in_rows = riveratlas, out_table=riveratlas_csv)

#Add monthly discharge associated with reaches
#Get all unique HYRIV_ID associated with the points
# hyrividset = {row[0] for row in arcpy.da.SearchCursor(EFpoints_1028_cleanriverjoin, 'HYRIV_ID')}
# #Get field names
# mdis_fnames = ['DIS_{0}_CMS'.format(format(x, '02')) for x in range(1, 13)]
# for newf in mdis_fnames:
#     arcpy.AddField_management(EFpoints_cleanjoin, newf)
# #Get monthly discharge values for points
# mdisdict = {row[0]: [row[x] for x in range(1, 13)]
#             for row in arcpy.da.SearchCursor(monthlyF_net, ['REACH_ID']+mdis_fnames)
#             if row[0] in hyrividset}
# #Write values to points
# with arcpy.da.UpdateCursor(EFpoints_cleanjoin, ['HYRIV_ID']+mdis_fnames) as cursor:
#     for row in cursor:
#         if row[0] in mdisdict:
#             row = tuple([row[0]]+mdisdict[row[0]])
#             cursor.updateRow(row)
