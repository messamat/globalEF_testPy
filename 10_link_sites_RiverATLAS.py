from globalEF_comparison_setup import *

#Intputs
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_clean = os.path.join(process_gdb, 'EFpoints_20230424_clean')
riveratlas = os.path.join(datdir, 'RiverATLAS_v10.gdb', 'RiverATLAS_v10')

DA_grid = os.path.join(datdir, 'upstream_area_skm_15s.gdb', 'up_area_skm_15s') #HydroSHEDS upstream area grid

#Outputs
riveratlas_csv = os.path.join(resdir, 'RiverATLAS_v10tab.csv')
EFpoints_clean_riverjoin = os.path.join(process_gdb, 'EFpoints_20230424_clean_riverjoin')
EFpoints_clean_riverjoin_csv = os.path.join(resdir, 'EFpoints_20230424_clean_riverjoin.csv')



#------------------------------- Analysis ------------------------------------------------------------------------------
#Join to RiverATLAS
arcpy.analysis.SpatialJoin(EFpoints_clean, riveratlas, EFpoints_clean_riverjoin,
                           join_operation='JOIN_ONE_TO_ONE',
                           join_type="KEEP_COMMON",
                           match_option='CLOSEST_GEODESIC',
                           distance_field_name='station_river_distance')

#Extract HydroSHEDS
ExtractMultiValuesToPoints(in_point_features=EFpoints_clean_riverjoin, in_rasters=DA_grid,
                           bilinear_interpolate_values='NONE')


if not arcpy.Exists(EFpoints_clean_riverjoin_csv):
    arcpy.management.CopyRows(in_rows = EFpoints_clean_riverjoin, out_table=EFpoints_clean_riverjoin_csv)

#Export riveratlas attributes to csv file
if not arcpy.Exists(riveratlas_csv):
    print('Exporting CSV table of RiverATLAS v1.0 attributes')
    arcpy.management.CopyRows(in_rows = riveratlas, out_table=riveratlas_csv)