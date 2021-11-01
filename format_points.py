from GEFIS_setup import *

#Input data
csdat = os.path.join(datdir, 'Formatted_data_20211018') #Data folder from Chandima Subasinghe (CS)
EFpoints_cs = os.path.join(csdat, "Combine_shiftedpoints.shp")
EFpoints_1028freeze = os.path.join(resdir, 'Master_20211031_parzered_notIWMI.csv')

hydroriv = os.path.join(datdir, 'HydroRIVERS_v10.gdb', 'HydroRIVERS_v10') #Download from https://www.hydrosheds.org/page/hydrorivers
riveratlas = os.path.join(datdir, 'RiverATLAS_v10.gdb', 'RiverATLAS_v10')
DA_grid = os.path.join(datdir, 'upstream_area_skm_15s.gdb', 'up_area_skm_15s') #HydroSHEDS upstream area grid
MAF_nat_grid = os.path.join(datdir, 'discharge_wg22_1971_2000.gdb', 'dis_nat_wg22_ls_year') #WaterGAP naturalized mean annual discharge grid downscaled to 15s
MAF_ant_grid = os.path.join(datdir, 'discharge_wg22_1971_2000.gdb', 'dis_ant_wg22_ls_year') #WaterGAP naturalized mean annual discharge grid downscaled to 15s
monthlyF_net = os.path.join(datdir, 'HS_discharge_monthly.gdb', 'Hydrosheds_discharge_monthly') #WaterGAP natural mean monthly discharge associated with HydroRIVERS
EMC_GEFIS = os.path.join(resdir, 'GEFIS_15s.gdb', 'EMC_10Variable_2')

#Outputs
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
pathcheckcreate(process_gdb)
EFpoints_cscopy = os.path.join(process_gdb, 'Combine_shiftedpoints_copy')
EFpoints_joinedit = os.path.join(process_gdb, 'EFpoints_joinedit')
EFpoints_clean = os.path.join(process_gdb, 'EFpoints_clean')
EFpoints_cleanjoin = os.path.join(process_gdb, 'EFpoints_cleanjoin')

riveratlas_csv = os.path.join(resdir, 'RiverATLAS_v10tab.csv')

#---------------------------------- FORMATTING OF SITES FROM CS / FIRST BATCH --------------------------------------------------
#Copy CS points
if not arcpy.Exists(EFpoints_cscopy):
    arcpy.CopyFeatures_management(EFpoints_cs, EFpoints_cscopy)

#Join EF points to nearest river reach in RiverAtlas
if not arcpy.Exists(EFpoints_joinedit):
    print('Join EF points to nearest river reach in RiverAtlas')
    arcpy.SpatialJoin_analysis(EFpoints_cscopy, hydroriv, EFpoints_joinedit,
                               join_operation='JOIN_ONE_TO_ONE', join_type="KEEP_COMMON",
                               match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                               distance_field_name='EFpoint_hydroriv_distance')

    arcpy.AddField_management(EFpoints_joinedit, field_name='DApercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints_joinedit, ['Upstream_C', 'UPLAND_SKM', 'DApercdiff']) as cursor:
        for row in cursor:
            if re.match('^[0-9]+$', string = row[0]):
                if int(row[0]) > 0L:
                    row[2] = (float(row[1]) - float(row[0]))/float(row[0])
            cursor.updateRow(row)

    arcpy.AddField_management(EFpoints_joinedit, field_name='totalAF', field_type='FLOAT')
    arcpy.AddField_management(EFpoints_joinedit, field_name='MAFpercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints_joinedit, ['Mean_Annua', 'DIS_AV_CMS', 'MAFpercdiff', 'totalAF']) as cursor:
        for row in cursor:
            if re.match('^[0-9]+\.*[0-9]*', string = row[0]):
                if float(row[0]) > 0:
                    row[2] = (31.557600*float(row[1]) - float(row[0]))/float(row[0])
                    row[3] = (31.557600 * float(row[1]))
            cursor.updateRow(row)

#Manual editing
#no, what is done (delete: -1, moved: 1), comment, OBJECTID
editdict = {
    0: [-1, 'artefact without data. It seems to correspond to Macalister/Below Lake Glenmaggie though'],
    1: [1, 'moved to intersect the nearest reach on the Balonne river'],
    6: [1, 'move to main stem'],
    7: [1, 'move to main stem'],
    11: [1, 'move very far. all the way to below Lake Glenmaggie in Victoria'],
    12: [1, 'move to main stem'],
    13: [1, 'move to below Tallowa Dam'],
    15: [-1, 'artefactual point created by CS for Tallowa Dam'],
    20: [1, 'move to river mouth'],
    23: [1, 'moved to Luan River aka Luanhe mouth. Point was completely off'],
    26: [-1, 'unreliable. insufficient information to correctly place point'],
    30: [1, 'moved downstream to match map and discharge (7.37 m3/s) in report'],
    31: [1, 'moved to Durance at Mirabeau'],
    32: [1, 'moved to Rhone. CS had moved it to Saone. But discharges does not match'],
    34: [-1, 'wrongly placed', 25],
    34: [-1, 'wrongly placed', 26],
    34: [0, 'Point placed by CS in the appropriate approximate location based on report. But MAR provided is wrong, '
            'in cubic meters per second', 27],
    41: [1, 'Moved to nearest river reach on main stem'],
    42: [0, 'Seems unreliable. No source information. No method description'],
    43: [1, 'Moved to Rizana instead of Badasevica'],
    51: [0, 'Upstream catchment area and MAR in comments do not match either HydroRivers or site no 54'],
    65: [1, 'Move closer to HydroRIVERS'],
    70: [1, 'Moved 1 km downstream so that it is closest to the right reach in HydroRivers. Odd contrats in MAR with downstream site.'],
    73: [1, 'Moved downstream of Koekedou dam at Ceres based on site name and discharge'],
    74: [1, 'Moved downstream of Koekedou dam at Ceres based on site name and discharge'],
    97: [1, 'Moved to mainstem'],
    104: [1, 'Moved to mainstem downstream of confluence to match upstream catchment area'],
    123: [1, 'Moved to mainstem'],
    137: [1, 'Moved closer to mainstem  for spatial join'],
    144: [1, 'Moved downstream of confluence'],
    156: [1, 'Mean Annual Runoff was reversed with site 157. Correct error.'],
    205: [1, 'Moved downstream of confluence and lake. Better matches discharge and logic'],
    421: [-1, 'Point created by CS in Senegal']
}

arcpy.AddField_management(EFpoints_joinedit, 'Point_shift_mathis', 'SHORT')
arcpy.AddField_management(EFpoints_joinedit, 'Comment_mathis', 'TEXT')

with arcpy.da.UpdateCursor(EFpoints_joinedit, ['no', 'Point_shift_mathis', 'Comment_mathis', 'OBJECTID']) as cursor:
    for row in cursor:
        if row[0] in editdict:
            if len(editdict[row[0]]) == 2:
                row[1] = editdict[row[0]][0] #Point_shift_mathis = first entry in dictionary
                row[2] = editdict[row[0]][1] #Comment_mathis = second entry in dictionary
            else:
                if row[3] in [25, 26]:
                    row[1] = -1
                    row[2] = 'Wrongly placed'
        else:
            row[1] = 0
        cursor.updateRow(row)

#---------------------------------- FORMATTING OF SITES FROM OCT 28th 2021 DATABASE FREEZE -------------------------------
#Create points for those with valid


#Join EF points to nearest river reach in RiverAtlas
if not arcpy.Exists(EFpoints_joinedit):
    print('Join EF points to nearest river reach in RiverAtlas')
    arcpy.SpatialJoin_analysis(EFpoints_cscopy, hydroriv, EFpoints_joinedit,
                               join_operation='JOIN_ONE_TO_ONE', join_type="KEEP_COMMON",
                               match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                               distance_field_name='EFpoint_hydroriv_distance')

    arcpy.AddField_management(EFpoints_joinedit, field_name='DApercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints_joinedit, ['Upstream_C', 'UPLAND_SKM', 'DApercdiff']) as cursor:
        for row in cursor:
            if re.match('^[0-9]+$', string = row[0]):
                if int(row[0]) > 0L:
                    row[2] = (float(row[1]) - float(row[0]))/float(row[0])
            cursor.updateRow(row)

    arcpy.AddField_management(EFpoints_joinedit, field_name='totalAF', field_type='FLOAT')
    arcpy.AddField_management(EFpoints_joinedit, field_name='MAFpercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints_joinedit, ['Mean_Annua', 'DIS_AV_CMS', 'MAFpercdiff', 'totalAF']) as cursor:
        for row in cursor:
            if re.match('^[0-9]+\.*[0-9]*', string = row[0]):
                if float(row[0]) > 0:
                    row[2] = (31.557600*float(row[1]) - float(row[0]))/float(row[0])
                    row[3] = (31.557600 * float(row[1]))
            cursor.updateRow(row)

#Manual editing
#no, what is done (delete: -1, moved: 1), comment, OBJECTID
editdict = {
    0: [-1, 'artefact without data. It seems to correspond to Macalister/Below Lake Glenmaggie though'],
}

arcpy.AddField_management(EFpoints_joinedit, 'Point_shift_mathis', 'SHORT')
arcpy.AddField_management(EFpoints_joinedit, 'Comment_mathis', 'TEXT')

with arcpy.da.UpdateCursor(EFpoints_joinedit, ['no', 'Point_shift_mathis', 'Comment_mathis', 'OBJECTID']) as cursor:
    for row in cursor:
        if row[0] in editdict:
            if len(editdict[row[0]]) == 2:
                row[1] = editdict[row[0]][0] #Point_shift_mathis = first entry in dictionary
                row[2] = editdict[row[0]][1] #Comment_mathis = second entry in dictionary
            else:
                if row[3] in [25, 26]:
                    row[1] = -1
                    row[2] = 'Wrongly placed'
        else:
            row[1] = 0
        cursor.updateRow(row)


#---------------------------------- MERGE AND FORMAT ALL SITES -----------------------------------------------------------

#Delete sites, clean fields
if not arcpy.Exists(EFpoints_clean):
    arcpy.CopyFeatures_management(EFpoints_joinedit, EFpoints_clean)
    with arcpy.da.UpdateCursor(EFpoints_clean, ['Point_shift_mathis']) as cursor:
        for row in cursor:
            if row[0] == -1:
                cursor.deleteRow()

    #Delete duplicates
    arcpy.DeleteIdentical_management(EFpoints_clean, ['no'])

    #Delete useless fields
    for f1 in arcpy.ListFields(EFpoints_clean):
        if f1.name not in [f2.name for f2 in arcpy.ListFields(EFpoints_cscopy)]+['Point_shift_mathis', 'Comment_mathis']:
            arcpy.DeleteField_management(EFpoints_clean, f1.name)

#Snap to river network
snapenv = [[hydroriv, 'EDGE', '1000 meters']]
arcpy.Snap_edit(EFpoints_clean, snapenv)

#Join to RiverATLAS
EFpoints_cleanjoin = os.path.join(process_gdb, 'EFpoints_cleanjoin')

arcpy.SpatialJoin_analysis(EFpoints_clean, riveratlas, EFpoints_cleanjoin, join_operation='JOIN_ONE_TO_ONE',
                           join_type="KEEP_COMMON",
                           match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                           distance_field_name='station_river_distance')


#Extract HydroSHEDS and WaterGAP layers
ExtractMultiValuesToPoints(in_point_features=EFpoints_cleanjoin, in_rasters=[DA_grid, MAF_nat_grid, MAF_ant_grid],
                           bilinear_interpolate_values='NONE')

#Add monthly discharge associated with reaches
#Get all unique HYRIV_ID associated with the points
hyrividset = {row[0] for row in arcpy.da.SearchCursor(EFpoints_cleanjoin, 'HYRIV_ID')}
#Get field names
mdis_fnames = ['DIS_{0}_CMS'.format(format(x, '02')) for x in range(1, 13)]
for newf in mdis_fnames:
    arcpy.AddField_management(EFpoints_cleanjoin, newf)

#Get monthly discharge values for points
mdisdict = {row[0]: [row[x] for x in range(1, 13)]
            for row in arcpy.da.SearchCursor(monthlyF_net, ['REACH_ID']+mdis_fnames)
            if row[0] in hyrividset}

#Write values to points
with arcpy.da.UpdateCursor(EFpoints_cleanjoin, ['HYRIV_ID']+mdis_fnames) as cursor:
    for row in cursor:
        if row[0] in mdisdict:
            row = tuple([row[0]]+mdisdict[row[0]])
            cursor.updateRow(row)

#Extract Biodiversity Threat Index at site point
EMC_GEFISatsite = arcpy.da.TableToNumPyArray(
    Sample(in_rasters=EMC_GEFIS, in_location_data=EFpoints_cleanjoin,
                    out_table=os.path.join(process_gdb, 'EMC_GEFIS_efsites'),
                    resampling_type= 'NEAREST', unique_id_field = 'no'),
    ['EFpoints_cleanjoin', 'EMC_10Variable_2_Band_1']
)

EMC_GEFISatsite_dict = {i:j for i,j in EMC_GEFISatsite}

arcpy.AddField_management(EFpoints_cleanjoin, 'ecpresent_gefis_atsite', 'float')
with arcpy.da.UpdateCursor(EFpoints_cleanjoin, ['no', 'ecpresent_gefis_atsite']) as cursor:
    for row in cursor:
        if row[0] in EMC_GEFISatsite:
            row[1] = EMC_GEFISatsite_dict[row[0]]
            cursor.updateRow(row)

#Add coordinates
arcpy.AddGeometryAttributes_management(EFpoints_cleanjoin, Geometry_Properties='POINT_X_Y_Z_M')

#Export riveratlas attributes to csv file
if not arcpy.Exists(riveratlas_csv):
    print('Exporting CSV table of RiverATLAS v1.0 attributes')
    arcpy.CopyRows_management(in_rows = riveratlas, out_table=riveratlas_csv)

#Get formatted GRDC stations from global non-perennial rivers project (Messager et al. 2021) - doesn't work, did it manually
arcpy.CopyFeatures_management(in_features = 'D://globalIRmap/results/GRDCstations_predbasic800.gpkg/GRDCstations_predbasic800',
                              out_feature_class= os.path.join(process_gdb, 'GRDCstations_predbasic800'))