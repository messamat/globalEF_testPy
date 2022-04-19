import arcpy

from GEFIS_setup import *

#Input data - 2021 Master database
csdat_1 = os.path.join(datdir, 'Formatted_data_Chandima_20211018') #Data folder from Chandima Subasinghe (CS)
EFpoints_cs1 = os.path.join(csdat_1, "Combine_shiftedpoints.shp")

csdat_2 = os.path.join(datdir, 'Formatted_data_Chandima_20211102')
EFpoints_cs2 = [os.path.join(csdat_2, cspath) for cspath in
                ['India_1.shp', 'Lesotho_1.shp', 'SA_Botswana_Zimbabwe_1.shp', 'South_africa_1.shp']]

EFpoints_1028freeze = os.path.join(resdir, 'Master_20211104_parzered_notIWMI.csv')

#Input data - Mexico
EFtab_Mexico = os.path.join(resdir, 'mexico_refdata_preformatted.csv')
basins_Mexico = os.path.join(datdir, 'GEFIS_test_data', 'Data by Country', 'Mexico',
                             'Cuencas_hidrolOgicas_que_cuentan_con_reserva', 'Cuencas_hidrologicas_Reservas_Act757.shp')

#Input data - General
wgs84_epsg = 4326
hydroriv = os.path.join(datdir, 'HydroRIVERS_v10.gdb', 'HydroRIVERS_v10') #Download from https://www.hydrosheds.org/page/hydrorivers
up_area = os.path.join(datdir, 'upstream_area_skm_15s.gdb', 'up_area_skm_15s')

#Outputs - 2021 Master database
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
pathcheckcreate(process_gdb)
EFpoints1_cscopy = os.path.join(process_gdb, 'Combine_shiftedpoints_copy')
EFpoints1_joinedit = os.path.join(process_gdb, 'EFpoints1_joinedit')
EFpoints1_clean = os.path.join(process_gdb, 'EFpoints1_clean')
EFpoints1_cleanjoin = os.path.join(process_gdb, 'EFpoints1_cleanjoin')

EFpoints2_cscopy = os.path.join(process_gdb, 'EFpoints2_merge')
EFpoints2_joinedit = os.path.join(process_gdb, 'EFpoints2_joinedit')
EFpoints2_clean = os.path.join(process_gdb, 'EFpoints2_clean')
EFpoints2_cleanjoin = os.path.join(process_gdb, 'EFpoints2_cleanjoin')

EFpoints_1028notIWMI_raw = os.path.join(process_gdb, 'Master_20211104_parzered_notIWMI_raw')
EFpoints_1028notIWMI_joinedit = os.path.join(process_gdb, 'Master_20211104_parzered_notIWMI_joinedit')

#Outputs - Mexico
EFbasins_Mexico = os.path.join(process_gdb, 'EFbasins_Mexico')
EFbasins_Mexico_wgs84 = os.path.join(process_gdb, 'EFbasins_Mexico_wgs84')
EFbasins_ptraw_Mexico = os.path.join(process_gdb, 'EFbasins_ptraw_Mexico')
EFbasins_ptraw_attri_Mexico = os.path.join(process_gdb, 'EFbasins_ptraw_attri_Mexico')
EFbasins_ptjointedit_Mexico = os.path.join(process_gdb, 'EFbasins_ptjoinedit_attri_Mexico')

#Outputs - General
EFpoints_1028_merge = os.path.join(process_gdb, 'EFpoints_20211104_merge')
EFpoints_1028_clean = os.path.join(process_gdb, 'EFpoints_20211104_clean')

#---------------------------------- FORMAT MEXICAN SITES ---------------------------------------------------------------
#Link EF tab from Salinas-Rodriguez et al. 2021 to basin shapefile
if not arcpy.Exists(EFbasins_Mexico):
    arcpy.MakeFeatureLayer_management(basins_Mexico, 'basins_Mexico_layer')
    arcpy.AddJoin_management('basins_Mexico_layer', in_field = 'id_cuenca',
                            join_table = EFtab_Mexico, join_field = 'E_flow_Location_Name_No_', join_type='KEEP_COMMON')
    arcpy.CopyFeatures_management('basins_Mexico_layer', EFbasins_Mexico)
    arcpy.Project_management(in_dataset=EFbasins_Mexico,
                             out_dataset=EFbasins_Mexico_wgs84, out_coor_system=4326)

#Compute basin areas
arcpy.AddGeometryAttributes_management(EFbasins_Mexico_wgs84,
                                       Geometry_Properties='AREA_GEODESIC',
                                       Area_Unit='SQUARE_KILOMETERS')

#Convert EF basins to pour points to link with RiverATLAS (Mexican basins do not overlaps with HydroBASINS)
basin_maxarea = ZonalStatistics(in_zone_data=EFbasins_Mexico_wgs84,
                                zone_field='E_flow_Location_Name_No_',
                                in_value_raster=up_area,
                                statistics_type='MAXIMUM',
                                ignore_nodata='DATA')
basin_prpt1 = Con(up_area==basin_maxarea, basin_maxarea)
arcpy.RasterToPoint_conversion(basin_prpt1, EFbasins_ptraw_Mexico, raster_field='Value')
arcpy.SpatialJoin_analysis(EFbasins_ptraw_Mexico, EFbasins_Mexico_wgs84, EFbasins_ptraw_attri_Mexico,
                           join_operation='JOIN_ONE_TO_ONE',
                           join_type='KEEP_COMMON',
                           match_option='WITHIN')

#Join EF points to nearest river reach in RiverAtlas
if not arcpy.Exists(EFbasins_ptjointedit_Mexico):
    print('Join Mexican EF points to nearest river reach in RiverAtlas')
    arcpy.SpatialJoin_analysis(EFbasins_ptraw_attri_Mexico, hydroriv, EFbasins_ptjointedit_Mexico,
                               join_operation='JOIN_ONE_TO_ONE', join_type="KEEP_COMMON",
                               match_option='CLOSEST_GEODESIC', search_radius=0.01,
                               distance_field_name='EFpoint_hydroriv_distance')

    arcpy.AddField_management(EFbasins_ptjointedit_Mexico, field_name='DApercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFbasins_ptjointedit_Mexico, ['AREA_GEO', 'UPLAND_SKM', 'DApercdiff']) as cursor:
        for row in cursor:
            if int(row[0]) > 0L:
                row[2] = (float(row[1]) - float(row[0]))/float(row[0])
            cursor.updateRow(row)

#Exclusions here are only for the sake of comparing with RiverATLAS
editdict_mexico = {
    1227: [-1, 'Mexican e-flow basin does not match hydrological network'],
    1235: [1, 'Move to mainstem'],
    1243: [1, 'Move to mainstem'],
    1249: [1, 'Move to correct segment'],
    1401: [1, 'Move to mainstem'],
    1502: [-1, 'Coastal basin encompassing other tributaries'],
    1505: [-1, 'Coastal basin encompassing other tributaries'],
    1507: [-1, 'Coastal basin encompassing other tributaries'],
    1509: [-1, 'Coastal basin encompassing other tributaries'],
    2005: [1, 'Move to mainstem'],
    2009: [-1, 'Coastal basin encompassing other tributaries'],
    2014: [1, 'Move downstream to include all tributaries in Mexican e-flow basin'],
    2015: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2020: [1, 'Move to mainstem'],
    2025: [-1, 'Coastal basin encompassing other tributaries'],
    2027: [-1, 'Coastal basin encompassing other tributaries'],
    2030: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2518: [-1, 'HydroRIVER segment lies almost fully outside of Mexican e-flow basin. Coastal basin'],
    2529: [1, 'Move to correct segment'],
    2533: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2540: [-1, 'Coastal basin encompassing other tributaries'],
    2541: [-1, 'Wrong river segment. Coastal basin encompassing other tributaries'],
    2606: [1, 'Move to mainstem'],
    2622: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2627: [-1, 'Mexican e-flow basin includes headwaters from other HydroSHEDS basins'],
    2630: [-1, 'Mexican e-flow basin includes headwaters from other HydroSHEDS basins'],
    2634: [-1, 'Mexican e-flow basin is missing headwaters'],
    2644: [-1, 'Mexican e-flow basin includes headwaters from various other HydroSHEDS basins'],
    2655: [-1, 'Mexican e-flow basin does not match hydrological network'],
    2657: [1, 'Move to mainstem'],
    2661: [-1, 'Mexican e-flow basin includes headwaters from other HydroSHEDS basins'],
    2663: [-1, 'Coastal basin encompassing other tributaries'],
    2803: [1, 'Move downstream to include large tributary in Mexican e-flow basin'],
    2805: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2807: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2808: [-1, 'Mexican e-flow basin includes headwaters from other HydroSHEDS basins'],
    2809: [1, 'Move to correct segment'],
    2810: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2811: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    2812: [-1, 'Coastal basin encompassing other tributaries'],
    2814: [1, 'Move to correct segment'],
    2818: [-1, 'Coastal basin encompassing other tributaries'],
    2914: [1, 'Move to mainstem'],
    2915: [-1, 'Coastal basin encompassing other tributaries'],
    3008: [1, 'Move to mainstem'],
    3010: [-1, 'Mexican e-flow basin includes headwaters from other HydroSHEDS basins'],
    3031: [-1, 'Mexican e-flow basin includes tributaries from other HydroSHEDS basins'],
    3033: [-1, 'Mexican e-flow basin does not match hydrological network'],
    3034: [-1, 'Mexican e-flow basin does not match hydrological network'],
    3036: [-1, 'Mexican e-flow basin does not match hydrological network'],
    3037: [1, 'Move downstream to include tributary in Mexican e-flow basin'],
    3041: [1, 'Move to correct segment'],
    3044: [1, 'Move upstream to only include tributaries in Mexican e-flow basin'],
    3047: [-1, 'Mexican e-flow basin does not match hydrological network'],
    3048: [-1, 'Mexican e-flow basin does not match hydrological network'],
    3049: [1, 'Move to correct segment'],
    3052: [1, 'Move to correct segment'],
    3057: [1, 'Move to mainstem'],
    3061: [-1, 'Mexican e-flow basin includes tributaries from other HydroSHEDS basins'],
    3062: [1, 'Move downstream to include tributary in Mexican e-flow basin'],
    3065: [-1, 'Mexican e-flow basin includes tributaries from other HydroSHEDS basins'],
    3068: [1, 'Move upstream to tributary'],
    3069: [1, 'Move upstream to tributary'],
    3075: [-1, 'Mexican e-flow basin includes tributaries from other HydroSHEDS basins'],
    3076: [-1, 'Mexican e-flow basin includes tributaries from other HydroSHEDS basins'],
    3078: [-1, 'Coastal basin encompassing other tributaries'],
    3080: [-1, 'Coastal basin encompassing other tributaries'],
    3081: [-1, 'Wrongly placed. Coastal basin encompassing other tributaries']
}

############## TO CONTINUE ##################
arcpy.AddField_management(EFbasins_ptjointedit_Mexico, 'Point_shift_mathis', 'SHORT')
arcpy.AddField_management(EFbasins_ptjointedit_Mexico, 'Comment_mathis', 'TEXT')

with arcpy.da.UpdateCursor(EFpoints1_joinedit, ['id_cuenca', 'Point_shift_mathis', 'Comment_mathis', 'OBJECTID']) as cursor:
    for row in cursor:
        if row[0] in editdict_mexico:
            row[1] = editdict_mexico[row[0]][0] #Point_shift_mathis = first entry in dictionary
            row[2] = editdict_mexico[row[0]][1] #Comment_mathis = second entry in dictionary
        else:
            row[1] = 0
        cursor.updateRow(row)

#---------------------------------- FORMATTING OF SITES FROM CS / FIRST BATCH --------------------------------------------------
#Copy CS points
if not arcpy.Exists(EFpoints1_cscopy):
    arcpy.CopyFeatures_management(EFpoints_cs1, EFpoints1_cscopy)

#Join EF points to nearest river reach in RiverAtlas
if not arcpy.Exists(EFpoints1_joinedit):
    print('Join EF points to nearest river reach in RiverAtlas')
    arcpy.SpatialJoin_analysis(EFpoints1_cscopy, hydroriv, EFpoints1_joinedit,
                               join_operation='JOIN_ONE_TO_ONE', join_type="KEEP_COMMON",
                               match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                               distance_field_name='EFpoint_hydroriv_distance')

    arcpy.AddField_management(EFpoints1_joinedit, field_name='DApercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints1_joinedit, ['Upstream_C', 'UPLAND_SKM', 'DApercdiff']) as cursor:
        for row in cursor:
            if re.match('^[0-9]+$', string = row[0]):
                if int(row[0]) > 0L:
                    row[2] = (float(row[1]) - float(row[0]))/float(row[0])
            cursor.updateRow(row)

    arcpy.AddField_management(EFpoints1_joinedit, field_name='totalAF', field_type='FLOAT')
    arcpy.AddField_management(EFpoints1_joinedit, field_name='MAFpercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints1_joinedit, ['Mean_Annua', 'DIS_AV_CMS', 'MAFpercdiff', 'totalAF']) as cursor:
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
    39: [1, 'Move to Matsoku River itself as described in Arthington et al. 2003. Unsure of exact location'],
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

arcpy.AddField_management(EFpoints1_joinedit, 'Point_shift_mathis', 'SHORT')
arcpy.AddField_management(EFpoints1_joinedit, 'Comment_mathis', 'TEXT')

with arcpy.da.UpdateCursor(EFpoints1_joinedit, ['no', 'Point_shift_mathis', 'Comment_mathis', 'OBJECTID']) as cursor:
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

#---------------------------------- FORMATTING OF SITES FROM CS / SECOND BATCH --------------------------------------------------
#Copy CS points
if not arcpy.Exists(EFpoints2_cscopy):
    arcpy.Merge_management(EFpoints_cs2, EFpoints2_cscopy)

#Join EF points to nearest river reach in RiverAtlas
if not arcpy.Exists(EFpoints2_joinedit):
    print('Join EF points to nearest river reach in RiverAtlas')
    arcpy.SpatialJoin_analysis(EFpoints2_cscopy, hydroriv, EFpoints2_joinedit,
                               join_operation='JOIN_ONE_TO_ONE', join_type="KEEP_COMMON",
                               match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                               distance_field_name='EFpoint_hydroriv_distance')

    arcpy.AddField_management(EFpoints2_joinedit, field_name='totalAF', field_type='FLOAT')
    arcpy.AddField_management(EFpoints2_joinedit, field_name='MAFpercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints2_joinedit, ['Natural_Na', 'DIS_AV_CMS', 'MAFpercdiff', 'totalAF']) as cursor:
        for row in cursor:
            if row[0] is not None:
                if float(row[0]) > 0:
                    row[2] = (31.557600*float(row[1]) - float(row[0]))/float(row[0])
                    row[3] = (31.557600 * float(row[1]))
            cursor.updateRow(row)

#Manual editing
#E_flow_Loc, what is done (delete: -1, moved: 1), comment
editdict2= {
    'Kaudiyala': [1, 'Moved to mainstem to overlap with HydroRIVERS. Site was correctly located'],
    'IFR P2': [1, 'Moved to mainstem'],
    'H9GOUK-EWR2': [1, 'Moved to mainstem. Matches river name and MAR better rather than tributary'],
    'EWR_Mooi_N3': [1, 'Moved to mainstem'],
    'Thukela_EWR3': [1, 'On wrong river. Moved South by 50 km'],
    'THU_EWR13A': [1, 'Moved to mainstem'],
    'Vaal_EWR 15': [1, 'Moved to mainstem'],
    'Letaba_EWR1': [1, 'Moved to mainstem'],
    'Vaal_EWR 2': [1, 'Moved to maistem'],
    'Vaal_EWR 9': [1, 'Moved to mainstem'],
    'EWR S1': [1, 'Moved to most probable reach on Schoonspruit River'],
    'CROC_EWR 2': [1, 'Moved downstream of confluence'],
    'MAR_EWR 1': [1, 'Moved to mainstem'],
    'MAR_EWR 2': [1, 'Moved to mainstem on Groot Marico'],
    'CROC_EWR 14': [-1, 'Duplicate coordinate. On Elands.'],
    'MAT_EWR1': [1, 'Moved to mainstem'],
    'MOK_EWR10': [1, 'Moved to mainstem'],
    'Olifants_EWR 14b': [1, 'Moved to mainstem'],
    'Olifants_SPE1': [1, 'Moved closer to maintem'],
    'EWR K1': [1, 'On wrong river. Moved by 100s of kms'],
    'EWR K2': [1, 'On wrong river. Moved by 100s of kms'],
    'EWR T1': [1, 'On wrong river. Moved by 100s of kms'],
    'EWR K3A': [1, 'On wrong river. Moved by 100s of kms'],
    'EWR L1': [1, 'On wrong river. Moved by 100s of kms'],
    'EWR KG1': [1, 'On wrong river. Moved by 100s of kms']
}

arcpy.AddField_management(EFpoints2_joinedit, 'Point_shift_mathis', 'SHORT')
arcpy.AddField_management(EFpoints2_joinedit, 'Comment_mathis', 'TEXT')

with arcpy.da.UpdateCursor(EFpoints2_joinedit, ['E_flow_Loc', 'Point_shift_mathis', 'Comment_mathis', 'River_']) as cursor:
    for row in cursor:
        if row[0] in editdict2:
            if row[0] == 'EWR S1' and row[3] == 'Sabie':
                row[1] = row[0]
            row[1] = editdict2[row[0]][0] #Point_shift_mathis = first entry in dictionary
            row[2] = editdict2[row[0]][1] #Comment_mathis = second entry in dictionary
        else:
            row[1] = 0
        cursor.updateRow(row)

#---------------------------------- FORMATTING OF SITES FROM OCT 28th 2021 DATABASE FREEZE -------------------------------
#Create points for those with valid coordinates (see merge_dbversions.r)
arcpy.MakeXYEventLayer_management(table = EFpoints_1028freeze,
                                  in_x_field = 'longitude_parzer',
                                  in_y_field = 'latitude_parzer',
                                  out_layer = 'ef1028lyr',
                                  spatial_reference= 4326,
                                  in_z_field = None)
arcpy.CopyFeatures_management('ef1028lyr', EFpoints_1028notIWMI_raw)

#Join EF points to nearest river reach in RiverAtlas
if not arcpy.Exists(EFpoints_1028notIWMI_joinedit):
    print('Join EF points to nearest river reach in RiverAtlas')
    arcpy.SpatialJoin_analysis(EFpoints_1028notIWMI_raw, hydroriv, EFpoints_1028notIWMI_joinedit,
                               join_operation='JOIN_ONE_TO_ONE', join_type="KEEP_COMMON",
                               match_option='CLOSEST_GEODESIC', search_radius=0.0005,
                               distance_field_name='EFpoint_hydroriv_distance')

    arcpy.AddField_management(EFpoints_1028notIWMI_joinedit, field_name='totalAF', field_type='FLOAT')
    arcpy.AddField_management(EFpoints_1028notIWMI_joinedit, field_name='MAFpercdiff', field_type='FLOAT')
    with arcpy.da.UpdateCursor(EFpoints_1028notIWMI_joinedit, ['Natural_Naturalised_Mean_Annual_Runoff_at_E_flow_Location',
                                                               'mar_unit', 'DIS_AV_CMS', 'MAFpercdiff',
                                                               'totalAF']) as cursor:
        for row in cursor:
            #if re.match('^[0-9]+\.*[0-9]*', string = row[0]):
            if row[0] is not None:
                if float(row[0]) > 0:
                    if row[1] == '10^6m3 y-1':
                        row[3] = (31.557600*float(row[2]) - float(row[0]))/float(row[0])
                        row[4] = (31.557600 * float(row[2]))
                    elif row[1] == 'm3 s-1':
                        row[3] = (float(row[2]) - float(row[0])) / float(row[0])
                        row[4] = row[2]
                    cursor.updateRow(row)

#Manual editing
#no, what is done (delete: -1, moved: 1), comment, OBJECTID
editdict = {
    418: [1, 'Coordinates from report are misleading. Map shows site at Jebel Aulia upstream of Khartoum as mentioned'
             'in table, but table "site" says Malakal which is > 500 km upstream. Moved to just upstream of jebel Aulia. '
             'Also matches MAR better.'],
    421: [1, 'Moved just upstream of Lake Nasser to match map and description in report + MAR'],
    445: [1, 'Moved to river outlet to better match MAR (and description)'],
    446: [1, 'Moved to river outlet to better match MAR (and description)'],
    447: [1, 'Moved to river outlet to better match MAR (and description)'],
    463: [1, 'Moved to mainstem'],
    471: [1, 'Moved to mainstem'],
    506: [1, 'Moved to mainstem'],
    507: [1, 'Moved to mainstem'],
    508: [1, 'Moved to correct tributary Saint-Louis'],
    516: [0, 'Not well represented in HydroRIVERS', None],
    530: [0, 'On same HydroRivers reach as 531'],
    531: [1, 'Moved to correct tributary des Eaux Volées'],
    532: [0, 'On same HydroRivers reach as 531'],
    534: [1, 'Moved to mainstem'],
    539: [0, 'Not well represented in HydroRIVERS', None],
    567: [1, 'Moved to mainstem'],
    574: [1, 'Moved to mainstem'],
    592: [1, 'Moved to mainstem'],
    593: [1, 'Moved to mainstem'],
    594: [1, 'Moved to mainstem'],
    595: [1, 'Moved to mainstem'],
    630: [1, 'Moved to correct tributary Mienia'],
    636: [1, 'Moved to correct tributary Mienia'],
    642: [1, 'Moved to correct tributary Mienia'],
    648: [1, 'Moved to correct tributary Mienia']
}

for id in range(208, 413)+[439, 440, 441]:
    editdict[id] = [-1, None]

arcpy.AddField_management(EFpoints_1028notIWMI_joinedit, 'Point_shift_mathis', 'SHORT')
arcpy.AddField_management(EFpoints_1028notIWMI_joinedit, 'Comment_mathis', 'TEXT')

with arcpy.da.UpdateCursor(EFpoints_1028notIWMI_joinedit, ['EFUID', 'Point_shift_mathis', 'Comment_mathis', 'HYRIV_ID']) as cursor:
    for row in cursor:
        if row[0] in editdict:
            row[1] = editdict[row[0]][0] #Point_shift_mathis = first entry in dictionary
            row[2] = editdict[row[0]][1] #Comment_mathis = second entry in dictionary
            if len(editdict[row[0]]) == 3:
                row[3] = None
        else:
            row[1] = 0
        cursor.updateRow(row)

arcpy.DeleteField_management(EFpoints_1028notIWMI_joinedit, 'Comments') #Cannot merge to others because special character, so delete field

#---------------------------------- MERGE AND FORMAT ALL SITES -----------------------------------------------------------
#Clean and merge datasets
arcpy.Merge_management([EFpoints1_joinedit, EFpoints2_joinedit, EFpoints_1028notIWMI_joinedit], EFpoints_1028_merge)

#Delete sites, clean fields
arcpy.CopyFeatures_management(EFpoints_1028_merge, EFpoints_1028_clean)
with arcpy.da.UpdateCursor(EFpoints_1028_clean, ['Point_shift_mathis']) as cursor:
    for row in cursor:
        if row[0] == -1:
            cursor.deleteRow()

#Delete duplicates
arcpy.DeleteIdentical_management(EFpoints_1028_clean, ['no', 'HYRIV_ID', 'Id', 'EFUID'])

#Delete useless fields
for f1 in arcpy.ListFields(EFpoints_1028_clean):
    if f1.name not in ['OBJECTID_1', 'Shape', 'no', 'Id', 'EFUID', 'Country', 'Point_shift_mathis', 'Comment_mathis']:
        arcpy.DeleteField_management(EFpoints_1028_clean, f1.name)

#Create new points for those that did not have coordinates
newpts_1028 = {
    397: arcpy.Point(-9.694778, 10.614154),
    398: arcpy.Point(-8.3025, 12.0062),
    399: arcpy.Point(-7.5544, 12.8683),
    400: arcpy.Point(-4.5164, 13.8097), #Djenné Dam near Soala
    401: arcpy.Point(-5.3548, 13.9564), #Ké-Macina
    402: arcpy.Point(-4.1995, 14.5244),
    403: arcpy.Point(29.2433, -9.1436), #No indication of actual location in database. Decided based on MAR, downstream of planned hydrpower at  Kundabwika Falls and just upstream of confluence with outflow from Lake Mweru Wantipa
    423: arcpy.Point(35.019201, -1.545872), #Purungat Bridge. Not
    424: arcpy.Point(35.0842, -1.4229), #Chemomul Bridge slightly upstream of Bomet
    425: arcpy.Point(35.3519, -0.7754), #Located near Rekero Camp tourist lodge, just downstream of confluence of Talek and Lemek Subcatchments
    426: arcpy.Point(35.4373, -0.8988), #Kapkimolwa Bridge
    442: arcpy.Point(84.6973, 27.5611), #Downstream of the confluence of East Rapti and Lothar rivers
    443: arcpy.Point(84.4884, 27.5781), #Downstream of the confluence of the East Rapti River and Khageri khola; Entry to Royal Chitwan National Park (a bit downstream to match network)
    444: arcpy.Point(84.1676, 27.5572) #Upstream of confluence of Narayani and East Rapti rivers; on East Rapti
}

# Open an InsertCursor and insert the new geometry
with arcpy.da.InsertCursor(EFpoints_1028_clean, ['EFUID', 'SHAPE@']) as cursor:
    for k in newpts_1028:
        cursor.insertRow((k, newpts_1028[k]))
with arcpy.da.InsertCursor(EFpoints_1028_clean, ['no', 'SHAPE@']) as cursor:
    cursor.insertRow((40, arcpy.Point(28.1006, -29.4591)))

#Snap to river network
snapenv = [[hydroriv, 'EDGE', '1000 meters']]
arcpy.Snap_edit(EFpoints_1028_clean, snapenv)

#Add coordinates
arcpy.AddGeometryAttributes_management(EFpoints_1028_clean, Geometry_Properties='POINT_X_Y_Z_M')

#Get formatted GRDC stations from global non-perennial rivers project (Messager et al. 2021) - doesn't work, did it manually
gaugesp_out = os.path.join(process_gdb, 'GRDCstations_predbasic800')
if not arcpy.Exists(gaugesp_out):
    arcpy.CopyFeatures_management(in_features = 'D://globalIRmap/results/GRDCstations_predbasic800.gpkg/GRDCstations_predbasic800',
                                  out_feature_class= os.path.join(process_gdb, 'GRDCstations_predbasic800'))