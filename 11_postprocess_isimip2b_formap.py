from globalEF_comparison_setup import * #See this module for directory structure

#Hydrographic data to extract EFs to
hydrorivers_links = os.path.join(datdir, 'HydroATLAS', 'HydroATLAS_Geometry', 'Link_shapefiles',
                                 'link_hyriv_v10.gdb', 'link_hyriv_v10') #HydroRIVERS vector network
hydrorivers_prpts = os.path.join(datdir, 'HydroATLAS', 'HydroATLAS_Geometry', 'Link_zone_grids',
                     'link_streams.gdb', 'link_str_pnt') #raster of stream pourpoints

#EF outputs to extract
isimp_qtotacc15s_gdb = os.path.join(resdir, 'isimp2_qtot_accumulated15s.gdb') #GDB to contain downsampled rasters
example_maf = os.path.join(isimp_qtotacc15s_gdb,
                           'watergap2_2c_gfdl_esm2m_picontrol_1860soc_qtot_maf_qtot_acc15s')
example_vmf = os.path.join(isimp_qtotacc15s_gdb,
                           'watergap2_2c_gfdl_esm2m_picontrol_1860soc_qtot_allefbutsmakhtin_q90q50_acc15s')

#Outputs
mapping_outputs_gdb = os.path.join(resdir, 'mapping_outputs.gdb')
pathcheckcreate(mapping_outputs_gdb)
out_maf_tab = os.path.join(mapping_outputs_gdb, 'watergap2_2c_gfdl_esm2m_maf_hydrorivers')
out_ef_tab = os.path.join(mapping_outputs_gdb, 'watergap2_2c_gfdl_esm2m_ef_q90q50_hydrorivers')

#Extract maf
if not arcpy.Exists(out_maf_tab):
    ZonalStatisticsAsTable(in_zone_data=hydrorivers_prpts,
                           zone_field='Value',
                           in_value_raster=example_maf,
                           out_table=out_maf_tab,
                           ignore_nodata='DATA', statistics_type='Mean')

#Extract e-flows as volume for e.g., VMF
if not arcpy.Exists(out_ef_tab):
    ZonalStatisticsAsTable(in_zone_data=hydrorivers_prpts,
                           zone_field='Value',
                           in_value_raster=example_vmf,
                           out_table=out_ef_tab,
                           ignore_nodata='DATA', statistics_type='Mean')

join_dict = {row[0]: [row[1]] for row in arcpy.da.SearchCursor(out_maf_tab, ['Value', 'MEAN'])}
for row in arcpy.da.SearchCursor(out_ef_tab, ['Value', 'MEAN']):
    join_dict[row[0]].append(row[1])

#Export by river size
for dis in reversed([[0, 0.1], [0.1, 1], [1, 10], [10, 100], [100, 1000], [1000, 10000], [10000, 1000000]]): #Reversed list to debug more easily
    subriver_out = os.path.join(mapping_outputs_gdb,
                                '{0}_DIS{1}_{2}'.format(os.path.split(hydrorivers_links)[1],
                                                        re.sub('[.]', '', str(dis[0])),
                                                        re.sub('[.]', '', str(dis[1]))))
    sqlexp = 'DIS_AV_CMS >= {0} AND DIS_AV_CMS <= {1}'.format(dis[0], dis[1])
    print(sqlexp)
    if not arcpy.Exists(subriver_out):
        arcpy.MakeFeatureLayer_management(hydrorivers_links, out_layer='subriver', where_clause=sqlexp)
        arcpy.CopyFeatures_management('subriver', subriver_out)
        arcpy.Delete_management('subriver')
    else:
        print('{} already exists. Skipping copy...'.format(subriver_out))

    #Join to each layer, then compute percentage
    subriver_fields = [f.name for f in arcpy.ListFields(subriver_out)]
    if 'watergap2_2c_gfdl_esm2m_picontrol_1860soc_maf' not in subriver_fields:
        arcpy.AddField_management(subriver_out, 'watergap2_2c_gfdl_esm2m_picontrol_1860soc_maf', 'LONG')
    if 'watergap2_2c_gfdl_esm2m_picontrol_1860soc_q90q50' not in subriver_fields:
        arcpy.AddField_management(subriver_out, 'watergap2_2c_gfdl_esm2m_picontrol_1860soc_q90q50', 'LONG')
    if 'watergap2_2c_gfdl_esm2m_picontrol_1860soc_q90q50_per' not in subriver_fields:
        arcpy.AddField_management(subriver_out, 'watergap2_2c_gfdl_esm2m_picontrol_1860soc_q90q50_per', 'SHORT')

    with arcpy.da.UpdateCursor(subriver_out, ['LINK_RIV',
                                              'watergap2_2c_gfdl_esm2m_picontrol_1860soc_maf',
                                              'watergap2_2c_gfdl_esm2m_picontrol_1860soc_q90q50',
                                              'watergap2_2c_gfdl_esm2m_picontrol_1860soc_q90q50_per']) as cursor:
        for row in cursor:
            if row[0] in join_dict:
                row[1] = join_dict[row[0]][0]
                row[2] = join_dict[row[0]][1]
                if join_dict[row[0]][0] == 0:
                    row[3] = 0
                else:
                    row[3] = int(100*join_dict[row[0]][1]/float(join_dict[row[0]][0]))
            cursor.updateRow(row)
