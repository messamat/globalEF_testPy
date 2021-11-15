from GEFIS_setup import *

px_grid = os.path.join(datdir, 'pixel_area_skm_15s.gdb', 'px_area_skm_15s') #HydroSHEDS pixel area grid
gefis15s_gdb = os.path.join(resdir, 'GEFIS_15s.gdb')
MAR_boolean = os.path.join(gefis15s_gdb, 'MAR_boolean')
EMC_boolean = os.path.join(gefis15s_gdb, 'EMC_boolean')

hydromask = os.path.join(gefis15s_gdb, 'HydroSHEDS_landmask')

#Create mask
if not arcpy.Exists(hydromask):
    wmask = Con(~IsNull(Raster(px_grid)), 1)
    wmask.save(hydromask)

    ZonalStatisticsAsTable(wmask, zone_field='Value', in_value_raster=px_grid,
                           out_table=os.path.join(resdir, 'hydrosheds_totalarea'), statistics_type='SUM')

    ZonalStatisticsAsTable(wmask, zone_field='Value', in_value_raster=(Raster(px_grid)*Raster(MAR_boolean)),
                           out_table=os.path.join(resdir, 'MARmask_totalarea'), statistics_type='SUM')

    ZonalStatisticsAsTable(wmask, zone_field='Value', in_value_raster=(Raster(px_grid)*Raster(EMC_boolean)),
                           out_table=os.path.join(resdir, 'EMCmask_totalarea'), statistics_type='SUM')

#get greenland area
greenlandbas = os.path.join(datdir, 'hybas_gr_lev00_v1c', 'hybas_gr_lev00_v1c.shp')
grarea = sum([row[0] for row in arcpy.da.SearchCursor(greenlandbas, ['SUB_AREA'])])

#Percentage of land area excluded
print('Percentage global land area excluded from MAR mask: {}%'.format(
    100*(
            1-(arcpy.da.TableToNumPyArray(os.path.join(resdir, 'MARmask_totalarea'), 'SUM')[0][0]/ \
               (arcpy.da.TableToNumPyArray(os.path.join(resdir, 'hydrosheds_totalarea'), 'SUM')[0][0]-grarea)
               )
    )))
print('Percentage global land area excluded from EMC mask: {}%'.format(
    100*(
1-(arcpy.da.TableToNumPyArray(os.path.join(resdir, 'EMCmask_totalarea'), 'SUM')[0][0]/ \
   (arcpy.da.TableToNumPyArray(os.path.join(resdir, 'hydrosheds_totalarea'), 'SUM')[0][0]-grarea)
   )
)))