from globalEF_comparison_setup import *

#Input variables
gefis_mpk = os.path.join(datdir, 'Global Environmental Flow Calculation_FAO_all.mpk') #Map package provided by CS with GEFIS data
HSras_template = os.path.join(datdir, 'flow_dir_15s_global.gdb', 'flow_dir_15s')

#Outputs
gefis_dir = os.path.join(datdir, 'GEFIS') #Folder to contain extracted map package
pathcheckcreate(gefis_dir)
gefis15s_gdb = os.path.join(resdir, 'GEFIS_15s.gdb') #GDB to contain downsampled rasters
pathcheckcreate(gefis15s_gdb)

#--------------------------------------- Analysis ---------------------------------------------------------------------
#Extract GEFIS package content
if (len(getfilelist(gefis_dir, repattern='.*[.]tif$'))) == 0:
    arcpy.ExtractPackage_management(in_package=gefis_mpk, output_folder=gefis_dir)
gefis_raslist = getfilelist(gefis_dir, repattern='.*[.]tif$')

#Check raster characteristics
rasex = gefis_raslist[0]
rdesc = arcpy.Describe(rasex)
print('GEFIS projection: {0}'.format(rdesc.SpatialReference.name))
print('GEFIS extent: {0}'.format(rdesc.Extent))
print('HydroSHEDS template extent: {0}'.format(arcpy.Describe(HSras_template).Extent))

#Downsampled
in_gefisdict = {rasname(ras): ras for ras in gefis_raslist}
out_gefisdict = {rasname(ras): os.path.join(gefis15s_gdb, rasname(ras)) for ras in gefis_raslist}
hydroresample(in_vardict = in_gefisdict, out_vardict = out_gefisdict,
              in_hydrotemplate = HSras_template, resampling_type='NEAREST')

#Get ratio in cell size between MODIS and EarthEnv DEm 90
cellsize_ratio = arcpy.Describe(rasex).meanCellWidth / arcpy.Describe(HSras_template).meanCellWidth
print('Aggregating DEM by cell size ratio of {0} would lead to a difference in resolution of {1} mm'.format(
    cellsize_ratio,
    11100000*(arcpy.Describe(rasex).meanCellWidth-round(cellsize_ratio, 1)*arcpy.Describe(HSras_template).meanCellWidth
              )
))

#Create boolean rasters to assess the percentage coverage within basins by GEFIS layers
GEFISmar = os.path.join(gefis15s_gdb, 'MAR_Natural_Annual_Runoff_v2')
GEFISemc = os.path.join(gefis15s_gdb, 'EMC_10Variable_2')
Con(IsNull(Raster(GEFISmar)), 0, 1).save(os.path.join(gefis15s_gdb, 'MAR_boolean'))
Con(IsNull(Raster(GEFISemc)), 0, 1).save(os.path.join(gefis15s_gdb, 'EMC_boolean'))