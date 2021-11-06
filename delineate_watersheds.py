from GEFIS_setup import *

#Inputs
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_QAQCed = os.path.join(resdir, 'Master_20211104_QAQCed.csv')
flowdir = os.path.join(datdir, 'flow_dir_15s_global.gdb', 'flow_dir_15s')

#Outputs
EFpoints_QAQCed_p = os.path.join(process_gdb, 'Master_20211104_QAQCed')
wsgdb = os.path.join(resdir, 'EFsites_watersheds.gdb')
pathcheckcreate(wsgdb)

#Create points for those with valid coordinates (see merge_dbversions.r)
if not arcpy.Exists(EFpoints_QAQCed_p):
    arcpy.MakeXYEventLayer_management(table=EFpoints_QAQCed,
                                      in_x_field='POINT_X',
                                      in_y_field='POINT_Y',
                                      out_layer='efp_QAQCed',
                                      spatial_reference=4326,
                                      in_z_field=None)
    arcpy.CopyFeatures_management('efp_QAQCed', EFpoints_QAQCed_p)

#This was the only way that it worked (using SHAPE@ would lead to Error 999999 for no reason, or selecting directly or using layers, or...)
idlist = [row[0] for row in arcpy.da.SearchCursor(EFpoints_QAQCed_p, ['EFUID'])]
for id in idlist:
    outp = os.path.join(wsgdb, 'point_{}'.format(id))
    print(id)

    if not arcpy.Exists(outp):
        arcpy.MakeFeatureLayer_management(EFpoints_QAQCed_p, 'ptsub', where_clause='EFUID = {}'.format(id))
        arcpy.CopyFeatures_management('ptsub', outp)

    outpoly = os.path.join(wsgdb, 'ws_{0}'.format(id))
    if not arcpy.Exists(outpoly):
        ws = Con(
            Watershed(in_flow_direction_raster=flowdir,
                      in_pour_point_data=outp) == 1,
            row[0]
        )
        ws.save(outpoly)