from globalEF_comparison_setup import *

#Inputs
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
flowdir = os.path.join(datdir, 'flow_dir_15s_global.gdb', 'flow_dir_15s')
EFpoints_QAQCed_p = os.path.join(process_gdb, 'Master_20211104_QAQCed')

#Outputs
wsgdb = os.path.join(resdir, 'EFsites_watersheds.gdb')
pathcheckcreate(wsgdb)

#This was the only way that it worked (using SHAPE@ would lead to Error 999999 for no reason, or selecting directly or using layers, or...)
idlist = [row[0] for row in arcpy.da.SearchCursor(EFpoints_QAQCed_p, ['EFUID'])]
for id in idlist:
    outp = os.path.join(wsgdb, 'point_{}'.format(id))
    print(id)
    if id != 177:
        if not arcpy.Exists(outp):
            arcpy.MakeFeatureLayer_management(EFpoints_QAQCed_p, 'ptsub', where_clause='EFUID = {}'.format(id))
            arcpy.CopyFeatures_management('ptsub', outp)

            outpoly = os.path.join(wsgdb, 'ws_{0}'.format(id))
            #if not arcpy.Exists(outpoly):
            ws = Con(
                Watershed(in_flow_direction_raster=flowdir,
                          in_pour_point_data=outp) == 1,
                id
            )
            ws.save(outpoly)