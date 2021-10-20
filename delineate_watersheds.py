from GEFIS_setup import *

#Inputs
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_cleanjoin = os.path.join(process_gdb, 'EFpoints_cleanjoin')
flowdir = os.path.join(datdir, 'flow_dir_15s_global.gdb', 'flow_dir_15s')

#Outputs
wsgdb = os.path.join(resdir, 'EFsites_watersheds.gdb')
pathcheckcreate(wsgdb)

#dict = {}
with arcpy.da.SearchCursor(EFpoints_cleanjoin, ['no', 'SHAPE@']) as cursor:
    for row in cursor:
        print(row[0])
        outpoly = os.path.join(wsgdb, 'ws_{0}'.format(row[0]))
        if not arcpy.Exists(outpoly):
            ws = Con(
                Watershed(in_flow_direction_raster= flowdir,
                          in_pour_point_data= row[1]) == 1,
                row[0]
            )
            ws.save(outpoly)