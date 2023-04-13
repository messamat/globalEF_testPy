from globalEF_comparison_setup import *
import EF_utils
#from utility_functions_py3 import *
import xarray as xr
import numpy as np
import time

arcpy.CheckOutExtension('Spatial')
arcpy.env.overwriteOutput = True

#Input variables
isimp2b_resdir = Path(resdir, 'isimp2b')
globwb_resdir = Path(resdir, 'PCR_GLOBWB_2019')

#Geometry rasters
geomdir = os.path.join(datdir, 'HydroATLAS', 'HydroATLAS_Geometry')
pxarea_grid = os.path.join(geomdir, 'Accu_area_grids', 'pixel_area_skm_15s.gdb', 'px_area_skm_15s')
flowdir_grid = os.path.join(geomdir, 'Flow_directions' ,'flow_dir_15s_global.gdb', 'flow_dir_15s')


#Outputs
scratchgdb = os.path.join(resdir, 'scratch.gdb')
pathcheckcreate(scratchgdb)

isimp_qtotacc15s_gdb = os.path.join(resdir, 'isimp2_qtot_accumulated15s.gdb') #GDB to contain downsampled rasters
pathcheckcreate(isimp_qtotacc15s_gdb)

globwb_qtotacc15s_gdb = os.path.join(resdir, 'globwb_qtot_accumulated15s.gdb')
pathcheckcreate(globwb_qtotacc15s_gdb)

def flowacc_efnc(in_ncpath, in_template_extentlyr, in_template_resamplelyr,
                 pxarea_grid, flowdir_grid, out_resdir, out_resgdb, scratchgdb,
                 lat_dimname = 'lat', lon_dimname = 'lon', time_dimname = 'month',
                 aggregate_time = True, integer_multiplier = None, convert_to_int = True):

    pred_nc = xr.open_dataset(in_ncpath)
    root_name = re.sub('[-]', '_', in_ncpath.stem)

    #First check whether all output flow accumulation exist. If not, proceed
    if not all(arcpy.Exists(p) for p in
               [os.path.join(out_resgdb, f"{root_name}_{in_var}_acc15s") for in_var in list(pred_nc.data_vars)
                if not in_var == 'raef']):
        # Crop xarray, sum across all months, and convert it to integer
        if not all([np.issubdtype(pred_nc[v].values.dtype, np.integer) #If all variables in nc4 are not in integer format
                    for v in list(pred_nc.data_vars)]):
            out_croppedintnc = os.path.join(out_resdir, f"{root_name}_croppedint.nc")

            if not arcpy.Exists(out_croppedintnc):
                print(f"Producing {out_croppedintnc}")
                templateext = arcpy.Describe(in_template_extentlyr).extent

                # Reverse slice order if lat is from north to south or lon is from east to west
                cropdict = {
                    lon_dimname:
                        slice(templateext.XMin, templateext.XMax) if
                        (pred_nc[lon_dimname][0] < pred_nc[lon_dimname][-1]) else
                        slice(templateext.XMax, templateext.XMin),
                    lat_dimname:
                        slice(templateext.YMin, templateext.YMax) if
                        (pred_nc[lat_dimname][0] < pred_nc[lat_dimname][-1]) else
                        slice(templateext.YMax, templateext.YMin)
                }
                pred_nc_cropped = pred_nc.loc[cropdict]

                if aggregate_time:
                    pred_nc_cropped = pred_nc_cropped.mean(dim=time_dimname, skipna=False)
                if convert_to_int:
                    (pred_nc_cropped * integer_multiplier).astype(np.intc).to_netcdf(out_croppedintnc)
                else:
                    pred_nc_cropped.to_netcdf(out_croppedintnc)

        else:
            out_croppedintnc = in_ncpath

        for in_var in list(pred_nc.data_vars):
            if in_var != 'raef':
                out_croppedint = os.path.join(scratchgdb, f"{root_name}_{in_var}_croppedint")
                if not arcpy.Exists(out_croppedint):
                    print(f"Saving {out_croppedintnc} to {out_croppedint}")

                    #time_dimname = time_dimname if time_dimname in list(pred_nc.dims) else None

                    arcpy.md.MakeNetCDFRasterLayer(in_netCDF_file=out_croppedintnc,
                                                   variable=in_var,
                                                   x_dimension=lon_dimname,
                                                   y_dimension=lat_dimname,
                                                   out_raster_layer='tmpras_check',
                                                   #band_dimension=time_dimname,
                                                   value_selection_method='BY_VALUE',
                                                   cell_registration='CENTER')
                    output_ras = Raster('tmpras_check')
                    Con(output_ras >= 0, output_ras).save(out_croppedint)

                # Set environment
                arcpy.env.extent = arcpy.env.snapRaster = in_template_resamplelyr

                # Run weighting
                out_rsmpbi = os.path.join(scratchgdb, f"{root_name}_{in_var}_rsmpbi")
                out_grid = os.path.join(out_resgdb, f"{root_name}_{in_var}_acc15s")

                # Resample
                if not arcpy.Exists(out_rsmpbi):
                    print(f"Resampling {out_croppedint}")
                    arcpy.management.Resample(in_raster=out_croppedint,
                                              out_raster=out_rsmpbi,
                                              cell_size=arcpy.Describe(in_template_resamplelyr).MeanCellWidth,
                                              resampling_type='BILINEAR')

                if not arcpy.Exists(out_grid):
                    print(f"Running flow accumulation for {root_name}, {in_var}")
                    # Multiply input grid by pixel area (save intermediate because keeps crashing
                    start = time.time()
                    Times(Raster(out_rsmpbi), Raster(pxarea_grid)).save(os.path.join(scratchgdb, 'valueXarea'))
                    outflowacc = FlowAccumulation(in_flow_direction_raster=flowdir_grid,
                                                  in_weight_raster=Raster(os.path.join(scratchgdb, 'valueXarea')),
                                                  data_type="FLOAT")
                    if convert_to_int:
                        outflowacc_m3s = Int(Divide(outflowacc, 10**3)+0.5)
                        outflowacc_m3s.save(out_grid)
                    else:
                        outflowacc.save(out_grid)
                    end = time.time()
                    print(end - start)
    else:
        print(f"All output accumulation layers exist for {root_name}. skipping...")

#-------------------------------------------------- PROCESS PCR_GLOBWB -------------------------------------------------
#These crash with Process exit on some computers for no apparent reason. If so, can run workflow line by line, which oddly works
arcpy.env.parallelProcessingFactor = "90%"

flowacc_efnc(
    in_ncpath=Path(globwb_resdir, "PCR_GLOBWB_runoff_allefbutsmakhtin.nc4"),
    in_template_extentlyr=pxarea_grid,
    in_template_resamplelyr=pxarea_grid,
    pxarea_grid=pxarea_grid,
    flowdir_grid=flowdir_grid,
    out_resdir=globwb_resdir,
    out_resgdb=globwb_qtotacc15s_gdb,
    scratchgdb=scratchgdb,
    lat_dimname='latitude',
    lon_dimname='longitude',
    time_dimname='month',
    aggregate_time=True,
    convert_to_int=False)


#######TOTAL OVER THE YEAR
lyrslist_qtot_globwb_nosum = [Path(globwb_resdir, bname) for bname in
                              ["PCR_GLOBWB_runoff_maf.nc4"] + \
                              [f"PCR_GLOBWB_runoff_smakthinef_{letter}.nc4" for letter in ['a', 'b', 'c', 'd']]
                              ]

for path in lyrslist_qtot_globwb_nosum:
    if path.exists():
        flowacc_efnc(
            in_ncpath=path,
            in_template_extentlyr=pxarea_grid,
            in_template_resamplelyr=pxarea_grid,
            pxarea_grid=pxarea_grid,
            flowdir_grid=flowdir_grid,
            out_resdir=globwb_resdir,
            out_resgdb=globwb_qtotacc15s_gdb,
            scratchgdb=scratchgdb,
            lat_dimname='latitude',
            lon_dimname='longitude',
            time_dimname='month',
            aggregate_time=False,
            convert_to_int=False)
    else:
        Warning(f'{path} file not found')

#-------------------------------------------------- PROCESS ISIMP2B-----------------------------------------------------
# Create a searchable dataframe of monthly and annual statistics and global EF computations for isimp2b runoff
lyrsdf_qtot_isimp2b = pd.DataFrame.from_dict(
    {path: path.stem.split('_') for path in
     isimp2b_resdir.glob('*[.]nc4') if
     re.search(r"((smakhtinef_[abcd])|(allefbutsmakhtin)|maf)", str(path))},
    orient='index',
    columns=['ghm', 'gcm', 'climate_scenario', 'human_scenario',
             'var',  'eftype', 'emc']
). \
    reset_index(). \
    rename(columns={'index' : 'path'})
lyrsdf_qtot_isimp2b = lyrsdf_qtot_isimp2b[lyrsdf_qtot_isimp2b['var']=='qtot'] #Only keep runoff

lyrsdf_qtot_isimp2b['sum_time'] = np.where(
    lyrsdf_qtot_isimp2b.eftype.isin(['maf', 'smakhtinef']),
    False,
    True)

lyrsdf_qtot_isimp2b.apply(lambda row:
                          flowacc_efnc(
                              in_ncpath = row['path'],
                              in_template_extentlyr = pxarea_grid,
                              in_template_resamplelyr = pxarea_grid,
                              pxarea_grid=pxarea_grid,
                              flowdir_grid=flowdir_grid,
                              out_resdir = isimp2b_resdir,
                              out_resgdb = isimp_qtotacc15s_gdb,
                              scratchgdb = scratchgdb,
                              lat_dimname = 'lat',
                              lon_dimname = 'lon',
                              time_dimname = 'month',
                              aggregate_time = row['sum_time'],
                              convert_to_int = True,
                              integer_multiplier = 10**9),
                          axis=1
                          )

