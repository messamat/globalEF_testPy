import os

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

qtotacc15s_gdb = os.path.join(resdir, 'isimp2_qtot_accumulated15s.gdb') #GDB to contain downsampled rasters
pathcheckcreate(qtotacc15s_gdb)

#-------------------------------------------------- PROCESS ISIMP2B---------------------------------------------------------------------
# Create a searchable dataframe of monthly and annual statistics and global EF computations for runoff
lyrsdf_qtot = pd.DataFrame.from_dict(
    {path: path.stem.split('_') for path in
     isimp2b_resdir.glob('*[.]nc4') if
     re.search(r"((smakhtinef_[abcd])|(allefbutsmakhtin)|mmf|maf)", str(path))},
    orient='index',
    columns=['ghm', 'gcm', 'climate_scenario', 'human_scenario',
             'var',  'eftype', 'emc']
). \
    reset_index(). \
    rename(columns={'index' : 'path'})
lyrsdf_qtot = lyrsdf_qtot[lyrsdf_qtot['var']=='qtot'] #Only keep runoff

lyrsdf_qtot['sum_time'] = np.where(
    lyrsdf_qtot.eftype.isin(['mmf', 'maf', 'smakhtinef']),
    False,
    True)


def flowacc_efnc(in_ncpath, in_template_extentlyr, in_template_resamplelyr,
                 pxarea_grid, flowdir_grid, out_resdir, out_resgdb, scratchgdb,
                 integer_multiplier,lat_dimname = 'lat', lon_dimname = 'lon', time_dimname = 'month',
                 sum_time = True):

    pred_nc = xr.open_dataset(in_ncpath)
    root_name = re.sub('[-]', '_', in_ncpath.stem)

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

            if sum_time:
                (pred_nc_cropped.sum(dim=time_dimname,
                                     skipna=False) * integer_multiplier).astype(np.intc).to_netcdf(out_croppedintnc)
            else:
                (pred_nc_cropped * integer_multiplier).astype(np.intc).to_netcdf(out_croppedintnc)
    else:
        out_croppedintnc = in_ncpath

    for in_var in list(pred_nc.data_vars):
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
            # Multiply input grid by pixel area
            start = time.time()
            valueXarea = Times(Raster(out_rsmpbi), Raster(pxarea_grid))
            outflowacc = FlowAccumulation(in_flow_direction_raster=flowdir_grid,
                                          in_weight_raster=Raster(valueXarea),
                                          data_type="FLOAT")
            outflowacc_m3s = Int(Divide(outflowacc, 10**3)+0.5)
            outflowacc_m3s.save(out_grid)
            end = time.time()
            print(end - start)

lyrsdf_qtot.apply(lambda row:
                  flowacc_efnc(
                      in_ncpath = row['path'],
                      in_template_extentlyr = pxarea_grid,
                      in_template_resamplelyr = pxarea_grid,
                      pxarea_grid=pxarea_grid,
                      flowdir_grid=flowdir_grid,
                      out_resdir = isimp2b_resdir,
                      out_resgdb = qtotacc15s_gdb,
                      scratchgdb = scratchgdb,
                      lat_dimname = 'lat',
                      lon_dimname = 'lon',
                      time_dimname = 'month',
                      sum_time = row['sum_time'],
                      integer_multiplier = 10**9),
                  axis=1
                  )

#h08_hadgem2_es_picontrol_1860soc_qtot_allefbutsmakhtin_croppedint.nc
#in_ncpath = Path('D:/IWMI_GEFIStest/results/isimp2b/h08_hadgem2-es_picontrol_1860soc_dis_allefbutsmakhtin.nc4')
# row  =lyrsdf_qtot.iloc[7,:]
# in_ncpath = row['path']
# in_template_extentlyr = pxarea_grid
# in_template_resamplelyr = pxarea_grid
# pxarea_grid = pxarea_grid
# flowdir_grid = flowdir_grid
# out_resdir = isimp2b_resdir
# out_resgdb = qtotacc15s_gdb
# scratchgdb = scratchgdb
# lat_dimname = 'lat'
# lon_dimname = 'lon'
# time_dimname = 'month'
# sum_time = row['sum_time']
# integer_multiplier = 10 ** 9