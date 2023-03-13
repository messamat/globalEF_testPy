import re

from globalEF_comparison_setup import * #See this module for directory structure
#import time
import xarray as xr
#import cProfile as profile
#import pstats
from EF_utils import *

#Set up structure
globwb_datdir = Path(datdir, 'PCR_GLOBWB_2019')

#hydrological output directory
globwb_resdir = Path(resdir, 'PCR_GLOBWB_2019')
if not globwb_resdir.exists():
    globwb_resdir.mkdir()

# Create a searchable dataframe of hydrological layers
discharge_path = Path(globwb_datdir, 'discharge_monthAvg_output_1958-01-31_to_2015-12-31.zip.nc')
runoff_path = Path(globwb_datdir, 'runoff_monthTot_output_1958-01-31_to_2015-12-31.zip.nc')


def split_netcdf(in_netcdf, factor,  out_pathroot, lat_dimname='latitude', lon_dimname='longitude'):
    if isinstance(in_netcdf, (str, Path)):
        in_netcdf = xr.open_dataset(in_netcdf)


    if len(np.unique(in_netcdf[lon_dimname].values[:-1] - in_netcdf[lon_dimname].values[1:])) > 1:
        lat_binlist = list(range(0, in_netcdf.dims[lat_dimname],
                                 int(np.ceil(in_netcdf.dims[lat_dimname]/factor))))
        lat_binlist.append(in_netcdf.dims[lat_dimname])

        lon_binlist = list(range(0, in_netcdf.dims[lon_dimname],
                                 int(np.ceil(in_netcdf.dims[lon_dimname]/factor))))
        lon_binlist.append(in_netcdf.dims[lon_dimname])

        out_tilelist = []
        for lat_min, lat_max in zip(lat_binlist[:-1], lat_binlist[1:]):
            for lon_min, lon_max in zip(lon_binlist[:-1], lon_binlist[1:]):
                print(f'Writing bbox: {lon_min}, {lat_min}, {lon_max}, {lat_max}')
                out_divxr = Path(f'{out_pathroot}_{lon_min}_{lat_min}_{lon_max}_{lat_max}.nc4')

                out_tilelist.append(out_divxr)

                slice_dict = {lon_dimname: slice(lon_min, lon_max),
                              lat_dimname: slice(lat_min, lat_max)}

                if not Path(out_divxr).exists():
                    in_netcdf.isel(slice_dict).to_netcdf(out_divxr)

        return(out_tilelist)

    else: #to continue
        bbox = [min(in_netcdf[lon_dimname].values),
                min(in_netcdf[lat_dimname].values),
                max(in_netcdf[lon_dimname].values),
                max(in_netcdf[lat_dimname].values)
                ]


def preprocess_GLOBWB(in_path, in_var, out_resdir):
    outpath_ef_notsmakhtin = Path(out_resdir, f'{in_path.name.split(".")[0]}_allefbutsmakhtin.nc4')

    # dis_xr = spatiotemporal_chunk_optimized_acrosstime(
    #     xr.open_dataset(in_path),
    #     lat_dimname='latitude',
    #     lon_dimname='longitude',
    #     time_dimname='time'
    # )
    dis_xr = xr.open_dataset(in_path)
    dis_xr = dis_xr.assign_coords(month=("time",
                                         dis_xr.time.dt.strftime("%m").data))

    if not outpath_ef_notsmakhtin.exists():
        print(f"Processing {outpath_ef_notsmakhtin.name}")
        compute_monthlyef_notsmakhtin(in_xr = dis_xr,
                                      out_efnc=outpath_ef_notsmakhtin ,
                                      vname = in_var,
                                      lat_dimname='latitude',
                                      lon_dimname='longitude',
                                      time_dimname='time',
                                      remove_outliers=True)
    else:
        print(f"{outpath_ef_notsmakhtin.name} already exists. Skipping...")

    #Divide the raster in four tiles to be processed separately
    dis_xr_tilelist = split_netcdf(in_netcdf=dis_xr,
                                   factor=10,
                                   out_pathroot=Path(out_resdir, os.path.splitext(os.path.splitext(in_path.name)[0])[0]),
                                   lat_dimname='latitude',
                                   lon_dimname='longitude')

    #For each tile
    for tile_path in dis_xr_tilelist:
        #For each EMC
        for shift, emc in enumerate(['a', 'b', 'c', 'd']):
            outpath_ef_smakhtin = f'{tile_path.name.split(".")[0]}_smakhtinef_{emc}.nc4'
            if not Path(out_resdir, outpath_ef_smakhtin).exists():
                print(f"Processing {outpath_ef_smakhtin}")

                import time
                start = time.time()
                distile_xr = xr.open_dataset(tile_path)

                compute_smakhtinef_stats(in_xr = distile_xr,
                                         out_dir = out_resdir,
                                         vname=in_var,
                                         out_efnc_basename=outpath_ef_smakhtin,
                                         n_shift=shift+1,
                                         lat_dimname='latitude',
                                         lon_dimname='longitude',
                                         scratch_file = 'scratch.nc4')
                end = time.time()
                print(end-start)

            else:
                print(f"{outpath_ef_smakhtin} already exists. Skipping...")


    full_outputpaths_list = [Path(globwb_resdir, f'{tile_path.name.split(".")[0]}_smakhtinef_{emc}.nc4'
                                  ).exists()
                             for tile_path in dis_xr_tilelist
                             for emc in ['a', 'b', 'c', 'd']]
    if all(full_outputpaths_list):
        return(full_outputpaths_list)

    #Re-merge the tiles

smakthinef_dis_pathlist = preprocess_GLOBWB(in_path=discharge_path,
                                            in_var='discharge',
                                            out_resdir = globwb_resdir)
smakthinef_qtot_pathlist = preprocess_GLOBWB(in_path=runoff_path,
                                             in_var='total_runoff',
                                             out_resdir = globwb_resdir)


#Merge
for var in ['discharge', 'runoff']:
    for emc in ['a', 'b', 'c', 'd']:
        tile_list = [p for p in globwb_resdir.iterdir() if
                     re.match(re.compile(f"{var}_.*_smakhtinef_{emc}.nc4"), str(p.name))]

        out_mergepath = Path(globwb_resdir,
                             f"PCR_GLOBWB_{var}_smakthinef_{emc}.nc4")
        #if not out_mergepath.exists():
        print(f'EF smakthin {var}, {emc}: '
              f'merging {len(list(tile_list))} tiles...')

        mfxr = xr.combine_by_coords([xr.open_dataset(tile) for tile in tile_list])
        mfxr.to_netcdf(out_mergepath)