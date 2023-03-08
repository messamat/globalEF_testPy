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
runoff_path = Path(globwb_datdir, 'totalRunoff_monthTot_output_1958-01-31_to_2015-12-31.zip.nc')

def preprocess_GLOBWB(in_path, in_var):
    outpath_ef_notsmakhtin = Path(globwb_resdir, f'{in_path.name.split(".")[0]}_allefbutsmakhtin.nc4')

    dis_xr = spatiotemporal_chunk_optimized_acrosstime(
        xr.open_dataset(in_path),
        lat_dimname='latitude',
        lon_dimname='longitude',
        time_dimname='time'
    )

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

    for shift, emc in enumerate(['a', 'b', 'c', 'd']):
        outpath_ef_smakhtin = f'{in_path.name.split(".")[0]}_smakhtinef_{emc}.nc4'
        if not Path(globwb_resdir, outpath_ef_smakhtin).exists():
            print(f"Processing {outpath_ef_smakhtin}")
            compute_smakhtinef_stats(in_xr = dis_xr,
                                     out_dir = globwb_resdir,
                                     vname=in_var,
                                     out_efnc_basename=outpath_ef_smakhtin,
                                     n_shift=shift)
        else:
            print(f"{outpath_ef_smakhtin} already exists. Skipping...")

preprocess_GLOBWB(in_path=discharge_path, in_var='discharge')
preprocess_GLOBWB(runoff_path, in_var='total_runoff')
