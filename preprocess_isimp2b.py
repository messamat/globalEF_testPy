from globalEF_comparison_setup import * #See this module for directory structure
from scipy import interpolate
#import time
import xarray as xr
#import cProfile as profile
#import pstats
import EF_utils

#Set up structure
isimp2b_datdir = Path(datdir, 'isimp2b')

#hydrological output directory
isimp2b_resdir = Path(resdir, 'isimp2b')
if not isimp2b_resdir.exists():
    isimp2b_resdir.mkdir()

# Create a searchable dataframe of hydrological layers
lyrsdf = pd.DataFrame.from_dict(
    {path: path.stem.split('_') for path in isimp2b_datdir.glob('*[.]nc4')},
    orient='index',
    columns=['ghm', 'gcm', 1, 'climate_scenario', 'human_scenario',
             2, 'var',  3, 'time_step', 'start_yr', 'end_yr']
). \
    drop(columns=[1, 2, 3]). \
    reset_index().\
    rename(columns={'index' : 'path'})

#Create output path for monthly resampling
lyrsdf['monthly_path'] = [Path(isimp2b_resdir,
                               re.sub("daily", "monthly", str(i.name))
                               ) for i in lyrsdf['path']]
lyrsdf['run'] = lyrsdf[['ghm', 'gcm', 'climate_scenario', 'human_scenario', 'var']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)

lyrsdf.apply(EF_utils.aggregate_monthly_fromdf, axis=1)

#### -------------- Running analysis ----------------------------------------------------------------------------------
for run in lyrsdf['run'].unique():
    print(run)
    lyrsdf_sub = lyrsdf.loc[lyrsdf['run']==run]

    run_xr = EF_utils.spatiotemporal_chunk_optimized_acrosstime(
        xr.open_mfdataset(lyrsdf_sub.monthly_path, concat_dim="time", combine='nested', parallel=True,
                          use_cftime=True)
    )

    # Cannot use run_xr.time.dt.month because open_mfdataset automatically reads data in two different calendars
    # cftime.DatetimeGregorian an cftime.DeatetimeProlepticGregorian
    xr_calendars = np.unique([i.calendar for i in run_xr["time"].values])
    if len(xr_calendars) > 1:
        run_xr = run_xr.assign_coords(month=(
            "time",
            xr.concat(
                [run_xr.where(run_xr.time <= max([i for i in run_xr["time"].values if i.calendar == list(xr_calendars)[0]]),
                              drop=True)['time'].dt.strftime("%m"),
                 run_xr.where(run_xr.time > max([i for i in run_xr["time"].values if i.calendar == list(xr_calendars)[0]]),
                              drop=True)['time'].dt.strftime("%m")],
                dim='time'
            ).data
        ))
    else :
        run_xr = run_xr.assign_coords(month=run_xr.time.dt.strftime("%m"))

    outpath_ef_notsmakhtin = Path(isimp2b_resdir, f'{run}_allefbutsmakhtin.nc4')
    if not outpath_ef_notsmakhtin.exists():
        print(f"Processing {outpath_ef_notsmakhtin.name}")
        EF_utils.compute_monthlyef_notsmakhtin(in_xr = run_xr, out_efnc=outpath_ef_notsmakhtin , remove_outliers=True)
    else:
        print(f"{outpath_ef_notsmakhtin.name} already exists. Skipping...")


    for shift, emc in enumerate(['a', 'b', 'c', 'd']):
        outpath_ef_smakhtin = f'{run}_smakhtinef_{emc}.nc4'
        if not Path(isimp2b_resdir, outpath_ef_smakhtin).exists():
            print(f"Processing {outpath_ef_smakhtin}")
            EF_utils.compute_smakhtinef_stats(in_xr = run_xr, out_dir = isimp2b_resdir,
                                     out_efnc_basename=outpath_ef_smakhtin, n_shift=shift)
        else:
            print(f"{outpath_ef_smakhtin} already exists. Skipping...")



#### -------------- Extra stuff ----------------------------------------------------------------------------------
# bin = xr.where((cell.dis < max_bin),
#                np.searchsorted(a=fdc,
#                                v=cell.dis.values,
#                                side='left'),
#                0) #To correct, should extrapolate otherwise
#
# np.exp(np.interp(np.log(cell.dis.values), xp=xp, fp=fp))
# fdc_xr.isel(exceedance_prob=16).dis.plot.pcolormesh(cmap='viridis', norm=colors.LogNorm(vmin=0.001, vmax=120000))
#
# run_maf.dis.plot.pcolormesh(cmap='viridis', norm=colors.LogNorm(vmin=0.001, vmax=120000))
# run_q90.dis.plot.pcolormesh(cmap='viridis',norm=colors.LogNorm(vmin=0.001, vmax=120000))
#
#
# hydromo_path = Path(isimp2b_resdir, f"{run}_monthly_{}.nc4")
# hydroxr = xr.open_mfdataset(lyrslist, concat_dim ="time", combine='nested',
#                             chunks={"time": 73048, "lat": 5, "lon": 5}, parallel=True, use_cftime=True).\
# hydroxr.to_netcdf(hydromo_path)
# check = xr.open_dataset(hydromo_path)
# start = time.time()
#
#
# #Example metadata creation
# data.attrs["long_name"] = "random velocity"
# data.attrs["units"] = "metres/sec"
# data.attrs["description"] = "A random variable created as an example."
# data.x.attrs["units"] = "x units" #Can add metadata to coordinates too
#
#



