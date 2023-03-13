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
lyrsdf_daily = pd.DataFrame.from_dict(
    {path: path.stem.split('_') for path in isimp2b_datdir.glob('*[.]nc4')},
    orient='index',
    columns=['ghm', 'gcm', 1, 'climate_scenario', 'human_scenario',
             2, 'var',  3, 'time_step', 'start_yr', 'end_yr']
). \
    drop(columns=[1, 2, 3]). \
    reset_index(). \
    rename(columns={'index' : 'path'})

#Create output path for monthly resampling
lyrsdf_daily['monthly_path'] = [Path(isimp2b_resdir,
                                     re.sub("daily", "monthly", str(i.name))
                                     ) for i in lyrsdf_daily['path']]
lyrsdf_daily['run'] = lyrsdf_daily[['ghm', 'gcm', 'climate_scenario', 'human_scenario', 'var']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)

#Aggregate GHM outputs from daily to monthly time series
if len(lyrsdf_daily[lyrsdf_daily['var']=='dis']) > 0:
    lyrsdf_daily[lyrsdf_daily['var']=='dis'].apply(
        EF_utils.aggregate_monthly_fromdf, axis=1, vname='dis')

if len(lyrsdf_daily[lyrsdf_daily['var']=='qtot']) > 0:
    lyrsdf_daily[lyrsdf_daily['var']=='qtot'].apply(
        EF_utils.aggregate_monthly_fromdf, axis=1, vname='qtot')

# Create a searchable dataframe of MONTHLY hydrological layers
# (this is put in place so that the following part of the analysis can take place without the need to keep
# the daily files on disk)
lyrsdf_monthly = pd.DataFrame.from_dict(
    {path: path.stem.split('_') for path in
     isimp2b_resdir.glob('*[.]nc4') if
     re.search(r"monthly", str(path))},
    orient='index',
    columns=['ghm', 'gcm', 1, 'climate_scenario', 'human_scenario',
             2, 'var',  3, 'time_step', 'start_yr', 'end_yr']
). \
    reset_index(). \
    rename(columns={'index' : 'path'})
lyrsdf_monthly['run'] = lyrsdf_monthly[['ghm', 'gcm', 'climate_scenario', 'human_scenario', 'var']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)

#### -------------- Running analysis ----------------------------------------------------------------------------------
for run in lyrsdf_monthly['run'].unique():
    print(run)
    lyrsdf_monthly_sub = lyrsdf_monthly.loc[lyrsdf_monthly['run']==run]

    #~~~~~~~~~~~~~~  READ GHM output ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #For each run, open all monthly netcdfs as one xr dataset
    run_xr = xr.open_mfdataset(lyrsdf_monthly_sub.path, concat_dim="time", combine='nested', parallel=True,
                          use_cftime=True)

    #~~~~~~~~~~~~~~  FORMAT dates ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Cannot use run_xr.time.dt.month because open_mfdataset automatically reads data in two different calendars
    # cftime.DatetimeGregorian an cftime.DeatetimeProlepticGregorian
    xr_calendars = np.unique([i.calendar for i in run_xr["time"].values])
    if len(xr_calendars) > 1:
        run_xr = run_xr.assign_coords(month=(
            "time",
            xr.concat(
                [run_xr.where(run_xr.time <= max([i for i in run_xr["time"].values
                                                  if i.calendar == list(xr_calendars)[0]]),
                              drop=True)['time'].dt.strftime("%m"),
                 run_xr.where(run_xr.time > max([i for i in run_xr["time"].values
                                                 if i.calendar == list(xr_calendars)[0]]),
                              drop=True)['time'].dt.strftime("%m")],
                dim='time'
            ).data
        ))
    else :
        run_xr = run_xr.assign_coords(month=run_xr.time.dt.strftime("%m"))

    #~~~~~~~~~~~~~~  COMPUTE EF based on all methods but Smakhtin's ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    outpath_ef_notsmakhtin = Path(isimp2b_resdir, f'{run}_allefbutsmakhtin.nc4')
    #if not outpath_ef_notsmakhtin.exists():
    print(f"Processing {outpath_ef_notsmakhtin.name}")
    EF_utils.compute_monthlyef_notsmakhtin(in_xr = run_xr,
                                           out_efnc=outpath_ef_notsmakhtin,
                                           remove_outliers=True,
                                           vname = lyrsdf_monthly[lyrsdf_monthly['run']==run]['var'].unique()[0],
                                           out_mmf=Path(isimp2b_resdir, f'{run}_mmf.nc4'),
                                           out_maf=Path(isimp2b_resdir, f'{run}_maf.nc4'))
    # else:
    #     print(f"{outpath_ef_notsmakhtin.name} already exists. Skipping...")

    #~~~~~~~~~~~~~~  COMPUTE EF based on Smakhtin's method for EMCs A-D  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    for shift, emc in enumerate(['a', 'b', 'c', 'd']):
        outpath_ef_smakhtin = f'{run}_smakhtinef_{emc}.nc4'
        if not Path(isimp2b_resdir, outpath_ef_smakhtin).exists():
            print(f"Processing {outpath_ef_smakhtin}")
            EF_utils.compute_smakhtinef_stats(in_xr = run_xr,
                                              out_dir = isimp2b_resdir,
                                              out_efnc_basename=outpath_ef_smakhtin,
                                              n_shift=shift+1,
                                              vname = lyrsdf_monthly[lyrsdf_monthly['run']==run]['var'].unique()[0])
        else:
            print(f"{outpath_ef_smakhtin} already exists. Skipping...")