import cftime
from datetime import datetime
import os
import numpy as np
import pandas as pd
from pathlib import Path
import re
import xarray as xr

import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cbook as cbook
from matplotlib import cm
from scipy import interpolate

rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = Path(rootdir, 'data')
resdir = Path(rootdir, 'results')

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
    reset_index(names='path')

#Create output path for monthly resampling
lyrsdf['monthly_path'] = [Path(isimp2b_resdir,
                               re.sub("daily", "monthly", str(i.name))
                               ) for i in lyrsdf['path']]
lyrsdf['run'] = lyrsdf[['ghm', 'gcm', 'climate_scenario', 'human_scenario', 'var']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)

# Resample at monthly scale
def aggregate_fromdf(row):
    if not row['monthly_path'].exists():
        print(f"Processing {row['monthly_path']}")
        xr.open_dataset(row['path']).resample(time='1MS').mean().to_netcdf(row['monthly_path'])
    else:
        print(f"{row['monthly_path']} already exists. Skipping... ")
lyrsdf.apply(aggregate_fromdf, axis=1)


for run in lyrsdf['run'].unique():
    print(run)
    lyrsdf_sub = lyrsdf.loc[lyrsdf['run']==run]

    # Cannot use run_xr.time.dt.month because open_mfdataset automatically reads data in two different calendars
    # cftime.DatetimeGregorian an cftime.DeatetimeProlepticGregorian
    run_xr = xr.open_dataset(lyrsdf_sub.monthly_path.iloc[0], use_cftime=True)
    run_xr = run_xr.assign_coords(month=(
        "time",
        run_xr.time.dt.strftime("%m").data
    ))

    # run_xr = xr.open_mfdataset(lyrsdf_sub.monthly_path, concat_dim="time", combine='nested',
    #                            chunks={"time": 2400, "lat": 30, "lon": 30}, parallel=True, use_cftime=True)
    # run_xr = run_xr.assign_coords(month = (
    #     "time",
    #     xr.concat(
    #         [run_xr.where(run_xr.time <= max([i for i in run_xr["time"].values if i.calendar == 'gregorian']),
    #                       drop=True)['time'].dt.strftime("%m"),
    #          run_xr.where(run_xr.time > max([i for i in run_xr["time"].values if i.calendar == 'gregorian']),
    #                       drop=True)['time'].dt.strftime("%m")],
    #         dim='time'
    #     ).data
    # ))

    # Remove outliers - monthly flow values over mean+3sd or under mean-3sd
    run_nooutliers = run_xr.where(
        run_xr.groupby('month') <= ((run_xr.groupby('month').mean(dim='time') +
                                     3 * run_xr.groupby('month').std(dim='time')))
    ).where(
        run_xr.groupby('month') >= ((run_xr.groupby('month').mean(dim='time') -
                                     3 * run_xr.groupby('month').std(dim='time')))
    )

    #Compute mean monthly flow (mmf)
    run_mmf = run_nooutliers.groupby('month').mean(dim='time')
    # Compute mean annual flow (maf)
    run_maf = run_nooutliers.mean(dim='time')
    #Compute Q90
    run_q90 = run_nooutliers.quantile(q=0.1, dim='time')
    #Compute Q50
    run_q50 = run_nooutliers.quantile(q=0.5, dim='time')


    #Compute Tennant, Q90_Q50, Tessmann and VMF e-flows
    global_monthly_eflows = xr.where(run_mmf<=run_maf,
                                     0.2*run_maf,
                                     0.4*run_maf).rename({"dis":"tennant"}). \
        merge(xr.where(run_mmf<=run_maf,
                       run_q90,
                       run_q50).rename({"dis":"q90q50"})). \
        merge(xr.where(run_mmf <= 0.4*run_maf,
                       run_mmf,
                       xr.where(0.4*run_mmf > 0.4*run_maf,
                                0.4*run_mmf,
                                0.4*run_maf)
                       ).rename({"dis": "tessmann"})). \
        merge(xr.where(run_mmf <= 0.4 * run_maf,
                       0.6 * run_mmf,
                       xr.where(run_mmf > 0.8 * run_maf,
                                0.3 * run_mmf,
                                0.45 * run_maf)
                       ).rename({"dis": "vmf"}))

    #Compute modern smakthin method
    #Generate flow duration curve for each cell
    quant_list = [0.0001, 0.001, 0.01, 0.05, 0.1, 0.2,
                  0.3, 0.4, 0.5, 0.6, 0.7,
                  0.8, 0.9, 0.95, 0.99, 0.999, 0.9999]
    exceedance_list = [1-i for i in quant_list]
    fdc_xr = run_xr.quantile(q=quant_list, dim='time').\
        rename({"quantile" : "exceedance_prob"})
    fdc_xr["exceedance_prob"] = 1 - fdc_xr["exceedance_prob"]


    cell = run_xr.isel(lat=51, lon=51)
    fdc_xr = fdc_xr
    n_shift = 1

    def compute_smakhtinef(cell, fdc_xr, n_shift):
        fdc = fdc_xr.sel(lat=cell.lat.values, lon=cell.lon.values).dis
        min_bin = min(fdc.values)
        max_bin = max(fdc.values)

        #If value is equal or less than min_bin, then replace with min_bin (also deals with 0s)
        xp = np.log(fdc.values)
        fp = np.roll(xp, n_shift)
        f_shift = interpolate.interp1d(x=xp[(n_shift):], y=fp[(n_shift):],
                                       kind='linear', fill_value='extrapolate')
        ts_eflow_unfloored = np.exp(f_shift(np.log(cell.dis.values)))
        return(xr.where(ts_eflow_unfloored <= min_bin, min_bin, ts_eflow_unfloored))

    #Apply to each cell






















        #Interpolate between the nearest two log values and get the anti-log value of the interpolated result
        #If you need to extrapolate, take the last two values and extrapolate using those two values


        bin = xr.where((cell.dis < max_bin),
                       np.searchsorted(a=fdc,
                              v=cell.dis.values,
                              side='left'),
                       0) #To correct, should extrapolate otherwise
        fdc.isel(exceedance_prob=bin.values)
        fdc.isel(exceedance_prob=bin.values-n_shift)


    np.exp(np.interp(np.log(cell.dis.values), xp=xp, fp=fp))
    fdc_xr.isel(exceedance_prob=16).dis.plot.pcolormesh(cmap='viridis', norm=colors.LogNorm(vmin=0.001, vmax=120000))




    run_maf.dis.plot.pcolormesh(cmap='viridis', norm=colors.LogNorm(vmin=0.001, vmax=120000))
    run_q90.dis.plot.pcolormesh(cmap='viridis',norm=colors.LogNorm(vmin=0.001, vmax=120000))

    lowflow.isel(time=0).dis

    tennant_eflow.isel(month=0).dis.plot.pcolormesh(cmap='viridis')


    #Compute Q90


    # #Compute Q50

    run_maf.dis.plot.pcolormesh(cmap='viridis')
    fig = plt.figure()
    plt.pcolormesh(run_maf.dis, #norm=colors.LogNorm(),
                                      cmap='viridis')
    plt.colorbar()


    check.isel(time=15).dis.plot()

    check.isel(month=0).dis.plot








    check.isel(month=0, lat=0, lon=0).dis
    check.dis
    (run_xr.groupby('month') -

    check.isel(time=0).dis.plot()


    run_xrstats = xr.merge([run_xr,
                            .
                           rename({"dis":"mmfplus3sd"})
                            ])
    run_xr['mmfplus3sd'] = run_xr.groupby('month').mean(dim='time') + 3*run_xr.groupby('month').std(dim='time')
    run_mmfplus3sd['month'] = [str(i).zfill(2) for i in range(1,13)]
    run_mmfminus3sd = run_xr.groupby('month').mean() - 3*run_xr.groupby('month').std()
    run_mmfminus3sd['month'] = [str(i).zfill(2) for i in range(1, 13)]



    check = run_xr.where(run_xr.dis < run_mmfplus3sd & run_xr.dis > )

    check.isel(time=0).dis.plot.pcolormesh(cmap='viridis')












hydromo_path = Path(isimp2b_resdir, f"{run}_monthly_{}.nc4")
hydroxr = xr.open_mfdataset(lyrslist, concat_dim ="time", combine='nested',
                            chunks={"time": 73048, "lat": 5, "lon": 5}, parallel=True, use_cftime=True).\
hydroxr.to_netcdf(hydromo_path)
check = xr.open_dataset(hydromo_path)
hydroxr.isel(time=1).dis.plot.pcolormesh(cmap='viridis')







#Example metadata creation
data.attrs["long_name"] = "random velocity"
data.attrs["units"] = "metres/sec"
data.attrs["description"] = "A random variable created as an example."
data.x.attrs["units"] = "x units" #Can add metadata to coordinates too





