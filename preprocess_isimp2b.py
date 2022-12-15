import os
import numpy as np
import pandas as pd
from pathlib import Path
import re
from scipy import interpolate
#import time
import xarray as xr
#import cProfile as profile
#import pstats
from GEFIS_setup import * #See this module for directory structure

#Set up structure
isimp2b_datdir = Path(datdir, 'isimp2b')

#Function
def spatiotemporal_chunk_optimized_acrosstime(in_xr):
    #This function rechunks xarray in chunks of 1,000,000 elements (ideal chunk size) — https://xarray.pydata.org/en/v0.10.2/dask.html
    n_timesteps = in_xr.dims['time']
    spatial_chunk_size = np.ceil((1000000/n_timesteps)**(1/2))
    return(in_xr.chunk(time=n_timesteps, lat=spatial_chunk_size, lon=spatial_chunk_size))

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

# Resample at monthly scale
# Some of the simulations have negative values of discharge and/or runoff
def aggregate_monthly_fromdf(row, remove_negative_values=True):
    if not row['monthly_path'].exists():
        print(f"Processing {row['monthly_path']}")
        xr_toresample = xr.open_dataset(row['path'])
        if remove_negative_values:
            xr_toresample['dis'] = xr.where(xr_toresample.dis < 0, 0, xr_toresample.dis)
        xr_toresample.resample(time='1MS').mean().to_netcdf(row['monthly_path'])
    else:
        print(f"{row['monthly_path']} already exists. Skipping... ")
lyrsdf.apply(aggregate_monthly_fromdf, axis=1)

#### -------------- Function to compute Tennant, Q90Q50, Tessmann and VMF eflows ---------------------------------------------------
def compute_monthlyef_notsmakhtin(in_xr, out_efnc, remove_outliers = True):
    # Remove outliers - monthly flow values over mean+3sd or under mean-3sd
    if remove_outliers:
        run_nooutliers = spatiotemporal_chunk_optimized_acrosstime(
            in_xr.where(
                in_xr.groupby('month') <= ((in_xr.groupby('month').mean(dim='time') +
                                            3 * in_xr.groupby('month').std(dim='time')))
            ).where(
                in_xr.groupby('month') >= ((in_xr.groupby('month').mean(dim='time') -
                                            3 * in_xr.groupby('month').std(dim='time')))
            )
        )
    else:
        run_nooutliers = spatiotemporal_chunk_optimized_acrosstime(in_xr)

    # Compute mean monthly flow (mmf)
    run_mmf = run_nooutliers.groupby('month').mean(dim='time')
    # Compute mean annual flow (maf)
    run_maf = run_nooutliers.mean(dim='time')
    # Compute Q90
    run_q90 = run_nooutliers.quantile(q=0.1, dim='time')
    # Compute Q50
    run_q50 = run_nooutliers.quantile(q=0.5, dim='time')

    # Compute Tennant, Q90_Q50, Tessmann and VMF e-flows
    global_monthly_eflows = xr.where(run_mmf <= run_maf,
                                     0.2 * run_maf,
                                     0.4 * run_maf).rename({"dis": "tennant"}). \
        merge(xr.where(run_mmf <= run_maf,
                       run_q90,
                       run_q50).rename({"dis": "q90q50"})). \
        merge(xr.where(run_mmf <= 0.4 * run_maf,
                       run_mmf,
                       xr.where(0.4 * run_mmf > 0.4 * run_maf,
                                0.4 * run_mmf,
                                0.4 * run_maf)
                       ).rename({"dis": "tessmann"})). \
        merge(xr.where(run_mmf <= 0.4 * run_maf,
                       0.6 * run_mmf,
                       xr.where(run_mmf > 0.8 * run_maf,
                                0.3 * run_mmf,
                                0.45 * run_maf)
                       ).rename({"dis": "vmf"}))

    global_monthly_eflows.to_netcdf(out_efnc)

#### -------------- Functions to compute modern smakthin method ---------------------------------------------------------------------
#Generate flow duration curve for each cell
def compute_xrfdc(in_xr, quant_list):
    fdc_xr = in_xr.quantile(q=quant_list, dim='time'). \
        rename({"quantile" : "exceedance_prob", "dis" : "fdc_dis"})
    fdc_xr["exceedance_prob"] = 1 - fdc_xr["exceedance_prob"]
    return(fdc_xr)

#loginterp_padding helps with running logarithmc interpolation for grids that have 0s
#Use interpolate.interp1d because allows extrapolation. But returns 0 when interval between two xs is 0.
#Example values for fdc to test the second case when not enough unique values in the FDC to interpolate after shifting
#But more than one unique value
# maxval = 50000
# fdc = np.array([0.001, 500, 1000, 10000])
# n_shift = 3
#Code to test on an individual cell:compute_smakhtinef_ts(run_fdc_merge.isel(lat=67, lon=153))
# Test nfc 'D:/IWMI_GEFIStest/results/isimp2b/watergap2-2c_miroc5_ewembi_picontrol_1860soc_co2_dis_global_monthly_1661_1670.nc4'
# cell = run_fdc_merge.sel(lat=69.75, lon=-169.25) #Example with only no data (in the sea)
# cell = run_fdc_merge.sel(lat=45.75, lon=5.25) #Example with data
# cell = run_fdc_merge.sel(lat=25.25, lon=22.25) #Example with 0s
def compute_smakhtinef_ts(cell, n_shift=1, loginterp_padding = 0.00001):
    fdc = np.round(cell.fdc_dis.values.squeeze() + loginterp_padding, 5)
    maxval = cell.dis.values.max()

    if (len(np.unique(fdc[~np.isnan(fdc)]))-n_shift) > 1: #If enough unique values in the FDC to interpolate after shifting
        xp = np.log(fdc)
        fp = np.roll(xp, n_shift)
        # Get unique FDC values (to avoid having errors in interp1d with 0 intervals on the x-axis
        xp_shift_unique = np.unique(xp[(n_shift):], return_index=True)

        #Build interpolation function
        f_shift = interpolate.interp1d(x=xp_shift_unique[0],
                                       y=fp[(n_shift):][xp_shift_unique[1]],
                                       kind='linear', fill_value='extrapolate')
        #Get logarithmic interpolation outputs from grid cell values - generate new flow time series
        ts_eflow_unfloored = np.exp(f_shift(np.log(cell.dis + loginterp_padding)))
        ts_eflow_unfloored[(ts_eflow_unfloored <= loginterp_padding)] = loginterp_padding #Bound it at 0 (after padding removal)

        return (
                xr.DataArray(
                    # Make sure that linear extrapolation based on last two values did not lead to e-flow values higher than source data
                    xr.where(ts_eflow_unfloored > (cell.dis.values + loginterp_padding),
                             cell.dis.values,
                             (ts_eflow_unfloored - loginterp_padding)
                             ).squeeze(),
                    dims='time')
        )

    elif len(np.unique(fdc[~np.isnan(fdc)])) > 1: #If more than one unique value but not enough to run linear interpolation after shift
        #Occurs very rarely and only when applied to little data. Probably only in short records in arid places (where most of the time series
        #is 0. In this case, interpolate linearly (rather than on log scale)

        #Add 0 and maximum value (in record) to the source intervals for the interpolation
        xp = np.append(
            np.insert(fdc[(n_shift):] - loginterp_padding,
                      0, 0),
            maxval)
        #Add 0 and lowest value that was shifted out to destination intervals for the interpolation
        fp = np.append(
            np.insert(np.roll(fdc, n_shift)[(n_shift):] - loginterp_padding,
                      0, 0),
            fdc[max(0, len(fdc)-n_shift)])

        xp_unique = np.unique(xp, return_index=True)

        #Interpolate (in this case, the extrapolation is controlled and is bound to 0 on one side and to the lowest
        #value that was shifted out on the other side)
        f_shift = interpolate.interp1d(x=xp_unique[0],
                                       y=fp[xp_unique[1]],
                                       kind='linear', fill_value='extrapolate')
        ts_eflow = f_shift(cell.dis)
        return (
            xr.DataArray(ts_eflow.squeeze(), dims='time')
        )

    else: #If only one unique value
        return(
            xr.DataArray(cell.dis.values.squeeze(), dims='time')
        )

# To deal with zeros
# check = run_fdc_merge_disk.isel(lat=slice(130,131), lon=slice(426, 427)).stack(gridcell=["lat", "lon"]). \
#     groupby("gridcell"). \
#     map(compute_smakhtinef_ts, args=[n_shift])
#
# cell = run_fdc_merge_disk.isel(lat=130, lon=426, time=slice(0,120))
# check = compute_smakhtinef_ts(cell)

def compute_smakhtinef_stats(in_xr, out_dir, out_efnc_basename, n_shift=1):
    #Compute time series of e-flow based on Smakthin flow duration curve method
    smakthin_quantlist = [0.0001, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3,
                                    0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 0.9999]
    fdc_xr = compute_xrfdc(in_xr = in_xr,
                           quant_list = smakthin_quantlist)
    xr.merge([in_xr, fdc_xr]).to_netcdf(Path(out_dir, 'scratch2.nc4'))
    run_fdc_merge_disk = xr.open_dataset(Path(out_dir, 'scratch2.nc4'))

    smakhtinef_a = run_fdc_merge_disk.stack(gridcell=["lat", "lon"]).\
        groupby("gridcell").\
        map(compute_smakhtinef_ts, args=[n_shift]).\
        unstack('gridcell')
    smakhtinef_a['time'] = run_fdc_merge_disk.time

    #Compute e-flow relative to MAF
    run_maf = run_fdc_merge_disk.dis.mean(dim='time')
    smakhtinef_a_mean = xr.merge([run_maf,
                                  xr.Dataset(dict(ef_a_mean = smakhtinef_a.mean(dim='time').round(5)))])
    smakhtinef_a_relative = xr.where((smakhtinef_a_mean.dis.round(decimals=5) > 0) | (np.isnan(smakhtinef_a_mean.dis)),
                                     (smakhtinef_a_mean.ef_a_mean/smakhtinef_a_mean.dis.round(decimals=5)).values, 1)
    #Compute total annual e-flow
    smakhtinef_a_taef = smakhtinef_a.groupby('month').mean(skipna=True).sum(dim='month',skipna=False)

    #Merge and write out
    xr.Dataset(dict(raef=smakhtinef_a_relative, taef=smakhtinef_a_taef)).\
        to_netcdf(Path(out_dir, out_efnc_basename))


#### -------------- Running analysis ----------------------------------------------------------------------------------
for run in lyrsdf['run'].unique():
    print(run)
    lyrsdf_sub = lyrsdf.loc[lyrsdf['run']==run]

    run_xr = spatiotemporal_chunk_optimized_acrosstime(
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
        compute_monthlyef_notsmakhtin(in_xr = run_xr, out_efnc=outpath_ef_notsmakhtin , remove_outliers=True)
    else:
        print(f"{outpath_ef_notsmakhtin.name} already exists. Skipping...")


    for shift, emc in enumerate(['a', 'b', 'c', 'd']):
        outpath_ef_smakhtin = f'{run}_smakhtinef_{emc}.nc4'
        if not Path(isimp2b_resdir, outpath_ef_smakhtin).exists():
            print(f"Processing {outpath_ef_smakhtin}")
            compute_smakhtinef_stats(in_xr = run_xr, out_dir = isimp2b_resdir,
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



