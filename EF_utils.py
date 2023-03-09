import numpy as np
import os
from pathlib import Path
from scipy import interpolate
import xarray as xr

def spatiotemporal_chunk_optimized_acrosstime(in_xr, lat_dimname='lat', lon_dimname='lon', time_dimname='time'):
    #This function rechunks xarray in chunks of 1,000,000 elements (ideal chunk size) — https://xarray.pydata.org/en/v0.10.2/dask.html
    n_timesteps = in_xr.dims[time_dimname]
    spatial_chunk_size = np.ceil((1000000/n_timesteps)**(1/2))
    kwargs = {
        time_dimname: n_timesteps,
        lat_dimname: spatial_chunk_size,
        lon_dimname: spatial_chunk_size
    }
    return(in_xr.chunk(**kwargs))

# Resample at monthly scale
# Some of the simulations have negative values of discharge and/or runoff
def aggregate_monthly_fromdf(row, remove_negative_values=True, vname = 'dis'):
    if not row['monthly_path'].exists():
        print(f"Processing {row['monthly_path']}")
        xr_toresample = xr.open_dataset(row['path'])
        if remove_negative_values:
            xr_toresample[vname] = xr.where(xr_toresample[vname] < 0, 0, xr_toresample[vname])
        xr_toresample.resample(time='1MS').mean().to_netcdf(row['monthly_path'])
    else:
        print(f"{row['monthly_path']} already exists. Skipping... ")

#### -------------- Function to compute Tennant, Q90Q50, Tessmann and VMF eflows ---------------------------------------------------
def compute_monthlyef_notsmakhtin(in_xr, out_efnc, vname = 'dis', time_dimname = 'time',
                                  lat_dimname = 'lat', lon_dimname = 'lon', remove_outliers = True):
    # Remove outliers - monthly flow values over mean+3sd or under mean-3sd
    if remove_outliers:
        run_nooutliers = spatiotemporal_chunk_optimized_acrosstime(
            in_xr.where(
                in_xr.groupby('month') <= ((in_xr.groupby('month').mean(dim='time') +
                                            3 * in_xr.groupby('month').std(dim='time')))
            ).where(
                in_xr.groupby('month') >= ((in_xr.groupby('month').mean(dim='time') -
                                            3 * in_xr.groupby('month').std(dim='time')))
            ),
            time_dimname = time_dimname,
            lat_dimname = lat_dimname,
            lon_dimname = lon_dimname
        )
    else:
        run_nooutliers = spatiotemporal_chunk_optimized_acrosstime(in_xr,
                                                                   time_dimname = time_dimname,
                                                                   lat_dimname = lat_dimname,
                                                                   lon_dimname = lon_dimname)

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
                                     0.4 * run_maf).rename({vname:"tennant"}). \
            merge(xr.where(run_mmf <= run_maf,
                           run_q90,
                           run_q50).rename({vname: "q90q50"})). \
                merge(xr.where(run_mmf <= 0.4 * run_maf,
                       run_mmf,
                       xr.where(0.4 * run_mmf > 0.4 * run_maf,
                                0.4 * run_mmf,
                                0.4 * run_maf)
                       ).rename({vname: "tessmann"})). \
        merge(xr.where(run_mmf <= 0.4 * run_maf,
                       0.6 * run_mmf,
                       xr.where(run_mmf > 0.8 * run_maf,
                                0.3 * run_mmf,
                                0.45 * run_maf)
                       ).rename({vname: "vmf"}))

    global_monthly_eflows.to_netcdf(out_efnc)


#### -------------- Functions to compute modern smakthin method ---------------------------------------------------------------------
#Generate flow duration curve for each cell
def compute_xrfdc(in_xr, quant_list, vname = 'dis'):
    fdc_xr = in_xr.quantile(q=quant_list, dim='time'). \
        rename({"quantile" : "exceedance_prob", vname : f"fdcv"})
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
def compute_smakhtinef_ts(cell, n_shift=1, loginterp_padding = 0.00001, vname = 'dis'):
    fdc = np.round(cell.fdcv.values.squeeze() + loginterp_padding, 5)
    maxval = cell[vname].values.max()

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
        ts_eflow_unfloored = np.exp(f_shift(np.log(cell[vname] + loginterp_padding)))
        ts_eflow_unfloored[(ts_eflow_unfloored <= loginterp_padding)] = loginterp_padding #Bound it at 0 (after padding removal)

        return (
                xr.DataArray(
                    # Make sure that linear extrapolation based on last two values did not lead to e-flow values higher than source data
                    xr.where(ts_eflow_unfloored > (cell[vname].values + loginterp_padding),
                             cell[vname].values,
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
        ts_eflow = f_shift(cell[vname])
        return (
            xr.DataArray(ts_eflow.squeeze(), dims='time')
        )

    else: #If only one unique value
        return(
            xr.DataArray(cell[vname].values.squeeze(), dims='time')
        )

# To deal with zeros
# check = run_fdc_merge_disk.isel(lat=slice(130,131), lon=slice(426, 427)).stack(gridcell=["lat", "lon"]). \
#     groupby("gridcell"). \
#     map(compute_smakhtinef_ts, args=[n_shift])
#
# cell = run_fdc_merge_disk.isel(lat=130, lon=426, time=slice(0,120))
# check = compute_smakhtinef_ts(cell)

def compute_smakhtinef_stats(in_xr, out_dir, out_efnc_basename, n_shift=1, vname='dis',
                             lat_dimname='lat', lon_dimname='lon',
                             scratch_file = 'scratch.nc4'):
    #Compute time series of e-flow based on Smakthin flow duration curve method

    #Quantiles used in deriving flow duration curve
    smakthin_quantlist = [0.0001, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3,
                                    0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 0.9999]

    #Generate flow duration curve for each cell
    print("Compute flow duration curve for each cell")
    fdc_xr = compute_xrfdc(in_xr = in_xr,
                           quant_list = smakthin_quantlist,
                           vname = vname)
    xr.merge([in_xr, fdc_xr]).to_netcdf(Path(out_dir, scratch_file))
    run_fdc_merge_disk = xr.open_dataset(Path(out_dir, scratch_file))

    #Compute e-flow values for each cell
    print("Compute e-flow values for each cell")
    smakhtinef = run_fdc_merge_disk.stack(gridcell=[lat_dimname, lon_dimname]).\
        groupby("gridcell").\
        map(compute_smakhtinef_ts, args=[n_shift, 0.00001, vname]).\
        unstack('gridcell')
    smakhtinef['time'] = run_fdc_merge_disk.time

    #Compute e-flow relative to MAF
    print("Compute e-flow relative to MAF")
    run_maf = run_fdc_merge_disk[vname].mean(dim='time') #Mean Flow
    smakhtinef_mean = xr.merge([run_maf,
                                  xr.Dataset(dict(ef_a_mean = smakhtinef.mean(dim='time').round(5)))])
    smakhtinef_relative = xr.where((smakhtinef_mean[vname].round(decimals=5) > 0) | (np.isnan(smakhtinef_mean[vname])),
                                     (smakhtinef_mean.ef_a_mean/smakhtinef_mean[vname].round(decimals=5)).values, 1)
    #Compute total annual e-flow
    print("Compute total annual e-flow")
    smakhtinef_taef = smakhtinef.groupby('month').mean(skipna=True).sum(dim='month', skipna=False)

    #Merge and write out
    print("Merge and write-out")
    xr.Dataset(dict(raef=smakhtinef_relative, taef=smakhtinef_taef)).\
        to_netcdf(Path(out_dir, out_efnc_basename))

    del run_fdc_merge_disk

    try:
        os.remove(Path(out_dir, scratch_file))
    except OSError:
        pass
