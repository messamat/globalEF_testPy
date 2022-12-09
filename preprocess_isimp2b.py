import cftime
from datetime import datetime
import os
import pandas as pd
from pathlib import Path
import re
import xarray as xr


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

    #

    check.isel(time=15).dis.plot()

    check.isel(month=0).dis.plot.pcolormesh(cmap='viridis')








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


# Compute mean monthly flow (MAF)
#Compute Q90
#Compute Q50









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





