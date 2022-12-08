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
    print(f"Processing {row['monthly_path']}")
    xr.open_dataset(row['path']).resample(time='1MS').mean().to_netcdf(row['monthly_path'])
lyrsdf.apply(aggregate_fromdf, axis=1)

##Compute mean monthly flow (MMF) and monthly SD
for run in lyrsdf['run'].unique():
    print(run)
    lyrsdf_sub = lyrsdf.loc[lyrsdf['run']==run]
    run_xr = xr.open_mfdataset(lyrsdf_sub.monthly_path, concat_dim="time", combine='nested',
                                   chunks={"time": 2400, "lat": 30, "lon": 30}, parallel=True, use_cftime=True)

    run_xr['month'] = xr.concat(
        [run_xr.where(run_xr.time <= max([i for i in run_xr["time"].values if i.calendar == 'gregorian']),
                     drop=True)['time'].dt.strftime("%m"),
        run_xr.where(run_xr.time > max([i for i in run_xr["time"].values if i.calendar == 'gregorian']),
                     drop=True)['time'].dt.strftime("%m")],
        dim='time'
    )

    #Remove outliers - monthly flow values over mean+3sd or under mean-3sd
    run_mmfplus3sd = run_xr.groupby('month').mean() + 3*run_xr.groupby('month').std()
    run_mmfminus3sd = run_xr.groupby('month').mean() - 3*run_xr.groupby('month').std()

    check = ds.where(run_xr.dis > run_mmfplus3sd....

    check.isel(time=0).dis.plot.pcolormesh(cmap='viridis')

#Compute mean annual flow (MAF)
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





