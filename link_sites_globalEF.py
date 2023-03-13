from globalEF_comparison_setup import *
import pandas as pd
import xarray as xr

#Input variables
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_0308_clean = os.path.join(process_gdb, 'EFpoints_20230308_clean')

#Input variables
isimp2b_resdir = Path(resdir, 'isimp2b')
globwb_resdir = Path(resdir, 'PCR_GLOBWB_2019')

# Create a searchable dataframe of hydrological layers
lyrsdf_globalef = pd.DataFrame.from_dict(
    {path: path.stem.split('_') for path in
     isimp2b_resdir.glob('*[.]nc4') if
     re.search(r"((smakhtinef_[abcd])|(allefbutsmakhtin))", str(path))},
    orient='index',
    columns=['ghm', 'gcm', 'climate_scenario', 'human_scenario',
             'var',  'eftype', 'emc']
). \
    reset_index().\
    rename(columns={'index' : 'path'})

lyrsdf_globalef['run'] = lyrsdf_globalef[['ghm', 'gcm', 'climate_scenario', 'human_scenario', 'var']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)

#Output variables
EFpoints_20230308_clean_globalEFtab = os.path.join(process_gdb, 'EFpoints_20230308_clean_globalEF')

#----------------------------------------- Extract by point ------------------------------------------------------------
#Use xarray so as not to have to create temporary raster layers iteratively for each netdf
def extract_xr_by_point(in_xr, in_pointdf, in_df_id,
                        in_xr_lon_dimname='lon', in_xr_lat_dimname='lat',
                        in_df_lon_dimname='POINT_X', in_df_lat_dimname='POINT_Y'
                        ):
    df_asxr = in_pointdf.set_index(in_df_id).to_xarray()

    isel_dict = {in_xr_lon_dimname: df_asxr[in_df_lon_dimname],
                 in_xr_lat_dimname: df_asxr[in_df_lat_dimname],}
    pixel_values = in_xr.sel(isel_dict , method="nearest")
    pixel_values_df = pixel_values.reset_coords(drop=True).\
        to_dataframe().\
        reset_index()
    return(pixel_values_df)


oidfn = arcpy.Describe(EFpoints_0308_clean).OIDFieldName

EFpoints_dict = pd.DataFrame.from_dict(
    {row[0]:(row[1], row[2])
     for row in arcpy.da.SearchCursor(EFpoints_0308_clean, ['OID@', 'POINT_X', 'POINT_Y'])},
    orient='index',
    columns=['POINT_X', 'POINT_Y']).\
    reset_index().\
    rename(columns={'index': oidfn})


efpoints_globalef_seriesofdf = lyrsdf_globalef.apply(
    lambda row: extract_xr_by_point(in_pointdf=EFpoints_dict,
                                    in_df_id=oidfn,
                                    in_xr=xr.open_dataset(row['path']),
                                    in_xr_lat_dimname='lat',
                                    in_xr_lon_dimname='lon',
                                    in_df_lat_dimname='POINT_Y',
                                    in_df_lon_dimname='POINT_X').assign(run=row['run'],
                                                                        var=row['var'],
                                                                        eftype=row['eftype'],
                                                                        emc=row['emc']),
    axis=1
)

efpoints_globalef_df = pd.concat(
    efpoints_globalef_seriesofdf.tolist())

#Make sure to get MAR from GHMs