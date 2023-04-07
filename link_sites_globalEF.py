import arcpy.management
import pandas as pd
import xarray as xr

from globalEF_comparison_setup import *

#Input variables
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_0308_clean = os.path.join(process_gdb, 'EFpoints_20230308_clean')

#Input variables
isimp2b_globalef_dis_resdir = Path(resdir, 'isimp2b')
globwb_globalef_dis_resdir = Path(resdir, 'PCR_GLOBWB_2019')
isimp2b_globalef_runoff_resdir = Path(resdir, 'isimp2_qtot_accumulated15s.gdb')
globwb_globalef_runoff_resdir = Path(resdir, 'globwb_qtot_accumulated15s.gdb')

#Output variables
EFpoints_20230308_clean_globalEF = os.path.join(process_gdb, 'EFpoints_20230308_clean_globalEF')
EFpoints_20230308_clean_globalEF_tab = os.path.join(resdir, 'EFpoints_20230308_clean_globalEF.csv')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Create searchable dataframe of all hydrological and ef layers ~~~~~~~~~~~~~~~~~~~~~
# Create a searchable dataframe of layers for isimp2b dis ef
lyrsdf_globalef_isimp2b_dis = pd.DataFrame.from_dict(
    {path: Path(path).stem.split('_') for path in
     getfilelist(dir=isimp2b_globalef_dis_resdir,
                 repattern=r"((dis_smakhtinef_[abcd])|(dis_allefbutsmakhtin))[.]nc4")
     },
    orient='index',
    columns=['ghm', 'gcm', 'climate_scenario', 'human_scenario',
             'var', 'eftype', 'emc']
). \
    reset_index(). \
    rename(columns={'index' : 'path'})

lyrsdf_globalef_isimp2b_dis['res'] = "0.5 arc-deg"
lyrsdf_globalef_isimp2b_dis['run'] = lyrsdf_globalef_isimp2b_dis[
    ['ghm', 'gcm', 'climate_scenario', 'human_scenario']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)

# Create a searchable dataframe of layers for isimp2b qot ef
"""It's not possible in this case to simply delimit run components by splitting by underscore because hyphens were
 converted to underscores when processing the layers with arcpy. and some gcm, for instance, have three components.
 Therefore, identify run components thanks to the discharge df"""

lyrsdf_globalef_isimp2b_qtot_root = lyrsdf_globalef_isimp2b_dis[['ghm', 'gcm', 'climate_scenario', 'human_scenario',
                                                                 'res', 'run']]
lyrsdf_globalef_isimp2b_qtot_root['run_format'] = lyrsdf_globalef_isimp2b_qtot_root['run'].apply(
    lambda x: re.sub('[-]','_', x))
lyrsdf_globalef_isimp2b_qtot_path = pd.DataFrame.from_dict(
    {path: re.sub("_qtot.*", '', Path(path).name) for path in
     getfilelist(dir=isimp2b_globalef_runoff_resdir,
                 repattern=r"((taef)|(tennant)|(q90q50)|(tessmann)|(vmf)|(qtot))_acc15s")
     },
    orient='index',
    columns=['run_format']). \
    reset_index(). \
    rename(columns={'index' : 'path'})

lyrsdf_globalef_isimp2b_qtot = pd.merge(lyrsdf_globalef_isimp2b_qtot_path, lyrsdf_globalef_isimp2b_qtot_root,
                                        how='inner',
                                        on='run_format').drop_duplicates('path')
lyrsdf_globalef_isimp2b_qtot['var'] = 'qtot'
lyrsdf_globalef_isimp2b_qtot['eftype'] = lyrsdf_globalef_isimp2b_qtot.path.str.extract(
    pat="(allefbutsmakhtin|smakhtinef|maf|mmf)",
    expand=False)
lyrsdf_globalef_isimp2b_qtot['eftype_2'] = lyrsdf_globalef_isimp2b_qtot.path.str.extract(
    pat="(tennant|q90q50|tessmann|vmf)",
    expand=False)
lyrsdf_globalef_isimp2b_qtot['emc'] = lyrsdf_globalef_isimp2b_qtot.path.str.extract(
    pat="([abcd](?=_taef))")

lyrsdf_globalef_isimp2b_qtot.drop(columns='run_format', inplace=True)

# Create a searchable dataframe of layers for pcr_globwb high-res dis ef
lyrsdf_globalef_globwb_dis = pd.DataFrame.from_dict(
    {path: re.sub("PCR_GLOBWB_discharge_", '', Path(path).stem).split('_')
     for path in
     getfilelist(dir=globwb_globalef_dis_resdir,
                 repattern=r"PCR_GLOBWB_discharge.*")
     },
    orient='index',
    columns=['eftype', 'emc']). \
    reset_index(). \
    rename(columns={'index' : 'path'})

lyrsdf_globalef_globwb_dis['ghm'] = 'pcr-globwb'
lyrsdf_globalef_globwb_dis['gcm'] = 'cru-era'
lyrsdf_globalef_globwb_dis['climate_scenario'] = 'historical_equivalent'
lyrsdf_globalef_globwb_dis['human_scenario'] = '1860soc_equivalent'
lyrsdf_globalef_globwb_dis['res'] = '0.0833 arc-deg'
lyrsdf_globalef_globwb_dis['var'] = 'dis'

lyrsdf_globalef_globwb_dis['run'] = lyrsdf_globalef_globwb_dis[
    ['ghm', 'gcm', 'climate_scenario', 'human_scenario']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)


# Create a searchable dataframe of layers for pcr_globwb high-res qtot ef
lyrsdf_globalef_globwb_qtot = pd.DataFrame.from_dict(
    {path: re.sub("PCR_GLOBWB_runoff_", '', Path(path).stem).split('_')[0]
     for path in
     getfilelist(dir=globwb_globalef_runoff_resdir,
                 repattern=r"((taef)|(tennant)|(q90q50)|(tessmann)|(vmf)|(qtot)|(maf)|(mmf))_acc15s")
     },
    orient='index',
    columns=['eftype']). \
    reset_index(). \
    rename(columns={'index' : 'path'})

lyrsdf_globalef_globwb_qtot['ghm'] = 'pcr-globwb'
lyrsdf_globalef_globwb_qtot['gcm'] = 'cru-era'
lyrsdf_globalef_globwb_qtot['climate_scenario'] = 'historical_equivalent'
lyrsdf_globalef_globwb_qtot['human_scenario'] = '1860soc_equivalent'
lyrsdf_globalef_globwb_qtot['res'] = '0.0833 arc-deg'
lyrsdf_globalef_globwb_qtot['var'] = 'qtot'
lyrsdf_globalef_globwb_qtot['eftype_2'] = lyrsdf_globalef_globwb_qtot.path.str.extract(
    pat="(tennant|q90q50|tessmann|vmf)",
    expand=False).values
lyrsdf_globalef_globwb_qtot['emc'] = lyrsdf_globalef_globwb_qtot.path.str.extract(
    pat="([abcd](?=_taef))")

lyrsdf_globalef_globwb_qtot['run'] = lyrsdf_globalef_globwb_qtot[
    ['ghm', 'gcm', 'climate_scenario', 'human_scenario']].apply(
    lambda row: '_'.join(row.values.astype(str)), axis=1)


lyrsdf_globalef = pd.concat([lyrsdf_globalef_isimp2b_dis,
                             lyrsdf_globalef_isimp2b_qtot,
                             lyrsdf_globalef_globwb_dis,
                             lyrsdf_globalef_globwb_qtot])

#----------------------------------------- Extract by point for file geodatabase files - accumulated runoff ------------
if not arcpy.Exists(EFpoints_20230308_clean_globalEF):
    arcpy.management.CopyFeatures(EFpoints_0308_clean, EFpoints_20230308_clean_globalEF)

#Get unique identifier for each global EF value
lyrsdf_globalef['merge_efextract_df'] = lyrsdf_globalef.apply(
    lambda row: re.sub('[-]', '_', f'{row["ghm"]}_{row["gcm"]}_{row["eftype"]}_{row["eftype_2"]}_{row["emc"]}'),
    axis=1)

#List of global EF layers to extract
qtotef_extraction_list = list(
    lyrsdf_globalef[lyrsdf_globalef['var']=='qtot'].apply(
        lambda row: [row['path'],
                     row['merge_efextract_df']],
        axis = 1)
)

#Extract values directly to the points' attribute table
ExtractMultiValuesToPoints(in_point_features=EFpoints_20230308_clean_globalEF,
                           in_rasters= qtotef_extraction_list,
                           bilinear_interpolate_values='NONE')

#Import attribute table to a panda df and melt it
qtotef_df = pd.melt(
    pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            EFpoints_20230308_clean_globalEF,
            field_names=[f.name for f in arcpy.ListFields(EFpoints_20230308_clean_globalEF) if not f.name=='Shape'],
            skip_nulls=False,
            null_value=-9999
        )
    ),
    id_vars = [f.name for f in arcpy.ListFields(EFpoints_20230308_clean_globalEF)
               if f.name not in [i[1] for i in qtotef_extraction_list]+['Shape']],
    value_vars= [i[1] for i in qtotef_extraction_list]
)

#Merge it to the full set of attributes about each EF layer
qtotef_df_format = pd.merge(left=qtotef_df,
                            right=lyrsdf_globalef[lyrsdf_globalef['var']=='qtot'],
                            how='left',
                            left_on='variable',
                            right_on='merge_efextract_df',
                            validate='many_to_one'
                            )

#----------------------------------------- Extract by point for netcdf files -------------------------------------------
#Get unique identifier for the points
oidfn = arcpy.Describe(EFpoints_20230308_clean_globalEF).OIDFieldName

#Make a simple point dataframe from the reference EF points' coordinates
EFpoints_dict = pd.DataFrame.from_dict(
    {row[0]:(row[1], row[2])
     for row in arcpy.da.SearchCursor(EFpoints_20230308_clean_globalEF, ['OID@', 'POINT_X', 'POINT_Y'])},
    orient='index',
    columns=['POINT_X', 'POINT_Y']). \
    reset_index(). \
    rename(columns={'index': oidfn})

#Extract global EF values calculated with isimp2b discharge data
efpoints_globalef_isimp2b_dis_seriesofdf = lyrsdf_globalef[(lyrsdf_globalef['var']=='dis') &
                                                           (lyrsdf_globalef['res']=='0.5 arc-deg')
                                                           ].apply(
    lambda row: extract_xr_by_point(in_pointdf=EFpoints_dict,
                                    in_df_id=oidfn,
                                    in_xr=xr.open_dataset(row['path']),
                                    in_xr_lat_dimname='lat',
                                    in_xr_lon_dimname='lon',
                                    in_df_lat_dimname='POINT_Y',
                                    in_df_lon_dimname='POINT_X').assign(merge_efextract_df=row['merge_efextract_df']),
    axis=1
)

#Extract global EF values calculated with high-res GLOBWB discharge data
efpoints_globalef_globwb_dis_seriesofdf = lyrsdf_globalef[(lyrsdf_globalef['var']=='dis') &
                                                          (lyrsdf_globalef['res']=='0.0833 arc-deg')
                                                          ].apply(
    lambda row: extract_xr_by_point(in_pointdf=EFpoints_dict,
                                    in_df_id=oidfn,
                                    in_xr=xr.open_dataset(row['path']),
                                    in_xr_lat_dimname='latitude',
                                    in_xr_lon_dimname='longitude',
                                    in_df_lat_dimname='POINT_Y',
                                    in_df_lon_dimname='POINT_X').assign(merge_efextract_df=row['merge_efextract_df']),
    axis=1
)

#Merge data from isimp2b and high-res GLOBWB and melt them

disef_df = pd.melt(
    pd.concat(
        efpoints_globalef_isimp2b_dis_seriesofdf.tolist() +
        efpoints_globalef_globwb_dis_seriesofdf.tolist()),
    id_vars=[oidfn, 'month', 'merge_efextract_df'],
    value_vars=['tennant', 'q90q50', 'tessmann', 'vmf', 'taef'],
    var_name='eftype_2'
)
disef_df = disef_df[~disef_df['value'].isna()]
disef_df.groupby(['OBJECTID', 'merge_efextract_df', 'eftype_2']).sum('value')
disef_df.columns

#Join them to unique IDs for reference EF points
efp_ids = pd.DataFrame(
    arcpy.da.TableToNumPyArray(
        EFpoints_20230308_clean_globalEF,
        field_names=[f.name for f in arcpy.ListFields(EFpoints_20230308_clean_globalEF)
                     if f.name not in [i[1] for i in qtotef_extraction_list]+['Shape']],
        skip_nulls=False,
        null_value=-9999
    )
)

disef_df_efp = pd.merge(
    disef_df,
    efp_ids,
    on=oidfn,
    how='left',
    validate='many_to_one'
)

#Merge it to the full set of attributes about each EF layer
disef_df_format = pd.merge(left=disef_df_efp,
                        right=lyrsdf_globalef[lyrsdf_globalef['var']=='dis'],
                        how='left',
                        on='merge_efextract_df',
                        validate='many_to_one'
                        )

#Merge data from discharge and accumulated runoff calculations
allef_df_format = pd.concat([disef_df_format,
                            qtotef_df_format])
allef_df_format = allef_df_format[~(allef_df_format['value'].isna())]

allef_df_format.groupby([c for c in allef_df_format.columns if not c in ['month', 'value']]).sum('value')

#Format
allef_df_format['eftype_format'] = allef_df_format['eftype_format']


allef_df_format.to_csv(
    EFpoints_20230308_clean_globalEF_tab
)

#Clean fields up




allef_df_format[allef_df_format['OBJECTID']==6].to_csv(Path(resdir,'test6.csv'))

#Make sure to get dis MAR from GHMs