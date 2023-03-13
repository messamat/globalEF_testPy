import arcpy.management
import numpy as np
import datetime
from globalEF_comparison_setup import *
import pandas as pd

rhonedir = os.path.join(datdir, 'GEFIS_test_data', 'Data by country', 'Europe', 'France', 'Rhone')

def paste3(*args, sep=", "):
    L = list(args)
    L = [list(map(lambda x: "" if x is None else x, l)) for l in L]
    ret = re.sub(f"(^{sep}|{sep}$)", "", re.sub(f"{sep}{sep}", sep, sep.join(["".join(map(str, l)) + sep for l in L])))
    ret = None if ret == "" else ret
    return ret

efpoints = os.path.join(resdir, "france_preprocessing.gdb", "Rhone_EFpoints_reportsraw")
ssbv_FRD = os.path.join(rhonedir, "ssbv_FRD.shp")

# Create new points based on original coordinates and project to Lambert 93
efpoints_originalwgs84 = os.path.join(resdir, "france_preprocessing.gdb", "Rhone_EFpoints_originalwgs84")
if not arcpy.Exists(efpoints_originalwgs84):
    arcpy.management.XYTableToPoint(in_table=efpoints, out_feature_class=efpoints_originalwgs84,
                                    x_field='Longitude_original', y_field='Latitude_original',
                                    coordinate_system = arcpy.SpatialReference(4326))

efpoints_originallambert = os.path.join(resdir, "france_preprocessing.gdb", "Rhone_EFpoints_originallambert")
if not arcpy.Exists(efpoints_originallambert):
    arcpy.management.Project(efpoints_originalwgs84, efpoints_originallambert,
                             out_coor_system=arcpy.SpatialReference(2154)
                             )
    arcpy.management.AddGeometryAttributes(efpoints_originallambert,
                                           Geometry_Properties='POINT_X_Y_Z_M')
    arcpy.AlterField_management(efpoints_originallambert, field='POINT_X',
                                new_field_name='Longitude_original_Lambert',
                                new_field_alias='Longitude_original_Lambert')
    arcpy.AlterField_management(efpoints_originallambert, field='POINT_Y',
                                new_field_name='Latitude_original_Lambert',
                                new_field_alias='Latitude_original_Lambert')

# Join the sub-basin polygons to the points and get basin name for each point
efpoints_originallambert_ssbvjoin = os.path.join(resdir, "france_preprocessing.gdb",
                                                 "Rhone_EFpoints_originallambert_ssbvjoin")
if not arcpy.Exists(efpoints_originallambert_ssbvjoin):
    arcpy.analysis.SpatialJoin(target_features=efpoints_originallambert,
                               join_features=ssbv_FRD,
                               out_feature_class=efpoints_originallambert_ssbvjoin,
                               join_operation='JOIN_ONE_TO_ONE',
                               join_type='KEEP_ALL',
                               match_option='INTERSECT'
                               )

# Find the latest file in the directory that matches the pattern
efdata_csv = os.path.join(rhonedir, 'efdata_csvforjoin.csv')
if not arcpy.Exists(efdata_csv):
    efdata_xl = pd.read_excel(
        getfilelist(rhonedir, "France_Rhone_catchment_extracted_.*")[-1])
    efdata_xl.to_csv(efdata_csv)

efpts_efdata_jointab_path = os.path.join(rhonedir, 'efpts_efdata_jointab.csv')

arcpy.MakeFeatureLayer_management(efpoints_originallambert_ssbvjoin, 'efcoors_lambert')
arcpy.env.qualifiedFieldNames = False
arcpy.management.AddJoin('efcoors_lambert', in_field='UID_Mathis',
                         join_table=efdata_csv, join_field='UID_Mathis',
                         join_type='KEEP_COMMON')
arcpy.management.CopyRows('efcoors_lambert', efpts_efdata_jointab_path)

efpts_efdata_jointab = pd.read_csv(efpts_efdata_jointab_path)
efpts_efdata_jointab.drop(labels=["UID_Mathis_1", "Code_station_1", "Cours_deau_1"],
                          axis=1,
                          inplace=True)

#Only needed if efdtata and ecostate were joined and then exported to csv
efpts_efdata_jointab.columns = pd.unique(
    [re.sub(r'[ -]+', '_', f.aliasName) for
     f in arcpy.ListFields('efcoors_lambert') if
     f.aliasName!='Shape']) #Use np.unique to get rid of second UID

#Only keep the most recent ecological state assessment
ecostate = pd.read_csv(os.path.join(rhonedir, "etat_stations_filtrees.csv"), sep=";", encoding="UTF-8")
ecostate_last = ecostate.sort_values(by=['annee']).groupby('numero_station').last()
ecostate_last_csv = os.path.join(rhonedir, 'ecostate_last.csv')
ecostate_last.to_csv(ecostate_last_csv)

#Merge EF points with all ecological state assessments on the Masse d'eau
#Multiple stations per Masse d'eau
#Compute distance between masse d'eau and ef point
efdata_ecostate_tab = efpts_efdata_jointab.merge(ecostate_last, how='outer',
                    left_on='Code_masse_deau', right_on='code_MDO')
#UID_Mathis was duplicated with arcpy.management.AddJoin('efcoors_lambert', in_field='UID_Mathis', join_table=efdata_csv)

efdata_ecostate_tab_sub = efdata_ecostate_tab.dropna(axis=0, how='any',
                                                     subset=['Longitude_original_Lambert',
                                                             'Latitude_original_Lambert'])
efdata_ecostate_tab_sub['efecodist'] = np.sqrt((efdata_ecostate_tab_sub['Longitude_original_Lambert'] -
                                                efdata_ecostate_tab_sub['x_lambert93'])**2 +
                                               (efdata_ecostate_tab_sub['Latitude_original_Lambert'] -
                                                efdata_ecostate_tab_sub['y_lambert93'])**2
                                               )

# Only keep the most recent ecological state assessment
efdata_ecostate_tab_sub_nearest = efdata_ecostate_tab_sub.sort_values('efecodist'). \
    groupby('UID_Mathis'). \
    first(). \
    reset_index(). \
    drop(
    columns=[
        'x_lambert93', 'y_lambert93', 'efecodist',
        'Longitude_original_Lambert', 'Latitude_original_Lambert'
    ]). \
    rename(columns={
    'LIB_SSBV': 'Sub-Basin', 'annee': 'ecostate_annee', 'ECO': 'ecostate'
})


# Remove records with NAs for e-flows
efdata_noNA = efdata_ecostate_tab_sub_nearest.dropna(subset=[
    'Débit_minimum_biologique_m3s_DMO', 'Débit_biologique_valeur_haute_m3s_DBh',
    'Débit_biologique_valeur_basse_m3s_DBb', 'Débit_de_survie_proposée_m3s_DBs'],
    axis=0,
    how='all')

efdata_noNA['eflow_selected'] = np.nan
efdata_noNA['eflow_selected_type'] = ""
efdata_noNA['eflow_methodtype'] = ""
efdata_noNA['eflow_methodname'] = ""

# Select e-flow value to use
# If there is a débit minimum biologique, use that one
efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                'eflow_selected'] = \
    efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                    'Débit_minimum_biologique_m3s_DMO']

efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                'eflow_selected_type'] = "Débit minimum biologique"

efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                'eflow_selected_methodtype'] = \
    efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                    'Méthode_type_Débit_minimum_biologique']

efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                'eflow_selected_methodname'] = \
    efdata_noNA.loc[~efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna(),
                    'Méthode_nom_Débit_minimum_biologique']




# If no débit minimum biologique, but a single "débit biologique" value, use that one
# select rows where Débit_minimum_biologique_m3s_DMO is NA and
# either Débit_biologique_valeur_haute_m3s_DBh or Débit_biologique_valeur_basse_m3s_DBb is not NA
mask = (efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna() &
        ((~efdata_noNA['Débit_biologique_valeur_haute_m3s_DBh'].isna() &
          efdata_noNA['Débit_biologique_valeur_basse_m3s_DBb'].isna()) |
         (efdata_noNA['Débit_biologique_valeur_haute_m3s_DBh'].isna() &
          ~efdata_noNA['Débit_biologique_valeur_basse_m3s_DBb'].isna())))

# calculate row means for selected columns and set new columns with appropriate values
efdata_noNA.loc[mask, 'eflow_selected'] = efdata_noNA.loc[mask, ['Débit_biologique_valeur_haute_m3s_DBh',
                                                                 'Débit_biologique_valeur_basse_m3s_DBb']].\
    mean(axis=1, skipna=True)

efdata_noNA.loc[mask, 'eflow_selected_type'] = "Débit biologique - single value"
efdata_noNA.loc[mask, 'eflow_selected_methodtype']  = [
    re.sub('None', '', r) for r in
    np.where(
        (efdata_noNA.loc[mask, 'Méthode_type_DBh'] == efdata_noNA.loc[mask, 'Méthode_type_DBb']) &
        ~(efdata_noNA.loc[mask, 'Méthode_type_DBh'].isna() | efdata_noNA.loc[mask, 'Méthode_type_DBb'].isna()),
        efdata_noNA.loc[mask, 'Méthode_type_DBh'].astype(str),
        efdata_noNA.loc[mask, 'Méthode_type_DBh'].astype(str) + efdata_noNA.loc[mask, 'Méthode_type_DBb'].astype(str)
    )]

efdata_noNA.loc[mask, 'eflow_selected_methodtype'] = [
    re.sub('None', '', r) for r in
    np.where(
        (efdata_noNA.loc[mask, 'Méthode_type_DBh'] == efdata_noNA.loc[mask, 'Méthode_type_DBb']) &
        ~(efdata_noNA.loc[mask, 'Méthode_type_DBh'].isna() | efdata_noNA.loc[mask, 'Méthode_type_DBb'].isna()),
        efdata_noNA.loc[mask, 'Méthode_type_DBh'].astype(str),
        efdata_noNA.loc[mask, 'Méthode_type_DBh'].astype(str) + efdata_noNA.loc[mask, 'Méthode_type_DBb'].astype(str)
    )]


efdata_noNA.loc[mask, 'eflow_selected_methodname']  = [
    re.sub('None', '', r) for r in
    np.where(
        (efdata_noNA.loc[mask, 'Méthode_nom_DBh'] == efdata_noNA.loc[mask, 'Méthode_nom_DBb']) &
        ~(efdata_noNA.loc[mask, 'Méthode_nom_DBh'].isna() | efdata_noNA.loc[mask, 'Méthode_nom_DBb'].isna()),
        efdata_noNA.loc[mask, 'Méthode_nom_DBh'].astype(str),
        efdata_noNA.loc[mask, 'Méthode_nom_DBh'].astype(str) + efdata_noNA.loc[mask, 'Méthode_nom_DBb'].astype(str)
    )]

#If no débit minimum biologique, but a low and high "débit biologique" value, get the average
mask2 = efdata_noNA['Débit_minimum_biologique_m3s_DMO'].isna() & \
            (~efdata_noNA['Débit_biologique_valeur_haute_m3s_DBh'].isna()) & \
            (~efdata_noNA['Débit_biologique_valeur_basse_m3s_DBb'].isna())

efdata_noNA.loc[mask2, 'eflow_selected'] = efdata_noNA.loc[mask2, ['Débit_biologique_valeur_haute_m3s_DBh',
                                                                       'Débit_biologique_valeur_basse_m3s_DBb']].\
    mean(axis=1)
efdata_noNA.loc[mask2, 'eflow_selected_type'] =  'Débit biologique - average of high and low values'

efdata_noNA.loc[mask2, 'eflow_selected_methodtype'] = [
    re.sub('None', '', r) for r in
    np.where(
        (efdata_noNA.loc[mask2, 'Méthode_type_DBh'] == efdata_noNA.loc[mask2, 'Méthode_type_DBb']) &
        ~(efdata_noNA.loc[mask2, 'Méthode_type_DBh'].isna() | efdata_noNA.loc[mask2, 'Méthode_type_DBb'].isna()),
        efdata_noNA.loc[mask2, 'Méthode_type_DBh'].astype(str),
        efdata_noNA.loc[mask2, 'Méthode_type_DBh'].astype(str) + '+' + efdata_noNA.loc[mask2, 'Méthode_type_DBb'].astype(str)
    )]

efdata_noNA.loc[mask2, 'eflow_selected_methodname'] = [
    re.sub('None', '', r) for r in
    np.where(
        (efdata_noNA.loc[mask2, 'Méthode_nom_DBh'] == efdata_noNA.loc[mask2, 'Méthode_nom_DBb']) &
        ~(efdata_noNA.loc[mask2, 'Méthode_nom_DBh'].isna() | efdata_noNA.loc[mask2, 'Méthode_nom_DBb'].isna()),
        efdata_noNA.loc[mask2, 'Méthode_nom_DBh'].astype(str),
        efdata_noNA.loc[mask2, 'Méthode_nom_DBh'].astype(str) + '+' + efdata_noNA.loc[mask2, 'Méthode_nom_DBb'].astype(str)
    )]

# Don't use débit biologique de survie, it doesn't translate well to yearly or monthly e-flows

# Translate ecological status
# The notations mean the following:
# TBE: Très bon état - High
# BE: Bon état - Good
# MOY: Etat moyen - Moderate
# MED: Etat médiocre - Poor
# MAUV: Etat mauvais - Bad
# IND: État indéterminé - NA/not determined

efdata_noNA['ecostate'] = efdata_noNA['ecostate'].replace({
    'TBE': 'High',
    'BE': 'Good',
    'MOY': 'Moderate',
    'MED': 'Poor',
    'MAUV': 'Bad',
    'IND': 'Not determined'
})


cols_to_keep =['UID_Mathis', 'Code_masse_deau','Code_station', 'Code_station_commentaire', 'Cours_deau', 'Nom_station',
 'Longitude', 'Latitude', 'Projection', 'Bassin_versant_km2','Débit_minimum_biologique_m3s_DMO',
 'Débit_biologique_valeur_haute_m3s_DBh','Débit_biologique_valeur_basse_m3s_DBb','Débit_de_survie_proposée_m3s_DBs',
 'Document_eflows','Document_pdf_location_eflows','Méthode_type_Débit_minimum_biologique', 'Méthode_nom_Débit_minimum_biologique',
 'Méthode_type_DBh','Méthode_nom_DBh','Méthode_type_DBb','Méthode_nom_DBb','Méthode_type_DBs','Méthode_nom_DBs',
 'Processing_comment','Sub-Basin','ecostate_annee','ecostate','Longitude_original','Latitude_original',
 'eflow_selected','eflow_selected_type','eflow_selected_methodtype','eflow_selected_methodname']

# Write records with eflow values
efdata_noNA.loc[~efdata_noNA['eflow_selected'].isna(),
                [x in cols_to_keep for x in efdata_noNA.columns]].to_csv(
    os.path.join(rhonedir, f"France_Rhone_catchment_preformatted_{datetime.date.today().strftime('%Y%m%d')}.csv"),
    index=False
)


#Delete intermediate products
for path in [efpoints_originalwgs84,
             efpoints_originallambert,
             efpoints_originallambert_ssbvjoin,
             efdata_csv,
             ecostate_last_csv,
             efpts_efdata_jointab_path]:
    arcpy.management.Delete(path)