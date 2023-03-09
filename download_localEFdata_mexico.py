import os.path

from globalEF_comparison_setup import *
import urllib.request
import zipfile

# Original source of data for officially-determined eflows:
# https://www.gob.mx/conagua/documentos/programa-nacional-hidrico-pnh-2020-2024
outdir = os.path.join(datdir, 'GEFIS_test_data', 'Data by country', 'Mexico')

def download_mexico(outdir):
    onlinedat = "https://www.mdpi.com/2071-1050/13/3/1240/s1"
    zip_path = os.path.join(outdir, 'sustainability-1066121-supplementary.zip')
    if not os.path.exists(zip_path):
        with open(zip_path, "wb") as file:
            # get request
            print(f"Downloading {Path(onlinedat).name}")
            response = requests.get(onlinedat, verify=False)
            file.write(response.content)
    else:
        print(zip_path, "already exists... Skipping download.")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(zip_path))

    onlinebasins = "http://201.116.60.46/DatosAbiertos/Cuencas_hidrolOgicas_que_cuentan_con_reserva.zip"
    zip_path_basins = os.path.join(outdir, os.path.basename(onlinebasins))
    if not os.path.exists(zip_path_basins):
        with open(zip_path_basins, "wb") as file:
            # get request
            print(f"Downloading {Path(onlinebasins).name}")
            response = requests.get(onlinebasins, verify=False)
            file.write(response.content)
    else:
        print(zip_path_basins, "already exists... Skipping download.")

    with zipfile.ZipFile(zip_path_basins, 'r') as zip_ref_basins:
        zip_ref_basins.extractall(os.path.dirname(zip_path_basins))

    return {'eflows_list': os.path.join(os.path.splitext(zip_path)[0],
                                        'sustainability-1066121-supplementary.xlsx'),
            'eflows_basins': getfilelist(os.path.splitext(zip_path_basins)[0], "[.]shp$")}

mexico_dat = download_mexico(outdir)

# Formatting of the database from the original format
mexico_refdata_raw = pd.read_excel(mexico_dat['eflows_list'],
                                   sheet_name='%_EWR_met',
                                   usecols='A:S')

mexico_refdata_raw[mexico_refdata_raw.select_dtypes("object").columns] = \
    mexico_refdata_raw[mexico_refdata_raw.select_dtypes("object").columns].apply(
    lambda x: x.str.strip(),
    axis=1)

# Get flow status
mexico_refdata_ApendD2Direct = pd.read_excel(mexico_dat['eflows_list'],
                                             skiprows=10,
                                             sheet_name='NMX_ApendD2_Direct',
                                             usecols='A:G'). \
    rename(columns={'Streamflow_type_(FDC_daily)': 'Naturally_nonperennial_mentioned'}). \
    assign(method_streamflowtype='daily')

mexico_refdata_ApendD2Indirect = pd.read_excel(mexico_dat['eflows_list'],
                                               sheet_name='NMX_ApendD2_Indirect',
                                               skiprows=10,
                                               usecols='A:G'
                                               ). \
    rename(columns={'Streamflow_type_(FDC_monthly)': 'Naturally_nonperennial_mentioned'}). \
    assign(method_streamflowtype='monthly')

# Merging two data tables and selecting required columns
mexico_flowstatus = pd.concat([
    mexico_refdata_ApendD2Direct,
    mexico_refdata_ApendD2Indirect],
    ignore_index=True, sort=False).loc[
    lambda x: ~x['ID_R2018'].isna(),
    ['ID_R2018', 'Naturally_nonperennial_mentioned', 'method']
].\
    sort_values('method').\
    drop_duplicates(subset=['ID_R2018'])

# For mat dataset
mexico_refdata_formatted = mexico_refdata_raw[~mexico_refdata_raw['Method'].isna()].assign(
    Country='Mexico',
    Scale='Basin',
    River=np.nan,
    E_flows_Assessment_Report_Av_13=np.nan,
    Report_Content_as_Summary_Da_14="Summary",
    Link_Attachment_for_E_flows__15="https://doi.org/10.3390/su13031240",
    Raw_Data_Available_on_Reques_16="Yes",
    E_flow_Method_Model_Type=np.nan,
    eftype_ref=np.nan,
    efname_ref=np.nan,
    ecname_ref="Ecological objective (Objetivos ambientales, NORMA MEXICANA NMX-AA-159-SCFI-2012)",
    Ecological_condition_comment="See NMX-AA-159-SCFI-2012 8/118 and 19/118.",
    ecpresent_ref=np.nan,
    hydrotype_ref="To be completed",
    mar_unit="hm3/yr",
    National_Legislation_for_E_f_39="Yes",
    Name_s__of_Laws="Ley de Aguas Nacionales",
    Supporting_Regulation_s__Exi_41="Yes",
    Name_s__of_Regulations="NORMA MEXICANA NMX-AA-159-SCFI-2012 QUE ESTABLECE EL PROCEDIMIENTO PARA LA DETERMINACIÓN DEL CAUDAL ECOLÓGICO EN CUENCAS HIDROLÓGICAS",
    Sources_of_Additional_Inform_43="ssalinas@ecosur.mx"
).merge(mexico_flowstatus, on='ID_R2018')

# Change column names
mexico_colnamedt = pd.DataFrame({'old': ['Hydro_Region_name', 'River basin', 'ID_R2018', 'MAR_hm3/yr', 'Env_Obj_NMX2016', 'Ewr_Est_hm3/yr', 'EWR1_%MAR'],
                                 'new': ['Basin', 'Sub_Basin', 'E_flow_Location_Name_No_', 'mar_ref', 'ecfuture_ref', 'efvol_ref', 'efper_ref']})
mexico_refdata_formatted = mexico_refdata_formatted.rename(columns=dict(zip(mexico_colnamedt.old, mexico_colnamedt.new)))

#Move ID column to the front
first_col = mexico_refdata_formatted.pop('E_flow_Location_Name_No_')
mexico_refdata_formatted.insert(0, 'E_flow_Location_Name_No_', first_col)

# Write to file
mexico_refdata_formatted.to_csv(os.path.join(resdir, 'mexico_refdata_preformatted.csv'),
                                encoding='utf-8-sig', index=False)
