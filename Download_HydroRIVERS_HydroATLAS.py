import os.path

import arcpy.management

from globalEF_comparison_setup import *
import urllib.request
import zipfile

HydroRIVERS_url = "https://data.hydrosheds.org/file/HydroRIVERS/HydroRIVERS_v10.gdb.zip"
RiverATLAS_url = "https://figshare.com/ndownloader/files/20087321"

zip_path_HydroRIVERS = os.path.join(datdir, 'HydroRIVERS_v10.gdb.zip')
zip_path_RiverATLAS = os.path.join(datdir, 'RiverATLAS_Data_v10.gdb.zip')

if not os.path.exists(zip_path_HydroRIVERS):
    with open(zip_path_HydroRIVERS, "wb") as file:
        # get request
        print(f"Downloading HydroRIVERs")
        response = requests.get(HydroRIVERS_url, verify=False)
        file.write(response.content)
else:
    print(zip_path_HydroRIVERS, "already exists... Skipping download.")

with zipfile.ZipFile(zip_path_HydroRIVERS, 'r') as zip_ref:
    zip_ref.extractall(os.path.dirname(zip_path_HydroRIVERS))

if not os.path.exists(zip_path_RiverATLAS):
    with open(zip_path_RiverATLAS, "wb") as file:
        # get request
        print(f"Downloading HydroRIVERs")
        response = requests.get(RiverATLAS_url, verify=False)
        file.write(response.content)
else:
    print(zip_path_RiverATLAS, "already exists... Skipping download.")

with zipfile.ZipFile(zip_path_RiverATLAS, 'r') as zip_ref:
    zip_ref.extractall(os.path.dirname(zip_path_RiverATLAS))

arcpy.management.CopyRows(
    os.path.join(os.path.splitext(zip_path_RiverATLAS)[0], 'RiverATLAS_v10'),
    out_table = os.path.join(resdir, 'RiverATLAS_v10tab.csv')
)




