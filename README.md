## Python code for: "Comparing global e-flow estimation methods to local e-flow assessments"

**Prerequisites**: All GIS analyses in this study require an ESRI ArcGIS license including the Spatial Analyst extension, 
which itself requires a Windows OS. We used the Python Arcpy module associated with ArcGIS 10.7 in Python 2.7 with 
64-bit background processing.

This repository includes the portions of the analysis conducted in Python, which encompass all spatial formatting of the
data prior to data analysis. 


In Python:
1. Download isimp2b
2. Pre-process isimp2b

#For Rhone sites
download_france_rhone.R
manual processing/data extraction
format_points.py
process_france_rhone.R

#For Mexico: download_format_Mexico.R -> 'mexico_refdata_preformatted.csv'



3. format_points.py - merge all databases
4. downscale_efmodels based on downscale_GEFIS - TO WRITE

TO RE-RUN
Then execute the following code in R: src/GEFIS_testR/postQAQC_merge_dbversions.R

And continue with the following workflow: TO RE-RUN
3. link_sites_GEFIS.py
4. link_sites_GRDC.py
5. link_sites_RiverATLAS.py
6. quantify_mask.py

Note that substantial manual quality-checking and editing was performed on the location of the e-flow assessment sites, 
a process that cannot be programatically reproduced.