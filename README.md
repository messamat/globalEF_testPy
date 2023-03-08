## Python code for: "Comparing global e-flow estimation methods to local e-flow assessments"

**Prerequisites**: All GIS analyses in this study require an ESRI ArcGIS license including the Spatial Analyst extension, 
which itself requires a Windows OS. We used the Python Arcpy module associated with ArcGIS 10.7 in Python 2.7 with 
64-bit background processing.

This repository includes the portions of the analysis conducted in Python, which encompass all spatial formatting of the
data prior to data analysis. 


In Python:
1. Download isimp2b #DONE
2. Pre-process isimp2b #Ran it for discharge. Need to run it for runoff now.
3. Pre-process PCR-GLOBWB 2.0 #DONE

#For Rhone sites
3. download_france_rhone.R #DONE
4. manual processing/data extraction #DONE

#For Mexico: 
5. download_format_Mexico.R -> 'mexico_refdata_preformatted.csv' #DONE

6. format_points.py #DONE

7. process_france_rhone.R #DONE

6. downscale_efmodels based on downscale_GEFIS - ########## TO WRITE ######


Then execute the following code in R: 
8. src/GEFIS_testR/postQAQC_merge_dbversions.R TO RE-RUN

9. In master database, add Mexico


And continue with the following workflow: TO RE-RUN
10. link_sites_GEFIS.py TO ADAPT AND RE-RUN
11. link_sites_GRDC.py TO ADAPT AND RE-RUN
12. link_sites_RiverATLAS.py TO ADAPT AND RE-RUN
13. quantify_mask.py

Note that substantial manual quality-checking and editing was performed on the location of the e-flow assessment sites, 
a process that cannot be programatically reproduced.