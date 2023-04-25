## Python code for: "Comparing global e-flow estimation methods to local e-flow assessments"

**Prerequisites**: All GIS analyses in this study require an ESRI ArcGIS license including the Spatial Analyst extension, 
which itself requires a Windows OS. We used the Python Arcpy module associated with ArcGIS 10.7 in Python 2.7 with 
64-bit background processing.

This repository includes the portions of the analysis conducted in Python, which encompass all spatial formatting of the
data prior to data analysis. 

#Format hydrological models and EF calculations
1. download_isimp2b #DONE
2. preprocess_isimp2b #DONE
3. preprocess PCR-GLOBWB 2.0 #DONE
4. downscale_EFs.py #DONE

#Format local EF data
5. download_localEFdata_france_rhone. #DONE
6. download_localEFdata_Mexico #DONE
7. preprocess_localEFdata_france_rhone
8. format_points.py #DONE

#Link everything 
9. link_sites_globalEF.py #DONE 
10. link_sites_RiverATLAS.py #DONE

#To do in the future
11. link_sites_GRDC.py TO ADAPT AND RE-RUN

Note that substantial manual quality-checking and editing was performed on the location of the e-flow assessment sites, 
a process that cannot be programatically reproduced.