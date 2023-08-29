## Python code for 'Global and local estimates of environmental flow requirements to sustain river ecosystems are poorly correlated'

This repository contains python code associated with _Messager, M. L., Dickens, W. S. C., Eriyagama, N., Tharme, R. E., Stassen, R. (In review). Global and local estimates of environmental flow requirements to sustain river ecosystems are poorly correlated. [to be updated]_

## Abstract
Environmental flows (e-flows) are a central element of sustainable water resource management to mitigate the impacts of 
hydrological alteration on freshwater ecosystems and their benefits to people. Many nations protect e-flows, and thousands
of e-flow assessments have been conducted globally, leveraging local data and knowledge to quantify how much water must be
kept instream for sustaining healthy ecosystems. However, e-flow assessments and implementation are geographically uneven 
and cover a small fraction of rivers worldwide, which hinders globally consistent target-setting, monitoring and evaluation 
for international agreements to curb water scarcity and biodiversity loss like the UN Sustainable Development Goals. Therefore, 
global models have been developed to estimate the e-flow requirements of global rivers seamlessly across basins and administrative 
boundaries. But there has been little effort to benchmark these models against locally derived e-flow estimates, which may 
limit confidence in the relevance of global targets. The aim of this study was thus to assess whether current global methods 
reflect e-flow estimates used on the ground by comparing global and local estimates for 1194 sites across 25 countries. 
While global approaches can broadly approximate the bulk amount of water that should be precautionarily set aside to 
sustain aquatic ecosystems at the scale of large basins or countries, they explain a negligible 0-1% of the variability 
in locally derived estimates of the percentage of river flow that must be protected at a given site. Such a disconnect
between global and local assessments of e-flow requirements limits the credibility of current global e-flow estimates and 
associated targets for human and ecosystem water use. To accelerate the global implementation of e-flows requires a concerted
effort to compile and draw from the thousands of local e-flow assessments that have been conducted worldwide to bridge the 
gap from local to global scales.

## Introduction

This repository includes the portions of the analysis conducted in Python, which encompass all spatial formatting of the
data prior to data analysis. This analysis workflow needs to be conducted prior to conducting data analysis in R with code in the 
following repository: https://github.com/messamat/globalEF_testR. 

These scripts are annotated but could be challenging to follow. If you encounter any trouble, please don't hesitate
to contact Mathis L. Messager for comments and clarifications by email or to log an issue in github.

Files needed to run this analysis are available by downloading the study's figshare permanent repository. 
The /data folder in the figshare repository contains raw data and the directory structure enables users to reproduce our 
study using the scripts herein.

### Prerequisites
All GIS analyses in this study require an ESRI ArcGIS Pro license including the Spatial Analyst extension, 
which itself requires a Windows OS. We used the Python Arcpy module associated with ArcGIS Pro 3.0 in Python 3.9.

## Workflow
### Utility codes
- [global_EF_comparison_setup.py](https://github.com/messamat/globalEF_testPy/blob/master/globalEF_comparison_setup.py) : import libraries, define folder structure, basic utility functions
- [EF_utils.py](https://github.com/messamat/globalEF_testPy/blob/master/EF_utils.py): functions to pre-process and compute e-flows from netCDF files (ith the xarray module)

### Download and format hydrological data, then compute global e-flow estimates
- [01_download_isimp2b.py](https://github.com/messamat/globalEF_testPy/blob/master/01_download_isimp2b.py): download discharge and total runoff data from the Inter-Sectoral Impact Model Intercomparison Project (ISIMIP) simulation round 2b for 16 combinations of Global Circulation Models (GCMs) and GLobal Hydrological Models (GHMs)
- [02_preprocess_isimp2b.py](https://github.com/messamat/globalEF_testPy/blob/master/02_preprocess_isimp2b.py): aggregate GHM outputs from ISIMIP 2b from daily to monthly time series, then compute e-flow estimates from ISIMIP 2b hydrological data with all global methods (Tennant, Tessman, Variable Monthly Flow, Q90_Q50, Smakhtin's flow duration curve shift)
- [03_preprocess PCR-GLOBWB.py](https://github.com/messamat/globalEF_testPy/blob/master/03_preprocess_PCR-GLOBWB.py): compute e-flows for higher-resolution version of one of the global hydrological models used in the main analysis — PCR-GLOBWB 2.0 at a spatial resolution of 5 arc-min, equating to 9 km at the equator, as implemented in Li et al (2019). Obtained data from personal communication with Dr. ir. Edwin H. Sutanudjaja.
- [04_downscale_EFs.py](https://github.com/messamat/globalEF_testPy/blob/master/04_downscale_EFs.py): downscale all global mean annual flow (MAF) and e-flow estimates based on runoff to yield estimates in terms of discharge at 0.25 arc-min (approximately 500 m at the equator). Estimates in terms of runoff (in m3 s-1 m-2) were first resampled to 0.25 arc-min pixels and then routed along the river network to simulate the flow of water across the landscape and produce estimates in terms of discharge (in m3 s-1). This river network routing was performed by simple area-weighted flow accumulation using the HydroSHEDS drainage direction grids at a resolution of 0.25 arc-min network (Lehner et al 2008, Lehner and Grill 2013). 

### Format local EF data
Note that substantial manual quality-checking and editing was performed on the location of the e-flow assessment sites, 
a process that cannot be programatically reproduced.
- [05_download_localEFdata_france_rhone.py](https://github.com/messamat/globalEF_testPy/blob/master/05_download_localEFdata_france_rhone.py): download e-flow assessment reports for the Rhone River Basin in France
- [06_download_localEFdata_Mexico.py](https://github.com/messamat/globalEF_testPy/blob/master/06_download_localEFdata_Mexico.py): download supplementary information from [Salinas-Rodriguez et al. 2021](https://doi.org/10.3390/su13031240) for Mexico and river basin polygons from the Open Data portal of the Comisión Nacional del Agua. 
- [07_preprocess_localEFdata_france_rhone.py](https://github.com/messamat/globalEF_testPy/blob/master/07_preprocess_localEFdata_france_rhone.py): pre-format extracted data from e-flow assessment reports for the Rhone River Basin, merge these data to ecological condition data from the European Water Framework Directive.
- [08_format_points.py](https://github.com/messamat/globalEF_testPy/blob/master/08_format_points.py): quality-check data for all e-flow sites and co-register them to the HydroRIVERS digital river network.

### Format sites to global e-flow estimates and other hydro-environmental variables
- [09_link_sites_globalEF.py](https://github.com/messamat/globalEF_testPy/blob/master/09_link_sites_globalEF.py): extract global estimates of MAF and e-flows for all sites
- [10_link_sites_RiverATLAS.py](https://github.com/messamat/globalEF_testPy/blob/master/10_link_sites_RiverATLAS.py): extract hydro-environmental variables from the RiverATLAS database for all sites

### Post-process global model data for mapping
- [11_post_process_isimip2b_](https://github.com/messamat/globalEF_testPy/blob/master/10_link_sites_RiverATLAS.py): extract global estimates of e-flows for all river reaches in the HydroRIVERs database for mapping