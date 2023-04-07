import arcpy
from arcpy.sa import *
from collections import defaultdict
from datetime import date
import math
import numpy as np
import os
import pandas as pd
from pathlib import Path
import re
import requests
import traceback
import sys
from inspect import getsourcefile
import zipfile

arcpy.CheckOutExtension('Spatial')
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

#Get current root directory
def get_root_fromsrcdir():
    return(os.path.dirname(os.path.abspath(
        getsourcefile(lambda:0)))).split('\\src')[0]

#Folder structure
rootdir = get_root_fromsrcdir()
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')

#Utility functions
#Get all files in a ArcGIS workspace (file or personal GDB)
def getwkspfiles(dir, repattern=None):
    arcpy.env.workspace = dir
    filenames_list = (arcpy.ListDatasets() or []) + (arcpy.ListTables() or [])  # Either LisDatsets or ListTables may return None so need to create empty list alternative
    if not repattern == None:
        filenames_list = [os.path.join(dir, filen)
                          for filen in filenames_list if re.search(repattern, filen)]
    return (filenames_list)
    arcpy.ClearEnvironment('workspace')

def getfilelist(dir, repattern=None, gdbf=True, nongdbf=True):
    """Function to iteratively go through all subdirectories inside 'dir' path
    and retrieve path for each file that matches "repattern"
    gdbf and nongdbf allows the user to choose whether to consider ArcGIS workspaces (GDBs) or not or exclusively"""

    try:
        if isinstance(dir, Path):
            dir = str(dir)

        if arcpy.Describe(dir).dataType == 'Workspace':
            if gdbf == True:
                print('{} is ArcGIS workspace...'.format(dir))
                filenames_list = getwkspfiles(dir, repattern)
            else:
                raise ValueError(
                    "A gdb workspace was given for dir but gdbf=False... either change dir or set gdbf to True")
        else:
            filenames_list = []

            if gdbf == True:
                for (dirpath, dirnames, filenames) in os.walk(dir):
                    for in_dir in dirnames:
                        fpath = os.path.join(dirpath, in_dir)
                        if arcpy.Describe(fpath).dataType == 'Workspace':
                            print('{} is ArcGIS workspace...'.format(fpath))
                            filenames_list.extend(getwkspfiles(dir=fpath, repattern=repattern))

            if nongdbf == True:
                for (dirpath, dirnames, filenames) in os.walk(dir):
                    for file in filenames:
                        if repattern is None:
                            filenames_list.append(os.path.join(dirpath, file))
                        else:
                            if re.search(repattern, file):
                                filenames_list.append(os.path.join(dirpath, file))
        return (filenames_list)

    # Return geoprocessing specific errors
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    # Return any other type of error
    except:
        # By default any other errors will be caught here
        e = sys.exc_info()[1]
        print(e.args[0])

def pathcheckcreate(path, verbose=True):
    """"Function that takes a path as input and:
      1. Checks which directories and .gdb exist in the path
      2. Creates the ones that don't exist"""

    dirtocreate = []
    # Loop upstream through path to check which directories exist, adding those that don't exist to dirtocreate list
    while not os.path.exists(os.path.join(path)):
        dirtocreate.append(os.path.split(path)[1])
        path = os.path.split(path)[0]

    dirtocreate.reverse()

    # After reversing list, iterate through directories to create starting with the most upstream one
    for dir in dirtocreate:
        # If gdb doesn't exist yet, use arcpy method to create it and then stop the loop to prevent from trying to create anything inside it
        if os.path.splitext(dir)[1] == '.gdb':
            if verbose:
                print('Create {}...'.format(dir))
            arcpy.management.CreateFileGDB(out_folder_path=path,
                                           out_name=dir)
            break

        # Otherwise, if it is a directory name (no extension), make a new directory
        elif os.path.splitext(dir)[1] == '':
            if verbose:
                print('Create {}...'.format(dir))
            path = os.path.join(path, dir)
            os.mkdir(path)


def rasname(path):
    return(os.path.splitext(os.path.split(path)[1])[0])

#Compare whether two layers' spatial references are the same
def compsr(lyr1, lyr2):
    return(arcpy.Describe(lyr1).SpatialReference.exportToString() ==
           arcpy.Describe(lyr2).SpatialReference.exportToString())

# Resample a dictionary of rasters (in_vardict) to the resolution of a template raster (in_hydrotemplate), outputting
# the resampled rasters to paths contained in another dictionary (out_vardict) by keys
#See resample tool for resampling_type options (BILINEAR, CUBIC, NEAREST, MAJORITY)
def hydroresample(in_vardict, out_vardict, in_hydrotemplate, resampling_type='NEAREST'):
    templatedesc = arcpy.Describe(in_hydrotemplate)

    # Check that all in_vardict keys are in out_vardict (that each input path has a matching output path)
    keymatch = {l: l in out_vardict for l in in_vardict}
    if not all(keymatch.values()):
        raise ValueError('All keys in in_vardict are not in out_vardict: {}'.format(
            [l for l in keymatch if not keymatch[l]]))

    # Iterate through input rasters
    for var in in_vardict:
        outresample = out_vardict[var]

        if not arcpy.Exists(outresample):
            print('Processing {}...'.format(outresample))
            arcpy.env.extent = arcpy.env.snapRaster = in_hydrotemplate
            arcpy.env.XYResolution = "0.0000000000000001 degrees"
            arcpy.env.cellSize = templatedesc.meanCellWidth
            print('%.17f' % float(arcpy.env.cellSize))

            try:
                arcpy.management.Resample(in_raster=in_vardict[var],
                                          out_raster=outresample,
                                          cell_size=templatedesc.meanCellWidth,
                                          resampling_type=resampling_type)
            except Exception:
                print("Exception in user code:")
                traceback.print_exc(file=sys.stdout)
                arcpy.ResetEnvironments()

        else:
            print('{} already exists...'.format(outresample))

        # Check whether everything is the same
        maskdesc = arcpy.Describe(outresample)

        extentcomp = maskdesc.extent.JSON == templatedesc.extent.JSON
        print('Equal extents? {}'.format(extentcomp))
        if not extentcomp: print("{0} != {1}".format(maskdesc.extent, templatedesc.extent))

        cscomp = maskdesc.meanCellWidth == templatedesc.meanCellWidth
        print('Equal cell size? {}'.format(cscomp))
        if not cscomp: print("{0} != {1}".format(maskdesc.meanCellWidth, templatedesc.meanCellWidth))

        srcomp = compsr(outresample, in_hydrotemplate)
        print('Same Spatial Reference? {}'.format(srcomp))
        if not srcomp: print("{0} != {1}".format(maskdesc.SpatialReference.name, templatedesc.SpatialReference.name))

    arcpy.ResetEnvironments()

#Use xarray so as not to have to create temporary raster layers iteratively for each netdf
def extract_xr_by_point(in_xr, in_pointdf, in_df_id,
                        in_xr_lon_dimname='lon', in_xr_lat_dimname='lat',
                        in_df_lon_dimname='POINT_X', in_df_lat_dimname='POINT_Y'
                        ):
    df_asxr = in_pointdf.set_index(in_df_id).to_xarray()

    isel_dict = {in_xr_lon_dimname: df_asxr[in_df_lon_dimname],
                 in_xr_lat_dimname: df_asxr[in_df_lat_dimname],}
    pixel_values = in_xr.sel(isel_dict , method="nearest")
    pixel_values_df = pixel_values.reset_coords(drop=True). \
        to_dataframe(). \
        reset_index()
    return(pixel_values_df)
