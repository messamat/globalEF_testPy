import arcpy.management
import pandas as pd
import xarray as xr

from globalEF_comparison_setup import *

#Input variables
process_gdb = os.path.join(resdir, 'processing_outputs.gdb')
EFpoints_0308_clean = os.path.join(process_gdb, 'EFpoints_20230308_clean')

#Input variables
isimp2b_globalef_runoff_resdir = Path(resdir, 'isimp2_qtot_accumulated15s.gdb')
globwb_globalef_runoff_resdir = Path(resdir, 'globwb_qtot_accumulated15s.gdb')


for path in getfilelist(isimp2b_globalef_runoff_resdir, 'taef'):
    print(re.sub('taef', 'maef', path))
    arcpy.management.Rename(path, re.sub('taef', 'maef', path))