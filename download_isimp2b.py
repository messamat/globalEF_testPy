from globalEF_comparison_setup import *

datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')

#hydrological output directory
isimp2b_datdir = Path(datdir, 'isimp2b')
if not isimp2b_datdir.exists():
    isimp2b_datdir.mkdir()

def list_isimp2layer(var):
    #Create list of URLs to get from API
    root_url = "https://files.isimip.org//ISIMIP2b//OutputData//water_global"
    ghm_list = ["H08", "LPJmL", "PCR-GLOBWB", "WaterGAP2-2c"]
    gcm_list = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
    middle_url = f"_ewembi_picontrol_1860soc_co2_{var}_global_daily_"

    years = [f"{i}_{i+9}" for i in range(1661, 1861, 10)]

    isimp2b_pathlist = []
    for ghm in ghm_list:
        for gcm in gcm_list:
                for y in years:
                    isimp2b_pathlist.append(
                        "//".join([
                            root_url, ghm, gcm, "pre-industrial",
                            f"{ghm.lower()}_{gcm.lower()}{middle_url}{y}.nc4"
                        ])
                    )

    return(isimp2b_pathlist)



#Download nc files (need to improve exception handling)

#For discharge
isimp2b_pathlist_dis = list_isimp2layer(var='dis')
for url in isimp2b_pathlist_dis:
    outnc = Path(isimp2b_datdir, Path(url).name)
    if not outnc.exists():
        with open(outnc, "wb") as file:
            # get request
            print(f"Downloading {Path(url).name}")
            response = requests.get(url)
            file.write(response.content)


#For runoff
isimp2b_pathlist_qtot = list_isimp2layer(var='qtot')
for url in isimp2b_pathlist_qtot:
    outnc = Path(isimp2b_datdir, Path(url).name)
    if not outnc.exists():
        with open(outnc, "wb") as file:
            # get request
            print(f"Downloading {Path(url).name}")
            response = requests.get(url, verify=False)
            file.write(response.content)



