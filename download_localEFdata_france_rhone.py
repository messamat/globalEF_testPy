from globalEF_comparison_setup import *
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

outdir = os.path.join(datdir, 'GEFIS_test_data', 'Data by country', 'Europe', 'France', 'Rhone')

studies_page = "https://www.rhone-mediterranee.eaufrance.fr/gestion-de-leaugestion-quantitative-de-la-ressource-en-eauetudes-volumes-prelevables/etudes-0"
response = requests.get(studies_page, verify=False)
studies_html = BeautifulSoup(response.content, "html.parser")

rapports_elems = [li.find_all('a') for li in studies_html.find_all('li')]
rapports_elems_id = [(a[0].get('data-nodeid')) is not None for a in rapports_elems if len(a) > 0]
studies_page_host = requests.utils.urlparse(studies_page)._replace(path='').geturl()
region_pages = [urljoin(studies_page_host, a[0]['href']) for a, id in zip(rapports_elems, rapports_elems_id)
                if id and len(a) > 0]

def process_rhone_eflow_page(url, outdir, verbose=True):
    if verbose:
        print(url)

    response = requests.get(url, verify=False)
    encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
    parser = 'html.parser'  # or lxml or html5lib
    study_page = BeautifulSoup(response.content, parser, from_encoding=encoding)

    # extract the text content of the HTML element with class 'title'
    title_element = study_page.select_one('title')
    title_text = title_element.text.strip()

    title = re.sub("\r?\n|\r|[|]", '',
                   str(study_page.select_one('title').text).
                   replace('/', '-')).\
        strip()

    url_host = urlparse(url)._replace(path='').geturl()

    pdfs_url = [urljoin(url_host, re.findall('.*[.]pdf$', a['href'])[0])
                for a in study_page.find_all('a') if re.match('.*[.]pdf$', a['href'])]

    outdir_full = os.path.join(outdir, title)
    os.makedirs(outdir_full, exist_ok=True)

    for in_url in pdfs_url:
        out_pdf = os.path.join(outdir_full, in_url.rsplit('/', 1)[-1])
        if not os.path.exists(out_pdf):
            print(f"Downloading {out_pdf}")
            with open(out_pdf, 'wb') as f:
                f.write(requests.get(in_url, verify=False).content)
        else:
            print(f"{out_pdf} was already downloaded.")

    return {'title': title, 'pdfs': pdfs_url}

for p in region_pages:
    process_rhone_eflow_page(url=p, outdir=outdir)

############### Download shapefile of Masses d'eau ##############################
sdage_masses_url = 'https://www.rhone-mediterranee.eaufrance.fr/sites/sierm/files/content/2019-07/RefSdage2016_mdoriv_FRD.zip'
sdage_masses_zip = Path(outdir, os.path.basename(sdage_masses_url))
if not os.path.exists(sdage_masses_zip):
    with open(sdage_masses_zip, "wb") as file:
        # get request
        print(f"Downloading {Path(sdage_masses_url).name}")
        response = requests.get(sdage_masses_url, verify=False)
        file.write(response.content)
else:
    print(sdage_masses_zip, "already exists... Skipping download.")

with zipfile.ZipFile(sdage_masses_zip, 'r') as zip_ref:
    zip_ref.extractall(os.path.dirname(sdage_masses_zip))

############### Download shapefile of Masses d'eau ##############################
sdage_subbasins_url = 'https://www.rhone-mediterranee.eaufrance.fr/sites/sierm/files/content/2019-07/RefSdage2016_ssbv_FRD.zip'
sdage_basins_zip = os.path.join(outdir, os.path.basename(sdage_subbasins_url))

if not os.path.exists(sdage_basins_zip):
    with open(sdage_basins_zip, "wb") as file:
        # get request
        print(f"Downloading {Path(sdage_subbasins_url).name}")
        response = requests.get(sdage_subbasins_url, verify=False)
        file.write(response.content)
else:
    print(sdage_masses_zip, "already exists... Skipping download.")

with zipfile.ZipFile(sdage_basins_zip, 'r') as zip_ref:
    zip_ref.extractall(os.path.dirname(sdage_basins_zip))

############### Get etat des cours d'eau ##############################
etat_cours_deau_db = "https://www.rhone-mediterranee.eaufrance.fr/surveillance-des-eaux/qualite-des-cours-deau/donnees-detat-des-cours-deau-superficiels?search_api_fulltext=&field_sq_region=All&field_sq_departement=All&field_sq_commune=All&field_sq_watershed=All&field_sq_subwatershed=All&field_sq_stream=All&items_per_page=90&view_mode=medium&page=1"
#Click on "T?l?charger le fichier des donn?es 2022/06/07