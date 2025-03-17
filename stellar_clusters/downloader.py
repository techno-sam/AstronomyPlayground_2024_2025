__all__ = ['download_data', 'DATA_COLUMNS']

import multiprocessing as mp
import typing
import sys
import time
import json

import requests
from urllib.parse import quote as urlencode


def mast_query(request):
    """Perform a MAST query.

        Parameters
        ----------
        request (dictionary): The MAST request json object

        Returns head,content where head is the response HTTP headers, and content is the returned data"""

    # Base API url
    request_url = "https://mast.stsci.edu/api/v0/invoke"

    # Grab Python Version
    version = ".".join(map(str, sys.version_info[:3]))

    # Create Http Header Variables
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain",
               "User-agent": "python-requests/" + version}

    # Encoding the request as a json string
    req_string = json.dumps(request)
    req_string = urlencode(req_string)

    # Perform the HTTP request
    resp = requests.post(request_url, data="request=" + req_string, headers=headers)

    # Pull out the headers and response content
    head = resp.headers
    content = resp.content.decode("utf-8")

    return head, content


def set_filters(parameters):
    return [{"paramName":p, "values":v} for p,v in parameters.items()]


def set_min_max(min, max):
    return [{"min": min, "max": max}]


def name_resolver(name: str) -> list[dict[str, ...]]:
    resolver_request = {
        "service": "Mast.Name.Lookup",
        "params": {
            "input": name,
            "format": "json"
        },
    }

    headers, resolved_object_string = mast_query(resolver_request)
    resolved_object = json.loads(resolved_object_string)
    return resolved_object["resolvedCoordinate"]


def cone(name: str, ra: float, dec: float, radius: float, page: int = 1, completion_count: typing.Optional[mp.Value] = None) -> dict[str, ...]:
    cone_request = {
        "service": " Mast.Catalogs.GaiaDR2.Cone",
        "params": {
            "ra": ra,
            "dec": dec,
            "radius": radius,
            "input": name
        },
        "format": "json",
        "pagesize": 5000,
        "page": page,
        "removenullcolumns": False,
        "removenullrows": False,
        "removecache": False,
        "columnsconfigid": "Mast.Catalogs.Gaia.Cone"
    }

    headers, mast_data_string = mast_query(cone_request)
    mast_data = json.loads(mast_data_string)

    if completion_count is not None:
        completion_count.value += 1

    return mast_data


def flatten(xss):
    return [x for xs in xss for x in xs]


FILTER_COLUMNS = {'visibility_periods_used', 'astrometric_excess_noise', 'parallax_over_error', 'phot_g_mean_flux_over_error', 'phot_bp_mean_flux_over_error', 'phot_rp_mean_flux_over_error'}
DATA_COLUMNS = {'source_id', 'phot_g_mean_flux', 'phot_g_mean_mag', 'bp_rp', 'bp_g', 'g_rp'}
RELEVANT_COLUMNS = FILTER_COLUMNS | DATA_COLUMNS


def filter_row(row: dict[str, ...]) -> bool:
    if any(row[k] is None for k in RELEVANT_COLUMNS):
        return False
    return row['visibility_periods_used'] >= 9 and \
        row['astrometric_excess_noise'] < 1 and \
        row['parallax_over_error'] > 10 and \
        row['phot_g_mean_flux_over_error'] > 50 and \
        row['phot_bp_mean_flux_over_error'] > 20 and \
        row['phot_rp_mean_flux_over_error'] > 20


def cone_data(*args, **kwargs):
    return cone(*args, **kwargs)['data']


def download_data(name: str) -> list[dict[str, ...]]:
    print(f"Starting download for {name}")
    print(f"> Resolving name")
    resolved = name_resolver(name)[0]
    ra = resolved['ra']
    dec = resolved['decl']
    radius = resolved['radius']

    all_data: list[dict[str, ...]] = []

    print(f"> Doing first-page cone search for {ra=} {dec=} {radius=}")
    page1 = cone(name, ra, dec, radius)
    all_data.extend(page1['data'])

    page_count = page1['paging']['pagesFiltered']
    print(f"> Fetching remaining pages")

    page_assignments = list(range(2, page_count+1))

    manager = mp.Manager()
    completed_count = manager.Value('i', 1)

    def update_progress():
        while completed_count.value < len(page_assignments)+1:
            print(f"\r> Fetched {completed_count.value}/{page_count} pages", end="")
            time.sleep(1)

    status_updater = mp.Process(target=update_progress)
    status_updater.start()

    with mp.Pool(16) as pool:
        all_data.extend(flatten(pool.starmap(
            cone_data,
            [
                (name, ra, dec, radius, page, completed_count)
                for page in page_assignments
            ]
        )))

    status_updater.terminate()
    print(f"\r> All pages fetched for {name}")

    relevant_data = [
        {k: row[k] for k in RELEVANT_COLUMNS}
        for row in all_data
    ]
    del all_data

    relevant_data = [*filter(filter_row, relevant_data)]
    relevant_data = [
        {k: row[k] for k in DATA_COLUMNS}
        for row in relevant_data
    ]

    print(f"> Fetched {len(relevant_data)} filtered rows of data\n")
    return relevant_data
