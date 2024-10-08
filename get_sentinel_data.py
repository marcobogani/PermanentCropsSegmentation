import os.path
import re
import requests
import json

from collections import defaultdict
from tqdm import tqdm

download_dir = os.path.expanduser("~/datasets-nas/permanent_crops/tiles")
conf_dir = "conf/config.json"
tile_id = "T33SVB"
relative_orbit_number = "R079"
start_date = "2017-06-01T00:00:00.000Z"
end_date = "2018-06-01T00:00:00.000Z"
product_level = "MSIL2A"

ACCESS_TOKEN = None


def fetch_products(url, params):
    products_by_date = defaultdict(list)
    baseline_pattern = r'_N(\d{4})_'

    while url:
        response = requests.get(url, params=params if url == base_url else None)

        if response.status_code == 200:
            data = response.json()

            for product in data['value']:
                name = product['Name']
                date = product['ContentDate']['Start'][:10]

                match = re.search(baseline_pattern, name)

                if match:
                    baseline_number = int(match.group(1))
                    products_by_date[date].append((baseline_number, product))

            url = data.get('@odata.nextLink', None)
        else:
            print(f"Request error: {response.status_code}")
            break

    product_list = list()
    for date, products in products_by_date.items():
        # Keep the product with most recent Processing Baseline number
        max_baseline_product = max(products, key=lambda x: x[0])[1]
        product_list.append(max_baseline_product)
        print(f"Date: {date}")
        print(f"Product Name: {max_baseline_product['Name']}")
        print(f"Product ID: {max_baseline_product['Id']}")
        print(f"Download URL: {max_baseline_product['S3Path']}")
        print("-" * 50)

    return product_list


def load_credentials(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config['username'], config['password']


def get_access_token(username, password):
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'client_id': 'cdse-public',
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        token_info = response.json()
        access_token = token_info.get('access_token')
        refresh_token = token_info.get('refresh_token')
        return access_token, refresh_token
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None


def regenerate_access_token(refresh_token):
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'grant_type': 'refresh_token',
        'refesh_token': refresh_token,
        'client_id': 'cdse-public',
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        token_info = response.json()
        print(token_info)
        access_token = token_info.get('access_token')
        refresh_token = token_info.get('refresh_token')
        return access_token, refresh_token
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None


def download_product(product, access_token):
    product_id = product['Id']
    product_name = product['Name']
    file_name = product_name.split('.')[0] + ".zip"
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    headers = {"Authorization": f"Bearer {access_token}"}

    session = requests.Session()
    session.headers.update(headers)

    response = session.get(url, stream=True)

    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
            
        with open(os.path.join(download_dir, file_name), "wb") as file:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {product_name}") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        pbar.update(len(chunk))
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        print(response.text)


base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
params = {
    "$filter": f"Collection/Name eq 'SENTINEL-2' and "
               f"contains(Name, '{tile_id}') and "
               f"contains(Name, '{relative_orbit_number}') and "
               f"contains(Name, '{product_level}') and "
               f"ContentDate/Start gt {start_date} and "
               f"ContentDate/Start lt {end_date}",
    "$orderby": "ContentDate/Start asc"
}
print("Fetching products...")
products = fetch_products(base_url, params)
print(f"Total products: {len(products)}")

if not ACCESS_TOKEN:
    username, password = load_credentials(conf_dir)
    ACCESS_TOKEN, _ = get_access_token(username, password)

print("-" * 50)
for i, product in enumerate(products):
    print(f"Product {i + 1} of {len(products)}")
    download_product(product, ACCESS_TOKEN)
    print("-" * 50)

print("Done")
