import os.path
import re
import requests
import json
from collections import defaultdict
from tqdm import tqdm

# Base configuration
download_dir = os.path.expanduser("~/datasets-nas/permanent_crops/tiles")
conf_dir = "conf/config.json"
ACCESS_TOKEN = None
REFRESH_TOKEN = None

# Query parameters
# Sentinel naming convention: MMM_MSIXXX_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
tile_id = "T33SVB"  # Txxxxx - MGRS format
relative_orbit_number = "R079"  # Rxxx
start_date = "2017-01-01T00:00:00.000Z"  # YYYY-MM-DDThh:mm:ss.sssZ
end_date = "2017-12-31T00:00:00.000Z"
product_level = "MSIL2A"  # MSIxxx


def fetch_products(params):
    """Fetch filtered products and return a list of the most recent baseline products per date

    Args:
        params ({str: str}): Query params for data filtering.

    Returns:
        list: Return a list of fetched products.
    """
    products_by_date = defaultdict(list)
    baseline_pattern = r'_N(\d{4})_'
    base_url = url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    while url:
        try:
            response = requests.get(
                url, params=params if url == base_url else None)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Request error: {e}")
            break

        data = response.json()
        for product in data['value']:
            name = product['Name']
            date = product['ContentDate']['Start'][:10]

            match = re.search(baseline_pattern, name)
            if match:
                baseline_number = int(match.group(1))
                products_by_date[date].append((baseline_number, product))

        url = data.get('@odata.nextLink', None)

    # Create list of products with the most recent baseline for each date
    product_list = []
    for date, products in products_by_date.items():
        max_baseline_product = max(products, key=lambda x: x[0])[1]
        product_list.append(max_baseline_product)
        print(f"Date: {date}")
        print(f"Product Name: {max_baseline_product['Name']}")
        print(f"Product ID: {max_baseline_product['Id']}")
        print(f"Download URL: {max_baseline_product['S3Path']}")
        print("-" * 50)

    return product_list


def load_credentials(config_file: str = "conf/config.json"):
    """Load credentials from the JSON config file
    <br/>
    Config file example:
    ```
    {
        "username": "your_username",
        "password": "your_password"
    }
    ```

    Args:
        config_file (str, optional): Path to config file. Defaults to "conf/config.json".

    Returns:
        (str, str): Return a tuple (username, password)
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config['username'], config['password']


def get_access_token(username: str, password: str):
    """Request a new access_token and refresh_token

    Args:
        username (str): Username
        password (str): Password

    Returns:
        (str|None, str|None): Return a tuple (access_token, refresh_token) otherwise (None, None)
    """
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'client_id': 'cdse-public',
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        token_info = response.json()
        access_token = token_info.get('access_token')
        refresh_token = token_info.get('refresh_token')
        return access_token, refresh_token
    except requests.RequestException as e:
        print(f"Error: {e}")
        return None, None


def regenerate_access_token(refresh_token: str):
    """Regenerate the access_token using the refresh_token

    Args:
        refresh_token (str): Refresh token

    Returns:
        str|None: Returns the new access_token, None otherwise.
    """
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': 'cdse-public',
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        token_info = response.json()
        access_token = token_info.get('access_token')
        print("Access token regenerated.")
        return access_token
    except requests.RequestException as e:
        print(f"Error regenerating token: {e}")
        return None


def handle_token_expiry():
    """Handle token expiry by either refreshing or requesting a new access token

    Raises:
        Exception: In case of re-authentication fails
    """
    global ACCESS_TOKEN, REFRESH_TOKEN
    ACCESS_TOKEN = regenerate_access_token(REFRESH_TOKEN)

    if not ACCESS_TOKEN:
        print("Refresh token expired. Re-authenticating...")
        username, password = load_credentials(conf_dir)
        ACCESS_TOKEN, REFRESH_TOKEN = get_access_token(username, password)
        if not ACCESS_TOKEN:
            raise Exception(
                "Re-authentication failed. Please check your credentials.")


def download_product(product):
    """Download a specific product

    Args:
        product: Sentinel product to download.

    Raises:
        Exception: In case of too many failed attempts.
    """
    global ACCESS_TOKEN, REFRESH_TOKEN
    product_id = product['Id']
    product_name = product['Name']
    file_name = product_name.split('.')[0] + ".zip"
    file_path = os.path.join(download_dir, file_name)

    if os.path.exists(file_path):
        print(f"The product {product_name} already exists.")
        return

    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    attempt = 0
    while attempt < 3:
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        session = requests.Session()
        session.headers.update(headers)

        try:
            response = session.get(url, stream=True)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Download failed: {e}")
            handle_token_expiry()
            attempt += 1
            if attempt >= 3:
                raise Exception(
                    f"Too many failed attempts for {product_name}: {e}")
            continue

        total_size = int(response.headers.get('content-length', 0))
        with open(file_path, "wb") as file:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {product_name}") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        pbar.update(len(chunk))
        break


params = {
    "$filter": f"Collection/Name eq 'SENTINEL-2' and "
               f"contains(Name, '{tile_id}') and "
               f"contains(Name, '{relative_orbit_number}') and "
               f"contains(Name, '{product_level}') and "
               f"ContentDate/Start gt {start_date} and "
               f"ContentDate/Start lt {end_date}",
    "$orderby": "ContentDate/Start asc"
}

# Fetch the products
print("Fetching products...")
products = fetch_products(params)
print(f"Total products found: {len(products)}")

# Load credentials and request token
if not ACCESS_TOKEN:
    username, password = load_credentials(conf_dir)
    ACCESS_TOKEN, REFRESH_TOKEN = get_access_token(username, password)

# Download the products
print("-" * 50)
for i, product in enumerate(products):
    print(f"Downloading product {i + 1} of {len(products)}")
    download_product(product)
    print("-" * 50)

print("Download completed")
