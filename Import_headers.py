import requests
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

base_url = os.getenv("base_url_qld")
endpoint = os.getenv("endpoint_qld")
url = f"{base_url}{endpoint}"
print(f"url: {url}")
response = requests.options(url)

print("Status Code:", response.status_code)
print("Headers:", response.headers)
print("Body:", response.text)
