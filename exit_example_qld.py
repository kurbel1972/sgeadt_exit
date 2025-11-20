import requests
import json
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

class CustomsWarehouseAPI:
    def __init__(self):
        self.base_url = os.getenv("base_url_qld")
        self.endpoint = os.getenv("endpoint_qld")
        self.url = f"{self.base_url}{self.endpoint}"
        self.client_id = os.getenv("client_id_qld")
        self.client_secret = os.getenv("client_secret_qld")
        self.headers = {
            #"User-Agent":"OutSystemsPlatform",
            "Content-Type": "application/json",
            "x-ibm-Client-ID": self.client_id,
            "x-ibm-Client-Secret": self.client_secret
            #"Host": "ctt-exp-customs-warehouse-rs-uat-mk4fed.kxd350.gbr-e1.cloudhub.io",
            #"Content-Length": "210"
        }
    
    def send_request(self, payload):
        try:
            response = requests.post(self.url, headers=self.headers, data=json.dumps(payload))
            response.raise_for_status()
            return response.json(), response.status_code
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}, None
        
    def main(self):
        print(self.client_id)
        print(self.client_secret)
        print(self.base_url)
        print(self.url)
        
        payload = [
            {
                "warehouseCode": "DTP00000969236PT",
                "internalReference": "CY358926326US",
                "customsRegime": "1000",
                "orderNumberDate": "2025-02-10",
                "diverseInfo1": "Devolução à Origem Cliente ou Expirado",
                "customsDebtValue": "0"
            }
        ]

        response, status_code = self.send_request(payload)
        
        if status_code:
            print(f"Response ({status_code}):", response)
        else:
            print("Error:", response["error"])


if __name__ == "__main__":
    api = CustomsWarehouseAPI()
    api.main()