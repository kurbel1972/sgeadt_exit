import requests
import json
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

def test_service():
    url = "https://ctt-sys-sgeadt-uat-mk4fed.internal-kxd350.gbr-e1.cloudhub.io:443/api/v1/exit"
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    headers = {
        "mulesoft_client_id": client_id,
        "mulesoft_client_secret": client_secret
    }
    payload = [
            {
                "warehouseCode": "DTP00000969236PT",
                "internalReference": "LW139190541DE",
                "customsRegime": "1000",
                "orderNumberDate": "1900-01-01",
                "diverseInfo1": "Devolução à Origem Cliente ou Expirado",
                "customsDebtValue": "0"
            }
        ]

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        print("Serviço disponível!")
        print(f"Response ({response.status_code}):", response.json())
    except requests.exceptions.RequestException as e:
        print(f"Falha ao verificar o serviço: {e}")
        if response is not None:
            print(f"Response ({response.status_code}):", response.text)
        else:
            print("Nenhuma resposta recebida.")

if __name__ == "__main__":
    test_service()