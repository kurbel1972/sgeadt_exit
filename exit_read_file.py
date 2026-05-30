import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime
import logging
import smtplib
import ssl
from email.mime.text import MIMEText

try:
    import pyodbc
except Exception:
    pyodbc = None

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

logger = logging.getLogger(__name__)


def _env(*keys: str, default=None):
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


def send_email_summary(subject: str, body: str) -> None:
    email_address = _env("EMAIL_ADDRESS", "email_address")
    email_to = _env("EMAIL_ADDRESS_TO_SENT", "email_address_to_sent", "EMAIL_TO")
    smtp_server = _env("SMTP_SERVER", "smtp_server")
    smtp_username = _env("SMTP_USERNAME", "smtp_username", default=email_address)
    smtp_password = _env("SMTP_PASSWORD", "smtp_password")

    try:
        smtp_port = int(_env("SMTP_PORT", "smtp_port", default=587))
    except (TypeError, ValueError):
        smtp_port = 587

    smtp_use_tls = str(_env("SMTP_USE_TLS", "smtp_use_tls", default="true")).lower() != "false"
    smtp_tls_verify = str(_env("SMTP_TLS_VERIFY", "smtp_tls_verify", default="true")).lower() != "false"

    if not all([email_address, email_to, smtp_server, smtp_username]):
        logger.warning("Email summary skipped due to incomplete SMTP configuration.")
        return

    message = MIMEText(body, "plain", "utf-8")
    message["Subject"] = subject
    message["From"] = email_address
    recipients = [addr.strip() for addr in email_to.split(",") if addr.strip()]
    message["To"] = ", ".join(recipients)

    with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
        if smtp_use_tls:
            if smtp_tls_verify:
                context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
                context.check_hostname = True
                context.verify_mode = ssl.CERT_REQUIRED
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                server.starttls(context=context)
            else:
                server.starttls()

        if smtp_password:
            server.login(smtp_username, smtp_password)

        server.sendmail(email_address, recipients, message.as_string())
        logger.info("Summary email sent to %s", ", ".join(recipients))

class CustomsWarehouseAPI:
    def __init__(self):
        self.base_url = os.getenv("base_url")
        self.endpoint = os.getenv("endpoint")
        self.url = f"{self.base_url}{self.endpoint}"
        self.client_id = os.getenv("client_id")
        self.client_secret = os.getenv("client_secret")
        # Caminho opcional para ficheiro de CA/certificado em formato .pem
        # Definir em .env como ca_bundle_path=C:\\caminho\\para\\certificado.pem
        self.ca_bundle_path = os.getenv("ca_bundle_path")
        # Permite desativar verificação de SSL apenas para testes (verify_ssl=false)
        self.verify_ssl = os.getenv("verify_ssl", "true").lower() != "false"
        self.headers = {
            "Content-Type": "application/json",
            "x-ibm-Client-ID": self.client_id,
            "x-ibm-Client-Secret": self.client_secret
        }
        self.inbox_path = os.getenv("inbox_path")
        self.processed_path = os.getenv("processed_path")
        self.error_path = os.getenv("error_path")
        self.sqlserver_connection_string = os.getenv("sqlserver_connection_string")
        self.sqlserver_driver = os.getenv("sqlserver_driver", "ODBC Driver 17 for SQL Server")
        self.sqlserver_server = os.getenv("sqlserver_server")
        self.sqlserver_database = os.getenv("sqlserver_database")
        self.sqlserver_user = os.getenv("sqlserver_user")
        self.sqlserver_password = os.getenv("sqlserver_password")
        self.sqlserver_encrypt = os.getenv("sqlserver_encrypt", "yes")
        self.sqlserver_trust_cert = os.getenv("sqlserver_trust_server_certificate", "yes")
        markers_env = os.getenv("success_on_error_markers", "")
        self.success_on_error_markers = [
            marker.strip().lower()
            for marker in markers_env.split("|")
            if marker.strip()
        ]
        self.counter = 0
        self.has_errors = False
        self.total_files_found = 0
        self.files_processed_ok = 0
        self.files_processed_with_errors = 0
        self.files_processing_failed = 0
        self.total_rows = 0
        self.total_error_rows = 0
        self.total_logical_success_rows = 0
        self.total_db_confirmed_rows = 0
        self.total_retried_rows_recovered = 0
        self.total_additional_retry_attempts = 0
        self.run_started_at = None
        self.inbox_unavailable = False
        self.no_files_found = False
    
    def send_request(self, payload):
        try:
            # Configurar parâmetro verify do requests:
            # - False se verify_ssl=false (apenas para testes)
            # - Caminho do bundle se ca_bundle_path estiver definido
            # - True (padrão) caso contrário
            if not self.verify_ssl:
                verify_param = False
            elif self.ca_bundle_path:
                verify_param = self.ca_bundle_path
            else:
                verify_param = True

            response = requests.post(
                self.url,
                headers=self.headers,
                data=json.dumps(payload),
                verify=verify_param,
                timeout=30
            )
            return response.json(), response.status_code
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}, None

    def log_error(self, row, log_path):
        # Ajustar a data para o formato "dd/MM/yyyy"
        row["orderNumberDate"] = row["orderNumberDate"].strftime('%d/%m/%Y') if not pd.isna(row["orderNumberDate"]) else None

        # Criar um DataFrame com a linha que deu erro
        error_df = pd.DataFrame([row])

        # Verificar se o arquivo de log já existe
        if os.path.exists(log_path):
            existing_df = pd.read_excel(log_path)
            updated_df = pd.concat([existing_df, error_df], ignore_index=True)
            updated_df.to_excel(log_path, index=False)
        else:
            error_df.to_excel(log_path, index=False)
        
        self.has_errors = True
        self.total_error_rows += 1

    def _response_text(self, response):
        if response is None:
            return ""

        if isinstance(response, str):
            return response

        if isinstance(response, (dict, list)):
            try:
                return json.dumps(response, ensure_ascii=False)
            except TypeError:
                return str(response)

        return str(response)

    def is_logical_success(self, status_code, response):
        """Trata alguns erros de retorno como sucesso lógico, via marcadores configuráveis."""
        if status_code is None or not self.success_on_error_markers:
            return False

        response_text = self._response_text(response).lower()
        return any(marker in response_text for marker in self.success_on_error_markers)

    def build_summary(self):
        run_finished_at = datetime.now()
        if self.run_started_at is None:
            self.run_started_at = run_finished_at

        duration_seconds = int((run_finished_at - self.run_started_at).total_seconds())
        total_success_rows = max(self.total_rows - self.total_error_rows, 0)

        if self.inbox_unavailable:
            status = "FALHA"
            detail = "A pasta de inbox está indisponível."
        elif self.no_files_found:
            status = "SEM FICHEIROS"
            detail = "Não foram encontrados ficheiros para processar."
        elif self.total_error_rows > 0 or self.files_processing_failed > 0:
            status = "CONCLUÍDO COM ERROS"
            detail = "O processamento terminou com registos/ficheiros com erro."
        else:
            status = "CONCLUÍDO COM SUCESSO"
            detail = "Todos os ficheiros e registos foram processados sem erros."

        subject = f"EXIT | Resumo de processamento | {status}"
        body = "\n".join([
            "Resumo do processamento EXIT",
            "=" * 60,
            f"Status: {status}",
            f"Detalhe: {detail}",
            "",
            f"Início: {self.run_started_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Fim: {run_finished_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Duração (s): {duration_seconds}",
            "",
            "Totalizadores:",
            f"- Ficheiros encontrados: {self.total_files_found}",
            f"- Ficheiros processados sem erros: {self.files_processed_ok}",
            f"- Ficheiros processados com erros: {self.files_processed_with_errors}",
            f"- Ficheiros com falha de processamento: {self.files_processing_failed}",
            f"- Linhas lidas: {self.total_rows}",
            f"- Linhas enviadas com sucesso: {total_success_rows}",
            f"- Linhas com erro: {self.total_error_rows}",
            f"- Linhas com sucesso lógico (erro com persistência): {self.total_logical_success_rows}",
            f"- Linhas confirmadas na BD SQL Server: {self.total_db_confirmed_rows}",
            f"- Linhas recuperadas por retry: {self.total_retried_rows_recovered}",
            f"- Tentativas adicionais de retry: {self.total_additional_retry_attempts}",
        ])
        return subject, body

    def send_final_email(self):
        subject, body = self.build_summary()
        print("\n" + "=" * 80)
        print("Resumo final")
        print(body)
        print("=" * 80)

        try:
            send_email_summary(subject, body)
            print("Resumo enviado por email.")
        except Exception as e:
            print(f"Falha ao enviar email de resumo: {e}")

    def _get_db_connection_string(self):
        if self.sqlserver_connection_string:
            return self.sqlserver_connection_string

        if not all([
            self.sqlserver_server,
            self.sqlserver_database,
            self.sqlserver_user,
            self.sqlserver_password,
        ]):
            return None

        return (
            f"DRIVER={{{self.sqlserver_driver}}};"
            f"SERVER={self.sqlserver_server};"
            f"DATABASE={self.sqlserver_database};"
            f"UID={self.sqlserver_user};"
            f"PWD={self.sqlserver_password};"
            f"Encrypt={self.sqlserver_encrypt};"
            f"TrustServerCertificate={self.sqlserver_trust_cert};"
        )

    def exists_in_sqlserver(self, internal_reference):
        if pyodbc is None:
            return False

        connection_string = self._get_db_connection_string()
        if not connection_string:
            return False

        query = """
        SELECT TOP 1 sai.sac_dcri
        FROM MOV_ENTL etl WITH(NOLOCK), MOV_ENTC etc WITH(NOLOCK), MOV_SAIC sai WITH(NOLOCK)
        WHERE etc.ARM_CODI = etl.ARM_CODI
          AND etc.ENC_NUME = etl.ENC_NUME
          AND etl.ARM_CODI = sai.ARM_CODI
          AND etl.ENC_NUME = sai.ENC_NUME
          AND etl.ENL_NLIN = sai.ENL_NLIN
          AND enl_refi = ?
        """

        try:
            with pyodbc.connect(connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute(query, internal_reference)
                row = cursor.fetchone()
                return row is not None
        except Exception as e:
            print(f"Aviso: falha ao consultar SQL Server para {internal_reference}: {e}")
            return False

    def reconcile_pending_rows(self, pending_error_rows):
        for item in pending_error_rows:
            row = item["row"]
            log_path = item["log_path"]
            internal_reference = str(row.get("internalReference", "")).strip()

            if internal_reference and self.exists_in_sqlserver(internal_reference):
                print(
                    f"Linha {internal_reference} tratada como sucesso: registo encontrado na BD SQL Server."
                )
                self.total_logical_success_rows += 1
                self.total_db_confirmed_rows += 1
                continue

            self.log_error(row, log_path)

    def create_payload(self, row):
        return [
            {
                "warehouseCode": row["warehouseCode"],
                "internalReference": row["internalReference"],
                "customsRegime": str(row["customsRegime"]).replace(" ", "") if not pd.isna(row["customsRegime"]) else None,
                "orderNumber": str(row["orderNumber"]).replace(" ", "") if not pd.isna(row["orderNumber"]) else None,
                "orderNumberDate": row["orderNumberDate"].strftime('%Y-%m-%d') if not pd.isna(row["orderNumberDate"]) else None,
                # A API não aceita null em diverseInfo1, espera sempre String
                "diverseInfo1": "" if pd.isna(row["diverseInfo1"]) else str(row["diverseInfo1"]),
                "diverseInfo2": row["diverseInfo2"] if not pd.isna(row["diverseInfo2"]) else None,
                # A API espera customsDebtValue como string (ver definição Exit em Customs_Warehouse_API-1.0.2.yaml)
                "customsDebtValue": str(row["customsDebtValue"]).replace(" ", "") if not pd.isna(row["customsDebtValue"]) else None
            }
        ]

    def process_row(self, index, row, max_retries, log_path, pending_error_rows):
        self.counter += 1
        print(f"Linha {index + 2} | internalReference: {row['internalReference']}")
        payload = self.create_payload(row)
        # Debug: ver sempre o payload que está a ser enviado
        try:
            print("Payload gerado:", json.dumps(payload, ensure_ascii=False))
        except TypeError:
            # Fallback caso exista algum tipo não serializável por json.dumps
            print(f"Payload gerado (repr): {payload}")

        attempt = 0
        while attempt < max_retries:
            response, status_code = self.send_request(payload)
            # Sucesso (2xx)
            if status_code is not None and 200 <= status_code < 300:
                print(f"Response {row['internalReference']} ({status_code}):", response)
                if attempt > 0:
                    self.total_retried_rows_recovered += 1
                return

            # Sucesso lógico: API devolve erro, mas já persistiu na BD.
            if self.is_logical_success(status_code, response):
                print(
                    f"Response {row['internalReference']} ({status_code}) tratado como sucesso lógico: {response}"
                )
                self.total_logical_success_rows += 1
                if attempt > 0:
                    self.total_retried_rows_recovered += 1
                return

            attempt += 1
            self.total_additional_retry_attempts += 1
            if status_code is None:
                print(f"Tentativa {attempt} falhou para {row['internalReference']}: {response.get('error', response)}")
            else:
                print(f"Tentativa {attempt} falhou para {row['internalReference']} ({status_code}): {response}")

            if attempt < max_retries:
                print(f"A tentar novamente {row['internalReference']}...")
                continue

            # Só regista erro após esgotar todas as tentativas
            try:
                print("Payload a enviar:", json.dumps(payload, ensure_ascii=False))
            except TypeError:
                print(f"Payload a enviar (repr): {payload}")
            pending_error_rows.append({
                "row": row.copy(),
                "log_path": log_path,
            })
            return

    def move_file(self, file_path, destination_folder):
        """Move o ficheiro para a pasta de destino"""
        try:
            # Criar a pasta de destino se não existir
            os.makedirs(destination_folder, exist_ok=True)
            
            # Construir o caminho de destino
            destination = Path(destination_folder) / Path(file_path).name
            
            # Mover o ficheiro
            shutil.move(str(file_path), str(destination))
            print(f"Ficheiro movido para: {destination}")
            
            # Mover também o ficheiro de log se existir
            log_path = str(file_path).replace(".xlsx", "_log.xlsx")
            if os.path.exists(log_path):
                log_destination = Path(destination_folder) / Path(log_path).name
                shutil.move(log_path, str(log_destination))
                print(f"Ficheiro de log movido para: {log_destination}")
                
        except Exception as e:
            print(f"Erro ao mover ficheiro {file_path}: {str(e)}")

    def process_file(self, file_path, max_retries):
        print(f"\n{'='*80}")
        print(f"Processando ficheiro: {file_path}")
        print(f"{'='*80}\n")
        
        log_path = str(file_path).replace(".xlsx", "_log.xlsx")
        self.has_errors = False
        pending_error_rows = []
        
        try:
            df = pd.read_excel(file_path, dtype={"customsRegime": str, "orderNumber": str})
            df["orderNumberDate"] = pd.to_datetime(df["orderNumberDate"], format='%d/%m/%Y', errors='coerce')
            self.total_rows += len(df)

            for index, row in df.iterrows():
                self.process_row(index, row, max_retries, log_path, pending_error_rows)

            self.reconcile_pending_rows(pending_error_rows)
                
            print(f"\nFicheiro {file_path} processado com sucesso!")
            
            # Mover ficheiro para a pasta adequada
            if self.has_errors:
                print("Ficheiro contém erros. A mover para pasta de erros...")
                self.move_file(file_path, self.error_path)
                self.files_processed_with_errors += 1
            else:
                print("Ficheiro processado sem erros. A mover para pasta de processados...")
                self.move_file(file_path, self.processed_path)
                self.files_processed_ok += 1
                
        except Exception as e:
            print(f"\nErro ao processar ficheiro {file_path}: {str(e)}")
            print("A mover ficheiro para pasta de erros...")
            self.move_file(file_path, self.error_path)
            self.files_processing_failed += 1

    def main(self, max_retries=2):
        self.run_started_at = datetime.now()
        try:
            print(self.client_id)
            print(self.client_secret)
            print(self.base_url)
            print(self.url)
            print(f"\nA procurar ficheiros em: {self.inbox_path}\n")

            # Verificar se a diretoria existe
            if not os.path.exists(self.inbox_path):
                print(f"ERRO: A diretoria {self.inbox_path} não existe ou não está acessível!")
                self.inbox_unavailable = True
                return

            # Obter todos os ficheiros .xlsx da diretoria
            excel_files = list(Path(self.inbox_path).glob("*.xlsx"))
            self.total_files_found = len(excel_files)
            
            if not excel_files:
                print(f"Nenhum ficheiro .xlsx encontrado em {self.inbox_path}")
                self.no_files_found = True
                return
            
            print(f"Encontrados {len(excel_files)} ficheiro(s) para processar:\n")
            for file in excel_files:
                print(f"  - {file.name}")
            print()

            # Processar cada ficheiro
            for file_path in excel_files:
                self.counter = 0  # Reset counter para cada ficheiro
                self.process_file(file_path, max_retries)

            print(f"\n{'='*80}")
            print("Processamento de todos os ficheiros concluído!")
            print(f"{'='*80}")
        finally:
            self.send_final_email()

if __name__ == "__main__":    
    api = CustomsWarehouseAPI()
    api.main(max_retries=2)