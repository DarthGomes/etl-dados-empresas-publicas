import os
import time
from datetime import datetime

import backoff
import httpx
from dateutil.relativedelta import relativedelta
from tqdm import tqdm


class DownloadData:
    def __init__(self):
        self.attempts = 4
        self.reference_month = self.get_reference_month()
        self.available_versions = self.version_generate()
        self.urls = {
            'cnae': f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{self.reference_month}/Cnaes.zip',
            'empreendimentos': [
                f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{self.reference_month}/Estabelecimentos{i}.zip'
                for i in self.available_versions
            ],
            'empresas': [
                f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{self.reference_month}/Empresas{i}.zip'
                for i in self.available_versions
            ],
            'municipios': f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{self.reference_month}/Municipios.zip',
            'paises': f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{self.reference_month}/Paises.zip',
        }

    def _make_request(self):
        base_dir = 'downloaded_files'
        os.makedirs(base_dir, exist_ok=True)
        print(f'Arquivos serão salvos em: {os.path.abspath(base_dir)}')

        for file_type, url_or_urls in self.urls.items():
            file_dir = os.path.join(base_dir, file_type)
            os.makedirs(file_dir, exist_ok=True)

            if isinstance(url_or_urls, list):
                for url in url_or_urls:
                    self._download_file(url, file_dir)
            else:
                self._download_file(url_or_urls, file_dir)

    @backoff.on_exception(
        backoff.expo, (httpx.RequestError, httpx.ReadTimeout), max_tries=5
    )
    def _download_file(self, url, file_dir):
        file_name = url.split('/')[-1]
        file_path = os.path.join(file_dir, file_name)

        try:
            with httpx.Client(verify=False, timeout=60.0) as client:
                with client.stream('GET', url) as response:
                    total_size = int(response.headers.get('content-length', 0))

                    with open(file_path, 'wb') as file, tqdm(
                        desc=file_name,
                        total=total_size,
                        unit='iB',
                        unit_scale=True,
                        unit_divisor=1024,
                    ) as progress_bar:
                        for data in response.iter_bytes(chunk_size=8192):
                            size = file.write(data)
                            progress_bar.update(size)
                        file.flush()

            print(f'Downloaded: {file_path}')
        except Exception as e:
            print(f'Erro ao baixar {file_name}: {str(e)}')
            raise

    def download_all(self):
        self._make_request()

    def get_reference_month(self):
        current_month = datetime.now().strftime('%Y-%m')
        previous_month = (datetime.now() - relativedelta(months=1)).strftime(
            '%Y-%m'
        )

        if self.check_month_avaiability(current_month):
            return current_month
        else:
            return previous_month

    def check_month_avaiability(self, month: str):
        test_url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{month}/'
        for attempt in range(self.attempts):
            try:
                with httpx.Client(verify=False, timeout=30.0) as client:
                    response = client.get(test_url, follow_redirects=True)
                    return response.status_code == 200
            except httpx.RequestError:
                if attempt < self.attempts - 1:
                    time.sleep(5)
                else:
                    return False

    def version_generate(self):
        available_versions = []
        for i in range(12):
            if self.check_file_avaiability(i):
                available_versions.append(i)

            else:
                break
        return available_versions

    def check_file_avaiability(self, version: int):
        test_url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{self.reference_month}/Estabelecimentos{version}.zip'
        for attempt in range(self.attempts):
            try:
                with httpx.Client(verify=False, timeout=30.0) as client:
                    response = client.head(test_url, follow_redirects=True)
                    return response.status_code == 200
            except httpx.RequestError:
                if attempt < self.attempts - 1:
                    time.sleep(5)
                else:
                    return False
