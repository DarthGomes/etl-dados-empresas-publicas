from models.download_data import DownloadData
from models.unzip_files import UnzipFiles
from models.data_cleansing import DataCleansing

downloader = DownloadData()
unziper = UnzipFiles()
cleaner = DataCleansing()


def main():

    downloader.download_all()
    unziper.extract_all()
    result = cleaner.join_dataframes()

    if result is not None:
        print('Operação concluída com sucesso.')
        print(f'Tamanho do DataFrame resultante: {result.shape}')
    else:
        print('A operação falhou.')


if __name__ == '__main__':
    main()
