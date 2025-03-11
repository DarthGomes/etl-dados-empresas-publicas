import os
import shutil
import zipfile


class UnzipFiles:
    def __init__(self):
        self.extension = '.zip'
        self.base_dir = 'downloaded_files'

    def _unzip_files(self, directory):

        for root, dirs, files in os.walk(directory):
            for item in files:
                file_path = os.path.join(root, item)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    temp_dir = os.path.join(root, 'temp_extract')
                    zip_ref.extractall(temp_dir)

                    for extracted_file in os.listdir(temp_dir):
                        file_full_path = os.path.join(temp_dir, extracted_file)
                        if os.path.isfile(file_full_path):
                            new_name = f'{os.path.splitext(item)[0]}.csv'
                            os.rename(
                                file_full_path, os.path.join(root, new_name)
                            )

                    shutil.rmtree(temp_dir)

                os.remove(file_path)

    def extract_all(self):
        self._unzip_files(self.base_dir)
