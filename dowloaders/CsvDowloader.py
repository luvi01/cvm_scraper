import asyncio  
import aiohttp
import pandas as pd
import io   
import zipfile  
import chardet

class CsvDownloader:    
    def __init__(self, urls):
        self.urls = urls
        self.files = []
    
    async def read_zip_csv_from_url_async(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.read()
                with io.BytesIO(content) as stream:
                    with zipfile.ZipFile(stream) as zf:
                        extracted_file = zf.open(zf.namelist()[0])
                        file_content = extracted_file.read()
                        encoding = chardet.detect(file_content[:1024])['encoding']
                        print("encoding: {}".format(encoding))
                        df = pd.read_csv(io.BytesIO(file_content), encoding=encoding, sep=";")
        return df
    
    async def download_and_read_multiple_zip_csv(self):
        dataframes = await asyncio.gather(*[self.read_zip_csv_from_url_async(url) for url in self.urls])
        return dataframes