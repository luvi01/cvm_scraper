import asyncio
import aiohttp
import os
from urllib.parse import urlparse, parse_qs


class PdfDownloader:
    def __init__(self):
        self.files = []

    async def download_pdf_file(self, session, url):
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        local_filename = f"{params['CodigoInstituicao'][0]}_{params['numProtocolo'][0]}_{params['numSequencia'][0]}_{params['numVersao'][0]}.pdf"
        local_filename = os.path.join(os.getcwd(), local_filename)
        async with session.get(url) as response:
            with open(local_filename, 'wb') as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
        return local_filename

    async def download_pdfs_files(self, urls_date):
        async with aiohttp.ClientSession() as session:
            self.files = []
            for url, date in urls_date:
                try:
                    file = await asyncio.wait_for(self.download_pdf_file(session, url), timeout=60.0)  # Set your desired timeout here
                    self.files.append([file, date, url])
                    await asyncio.sleep(0.25)  # Wait for 1 second
                except asyncio.TimeoutError:
                    print("Task took too long to complete and was skipped.")
        return self.files
    
    def filter_company_data_from_df(self, df, company_name):
        master_df = df

        master_df = master_df[master_df["Categoria"] == "Valores Mobiliários negociados e detidos (art. 11 da Instr. CVM nº 358)"]
        company_filtered_df = master_df[master_df["Nome_Companhia"] == company_name]
        if company_filtered_df.empty:
            return []   
        else:
            urls_dates = list(company_filtered_df[["Link_Download", "Data_Referencia"]].itertuples(index=False))
            return urls_dates 

    async def get_company_pdfs(self, dfs, company_name):
        i = 1
        for df in dfs:
            i += 1
            print(i)
            urls_dates = self.filter_company_data_from_df(df, company_name)
            print(len(urls_dates), "urls")
            print(urls_dates)

            self.files = await self.download_pdfs_files(urls_dates)
        
        print("Fim do download dos arquivos")
            
        return self.files