#create code to download the data and get the month operation

# In[ ]:


import asyncio
import aiohttp
import pandas as pd
import io   
import zipfile  
import chardet
import os
import re
import pdfplumber
from datetime import datetime
from datetime import timedelta
from parsers.MonthParser import MonthParser
from dowloaders.CsvDowloader import CsvDownloader
from dowloaders.PdfDownloader import PdfDownloader


def generate_urls_for_years(years, base_url="https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{}.zip"):
    return [base_url.format(year) for year in years]
    


async def main():
    urls = generate_urls_for_years(range(2010, 2022))

    csvdowloader = CsvDownloader(urls)

    pdfdownloader = PdfDownloader()

    files_df = await csvdowloader.download_and_read_multiple_zip_csv()

    company_name = "AMBEV S.A."

    pdfs = await pdfdownloader.get_company_pdfs(files_df, company_name)

    month_parser = MonthParser(pdfs)

    result = month_parser.parse_pdfs()  

    result.to_excel("operations_{}.xlsx".format(str(company_name)), index=False)


if __name__ == "__main__":
    asyncio.run(main())
    

# %%
