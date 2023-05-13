from datetime import datetime
import re
import pandas as pd
import pdfplumber
import asyncio
import aiohttp
import zipfile
import io
import cchardet as chardet



def generate_operations_df(text, month, year, responsible):
    # Define regex patterns for different types of operations
    intermediario_options = r"(?:Direto c/ a Cia|JP Morgan|Corretora Itaú|Goldman Sachs|BTG Pactual)"
    operacao_options = r"(?:Venda à vista|Compra à vista)"
    pattern = re.compile(r"((?:Ações|Outros))\s+((?:ON|ADR\sORDINARIA))\s+({})\s+({})\s+(\d+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)".format(intermediario_options, operacao_options), re.MULTILINE)
    exercicio_options_pattern = re.compile(r"Exercício de\n(Ações)\s+(ON)\s+(.+?)\s+(\d+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)")
    exercicio_options_subscription_pattern = re.compile(r"(Ações|Outros)\s+((?:ON|ADR\sORDINARIA))\s+(.+?)\s+((?:Subscrição\s+)?Exercício)(?:\s+Opção)?\s+(\d+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)")
    entrega_pattern = re.compile(r"(Ações|Outros)\s+(ON)\s+(.+?)\s+(.+?)\s+(\d+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)\n\((PSU)\)")

    # Find all matches for each pattern in the text
    matches = pattern.findall(text)
    exercicio_matches = exercicio_options_pattern.findall(text)
    exercicio_options_subscription_pattern_matches = exercicio_options_subscription_pattern.findall(text)
    entrega_pattern_matches = entrega_pattern.findall(text)

    # Initialize the operations list and set the header
    operations_list = []
    header = ["Valor Mobiliário/Derivativo", "Características dos Títulos", "Intermediário", "Operação", "Dia", "Quantidade", "Preço", "Volume (R$)"]
    operations_list.append(header)

    # Loop through all matches and append each operation to the list
    for match in matches + exercicio_matches + exercicio_options_subscription_pattern_matches + entrega_pattern_matches:
        operation = [match[0], match[1], match[2].strip(), match[3].strip() if len(match) == 8 else "Exercício de Opções", match[4 if len(match) == 8 else 3], match[5 if len(match) == 8 else 4], match[6 if len(match) == 8 else 5], match[7 if len(match) == 8 else 6]]
        operations_list.append(operation)

    # Create a DataFrame from the operations list
    operations_df = pd.DataFrame(operations_list[1:], columns=operations_list[0])

    # Add additional columns for month, year, and responsible
    operations_df['Mês'] = month
    operations_df['Ano'] = year
    operations_df['Responsável'] = responsible

    return operations_df

def extract_datetime(text):
    pattern = r"Em\s+(\d{2})\/(\d{4})"
    match = re.search(pattern, text)
    if match:
        month, year = match.groups()
        return datetime(int(year), int(month), 1)
    else:
        "return 2100-01-01 00:00:00"
        return datetime(2100, 1, 1) 
        
def generate_trade_df(operations_list, date):
    operations_responsibles = ["Conselho Administração", "Diretoria", "Conselho Fiscal"]
    dfs = []
    for operation, responsible in zip(operations_list, operations_responsibles):
        dfs.append(generate_operations_df(operation, date.month, date.year, responsible))
    if dfs:
        return pd.concat(dfs)
    else:
        return pd.DataFrame()

async def download_file(session, url):
    local_filename = url.split('/')[-1]
    async with session.get(url) as response:
        with open(local_filename, 'wb') as f:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                f.write(chunk)
    return local_filename


async def download_files(urls):
    files = []
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url) for url in urls]
        files = await asyncio.gather(*tasks)
    return files

def extract_operations(text, start_phrase, end_phrase):
    pattern = re.compile(f"{re.escape(start_phrase)}(?:.*?\n)+?.*?{re.escape(end_phrase)}", re.DOTALL)
    matches = pattern.findall(text)
    return matches

def filter_company_data(file_path, company_name):
    master_df = pd.read_excel(file_path)
    master_df = master_df[master_df["Categoria"] == "Valores Mobiliários negociados e detidos (art. 11 da Instr. CVM nº 358)"]
    company_filtered_df = master_df[master_df["Nome_Companhia"] == company_name]
    urls = list(company_filtered_df["Link_Download"])
    
    return urls

def filter_company_data_from_df(df, company_name):
    master_df = df

    master_df = master_df[master_df["Categoria"] == "Valores Mobiliários negociados e detidos (art. 11 da Instr. CVM nº 358)"]
    company_filtered_df = master_df[master_df["Nome_Companhia"] == company_name]
    urls = list(company_filtered_df["Link_Download"])
    
    return urls


async def concatenate_pdfs(files):
    dataframes = []

    for file in files:
        with pdfplumber.open(file) as pdf:
            pages = pdf.pages
            text = ""
            for page in pages:
                text += page.extract_text()

        operations_list = extract_operations(text, "Movimentações no Mês", "Saldo Final")
        operations_df = generate_trade_df(operations_list, extract_datetime(text))

        dataframes.append(operations_df)
    if dataframes:
        concatenated_df = pd.concat(dataframes)
        return concatenated_df
    else:
        return pd.DataFrame()

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.read()

async def read_zip_csv_from_url_async(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.read()
            with io.BytesIO(content) as stream:
                with zipfile.ZipFile(stream) as zf:
                    extracted_file = zf.open(zf.namelist()[0])
                    file_content = extracted_file.read()
                    encoding = chardet.detect(file_content)['encoding']
                    df = pd.read_csv(io.BytesIO(file_content), encoding=encoding, sep=";")
    return df

async def download_and_read_multiple_zip_csv(urls):
    dataframes = await asyncio.gather(*[read_zip_csv_from_url_async(url) for url in urls])
    return dataframes

def generate_urls_for_years(years, base_url="https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{}.zip"):
    return [base_url.format(year) for year in years]

async def process_company_data(dfs, company_name):
    concatenated_dfs = []

    for df in dfs:
        urls = filter_company_data_from_df(df, company_name)
        ap = await download_files(urls)
        operations = await concatenate_pdfs(ap)
        concatenated_dfs.append(operations)

    final_concatenated_df = pd.concat(concatenated_dfs)
    return final_concatenated_df

async def main():
    urls = generate_urls_for_years(range(2010, 2022))

    dataframes = await download_and_read_multiple_zip_csv(urls)

    company_name = "AMBEV S.A."

    result = await process_company_data(dataframes, company_name)

    result.to_excel("operations_{}.xlsx".format(str(company_name)), index=False)
# Run the main async function
asyncio.run(main())