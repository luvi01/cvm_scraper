import re
import pandas as pd
import pdfplumber


class OperationsParser:
    def __init__(self, files):
        self.files = files
        self.operations_df = pd.DataFrame()

    def generate_operations_df(self, text, month, year, responsible):
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

    async def concatenate_pdfs(self):
        dataframes = []

        for file in self.files:
            with pdfplumber.open(file) as pdf:
                pages = pdf.pages
                text = ""
                for page in pages:
                    text += page.extract_text()

            operations_list = self.extract_operations(text, "Movimentações no Mês", "Saldo Final")
            operations_df = self.generate_trade_df(operations_list, self.extract_datetime(text))

            dataframes.append(operations_df)
        if dataframes:
            self.operations_df = pd.concat(dataframes)
        return self.operations_df
    
    def extract_operations(self, text, start_phrase, end_phrase):
        pattern = re.compile(f"{re.escape(start_phrase)}(?:.*?\n)+?.*?{re.escape(end_phrase)}", re.DOTALL)
        matches = pattern.findall(text)
        return matches  
    
    def extract_datetime(self, text):   
        pattern = re.compile(r"(\d{2})/(\d{2})/(\d{4})")
        match = pattern.search(text)
        if match:
            return pd.to_datetime(match.group(0), format="%d/%m/%Y")
        else:
            return pd.NaT   
        
    