import re
import pandas as pd
import pdfplumber


class MonthParser:
    def __init__(self, files):
        self.files = files
        self.month_df = pd.DataFrame()

    def extract_parts(self, text):
        # Extract Saldo Inicial
        saldo_inicial = re.findall(r'Saldo Inicial(.*?)Movimentações no Mês', text, re.DOTALL)

        # Extract Saldo Final
        saldo_final = re.findall(r'Saldo Final(.*?)(FORMULÁRIO CONSOLIDADO|$)', text, re.DOTALL)

        saldo_final = [t[0] for t in saldo_final]

        return saldo_inicial, saldo_final

    def extract_info(self, text):
        # Extract the class and quantity
        matches = re.findall(r'((?:\w+ ?){1,4}) (\d{1,3}(?:\.\d{3})*)', text)

        # Convert the matches to a list of lists, removing the dots from the quantities
        info = [[match[0].strip(), int(match[1].replace('.', ''))] for match in matches]

        return info

    def create_df(self, saldo_inicial_list, saldo_final_list, members, date, url):
        df_final = pd.DataFrame(columns=["Class", "Initial Qty", "Final Qty", "Net Qty", "Member", "Date"])   

        for saldo_inicial, saldo_final, member in zip(saldo_final_list, saldo_inicial_list, members): 
            saldo_inicial = pd.DataFrame(saldo_inicial, columns=["Class", "Initial Qty"])
            saldo_final = pd.DataFrame(saldo_final, columns=["Class", "Final Qty"])

            df = pd.merge(saldo_inicial, saldo_final, on="Class")
            df["Net Qty"] = df["Final Qty"] - df["Initial Qty"]
            df["Member"] = member
            df["Date"] = date
            df["URL"] = url

            df_final = pd.concat([df_final, df])
        
        return df_final

    def parse_pdfs(self):
        dataframes = []

        for file, date, url in self.files:
            with pdfplumber.open(file) as pdf:
                pages = pdf.pages
                text = ""
                for page in pages:
                    text += page.extract_text()

            saldo_inicial_text_list, saldo_final_text_list = self.extract_parts(text)

            saldo_inicial_list = []
            saldo_final_list = []

            for saldo_inicial_text, saldo_final_text in zip(saldo_inicial_text_list, saldo_final_text_list):
                saldo_inicial_list.append(self.extract_info(saldo_inicial_text))
                saldo_final_list.append(self.extract_info(saldo_final_text))

            members = ["Conselho Administração", "Diretoria", "Conselho Fiscal"]

            month_operations_df = self.create_df(saldo_inicial_list,
                                                 saldo_final_list,
                                                 members,
                                                 date,
                                                 url)

            dataframes.append(month_operations_df)

        if dataframes:
            self.operations_df = pd.concat(dataframes)
        return self.operations_df
    
    def extract_datetime(self, text):   
        pattern = re.compile(r"(\d{2})/(\d{2})/(\d{4})")
        match = pattern.search(text)
        if match:
            return pd.to_datetime(match.group(0), format="%d/%m/%Y")
        else:
            return pd.NaT   
        
    