import pandas as pd
from ftplib import FTP

def get_population_dict():
    # log into the ftp server
    ftp = FTP('ftp.ibge.gov.br')
    ftp.login()
    # switch directory and get list of files, last file is the latest file
    ftp.cwd('Estimativas_de_Populacao/Estimativas_2025')
    files = ftp.nlst()
    latest_file = files[-1]

    # open a file on the local machine and download the file from the ftp server into it
    with open(latest_file, 'wb') as f:
        ftp.retrbinary(f'RETR {latest_file}', f.write)
    ftp.quit()

    # load the xls file using pandas
    df = pd.read_excel(latest_file, sheet_name=0, header=None)

    print(df.head(10))

    # assign the column names and drop the extra column
    df.columns = ['State', 'Population', 'Extra']
    df = df[['State', 'Population']]

    brazil_states = [
        'Rondônia', 'Acre', 'Amazonas', 'Roraima', 'Pará', 'Amapá', 'Tocantins',
        'Maranhão', 'Piauí', 'Ceará', 'Rio Grande do Norte', 'Paraíba', 'Pernambuco', 'Alagoas', 'Sergipe', 'Bahia',
        'Minas Gerais', 'Espírito Santo', 'Rio de Janeiro', 'São Paulo', 'Paraná', 'Santa Catarina', 'Rio Grande do Sul',
        'Mato Grosso do Sul', 'Mato Grosso', 'Goiás', 'Distrito Federal'
    ]
    df_states = df[df['State'].isin(brazil_states)]

    df_states['Population'] = pd.to_numeric(df_states['Population'], errors='coerce')

    return df_states.to_dict(orient = 'records')
