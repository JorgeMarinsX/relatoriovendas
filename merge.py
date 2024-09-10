import pandas as pd
import os

def juntar_csvs(pasta):
    # Listar todos os arquivos CSV na pasta
    arquivos_csv = [f for f in os.listdir(pasta) if f.endswith('.csv')]
    
    # Criar uma lista para armazenar os DataFrames
    dataframes = []
    
    # Iterar sobre os arquivos CSV e carregar cada um deles em um DataFrame
    for arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(pasta, arquivo)
        df = pd.read_csv(caminho_arquivo, sep=",")  # Altere o separador se necessário
        dataframes.append(df)
    
    # Concatenar todos os DataFrames em um único
    df_final = pd.concat(dataframes, ignore_index=True)
    
    return df_final


# Caminho para a pasta contendo os CSVs
pasta = 'df/pool'

# Juntar todos os CSVs em um único DataFrame
df_final = juntar_csvs(pasta)




