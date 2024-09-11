import connect as ct
import pandas as pd

#Cada função representa uma query

def pathData(data):
    if (data == 'vendas'):
        df =  pd.read_sql('SELECT * FROM `vendas`', con=ct.conecta())
        return df
    elif (data == 'leads'):
        df = pd.read_sql('SELECT * FROM `leads`', con=ct.conecta())
        return df

def queryGeralRelatorioVendas():
    df = pd.read_sql('SELECT * FROM `relatorioconversaogeral`', con=ct.conecta())
    return df

def verificaLeadsPorEmail():
    df = pd.read_sql("SELECT Email FROM leads", con=ct.conecta())
    return df

def verificaCodContrato():
    df= pd.read_sql("SELECT Cod_Contrato FROM vendas", con=ct.conecta())
    return df

def tabelaConversaoGeral():
    df = pd.read_sql("SELECT * FROM relatorioconversaogeral", con=ct.conecta())
    return df