import streamlit as st
import pandas as pd
import queries

# Carregar os dados do banco
df = queries.queryGeralRelatorioVendas()

# Converter 'Data_Adesao' para datetime, ignorando possíveis erros
df['Data_Adesao'] = pd.to_datetime(df['Data_Adesao'], errors='coerce')

# Garantir que a conversão foi bem-sucedida
df = df[df['Data_Adesao'].notna()]

# Mostrar os primeiros registros de Data_Adesao para verificar
st.write("Visualização dos primeiros registros de Data_Adesao:", df['Data_Adesao'].head(10))

# Verificar se os meses problemáticos realmente existem nos dados
meses_verificacao = df[df['Data_Adesao'].dt.month.isin([5, 6, 7])]
st.write("Visualizando entradas para os meses 5, 6 e 7:", meses_verificacao[['Data_Adesao', 'Cod_Contrato']])

# Extrair os anos únicos e meses únicos
anos_disponiveis = df['Data_Adesao'].dt.year.dropna().unique()
meses_disponiveis = df['Data_Adesao'].dt.month.dropna().unique()

# Verificar se os meses estão sendo corretamente capturados
st.write("Meses disponíveis:", meses_disponiveis)

# Ordenar as listas de anos e meses
anos_disponiveis = sorted(anos_disponiveis)
meses_disponiveis = sorted(meses_disponiveis)

# Criar selectboxes para ano e mês
selecionaAno = st.selectbox('Selecione o ano:', anos_disponiveis)
selecionaMes = st.selectbox('Selecione o mês:', meses_disponiveis)

# Exibir valores selecionados para verificação
st.write(f"Ano selecionado: {selecionaAno}")
st.write(f"Mês selecionado: {selecionaMes}")
