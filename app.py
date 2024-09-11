import data
import dash
import queries
import streamlit as st

st.title("Leads que se tornaram clientes")
df = data.conversaoLista().reset_index(drop=True)
st.dataframe(df)

st.header("Pronto para inclusão em banco de dados")
st.write(data.conversaoLista())


st.header("Verificação do Banco de Dados de conversão")
st.write(queries.tabelaConversaoGeral())


st.header("Verificação do Banco de Dados de vendas")
st.write(queries.pathData('vendas'))