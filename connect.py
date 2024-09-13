import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import OperationalError
import time

def conecta():
    load_dotenv()
    hostEnv = os.getenv("host")
    userEnv = os.getenv("user")
    passwordEnv = os.getenv("password")
    dbEnv = os.getenv("db")

    # Formatar a string de conexão com SQLAlchemy
    engine = create_engine(f'mysql+mysqlconnector://{userEnv}:{passwordEnv}@{hostEnv}/{dbEnv}', pool_recycle=3600)
    
    return engine

def executa(instrucao=None, valores=None, tabela=None, bulk_insert=False):
    engine = conecta()
    metadata = MetaData()

    # Tentativa com reconexão automática
    for tentativa in range(3):  # Tentar 3 vezes
        try:
            with engine.connect() as connection:
                if bulk_insert and tabela is not None and valores:
                    connection.execute(tabela.insert(), valores)
                elif instrucao and valores:
                    connection.execute(instrucao, valores)
                elif instrucao:
                    connection.execute(instrucao)
                break  # Se a execução foi bem-sucedida, sair do loop
        except OperationalError as e:
            print(f"Erro de conexão: {e}. Tentando novamente em 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"Erro inesperado: {e}")
            raise e  # Rethrow qualquer outro erro não relacionado à conexão
