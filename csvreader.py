import pandas as pd
import numpy as np
from questdb.ingress import Sender, TimestampNanos

# Caminho do arquivo CSV
csv_file = 'questdb-usuarios-dataset.csv'

# Ler o arquivo CSV em um DataFrame do pandas
df = pd.read_csv(csv_file)

# Função para converter colunas específicas para o tipo correto
def convert_columns(row_data):
    # Tentar converter colunas específicas para float (double)
    for column in ['conexaoFinal', 'popCliente', 'motivoDesconexao']:
        if column in row_data:
            try:
                row_data[column] = float(row_data[column])
            except ValueError:
                # Se a conversão falhar, defina um valor padrão ou trate o erro conforme necessário
                row_data[column] = 0.0  # Exemplo de valor padrão
    return row_data

# Configuração de conexão com o QuestDB
conf = f'http::addr=teste_selecao_questdb:9000;'  # Use o nome do container QuestDB

# Conectando-se ao QuestDB e enviando os dados
with Sender.from_conf(conf) as sender:
    # Loop através de cada linha do DataFrame
    for index, row in df.iterrows():
        # Convertendo os valores para tipos nativos do Python e tratando os tipos de dados
        row_data = {key: (int(value) if isinstance(value, (np.int64, np.int32)) else value)
                    for key, value in row.items()}
        
        # Converte colunas específicas para o tipo esperado
        row_data = convert_columns(row_data)

        # Inserir dados no QuestDB
        sender.row(
            'nome_da_tabela',
            columns=row_data,
            at=TimestampNanos.now()
        )

    # Enviar os dados acumulados para o QuestDB
    sender.flush()

print("Dados importados com sucesso para o QuestDB!")

