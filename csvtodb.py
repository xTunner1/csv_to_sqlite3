import os
import pandas as pd
import sqlite3
from threading import Thread
from queue import Queue
from tqdm import tqdm
import psutil
from time import sleep
import csv
from pandas.io.json import build_table_schema
from termcolor import colored  # Importa para colorir o texto no console
import ctypes  # Importa para interagir com o sistema operacional

# Função para obter o uso de CPU e RAM
def get_cpu_ram_usage():
    cpu_usage = psutil.cpu_percent()
    ram_usage = psutil.virtual_memory().percent
    return cpu_usage, ram_usage

# Função para atualizar o título do console com o uso de CPU e RAM
def update_console_title():
    while True:
        cpu_usage, ram_usage = get_cpu_ram_usage()
        ctypes.windll.kernel32.SetConsoleTitleW(f"CPU: {cpu_usage}% | RAM: {ram_usage}%")
        sleep(1)

# Função para detectar o delimitador do CSV
def detect_delimiter(file_path, encoding='utf-8'):
    with open(file_path, 'r', encoding=encoding) as file:
        first_line = file.readline()
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(first_line).delimiter
    return delimiter

# Função para tentar diferentes codificações para ler o CSV
def try_different_encodings(file_path, encodings=['utf-8', 'latin1', 'iso-8859-1']):
    for encoding in encodings:
        try:
            delimiter = detect_delimiter(file_path, encoding)
            return delimiter, encoding
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("Nenhuma codificação válida encontrada para o arquivo.")

# Função para criar o esquema da tabela no banco de dados
def create_table_schema(cursor, table_name, schema):
    reserved_keywords = set([
        "ADD", "ALTER", "AND", "AS", "ASC", "BETWEEN", "BY", "CASE", "CHECK", "COLUMN", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCEPT", "EXISTS", "FOREIGN", "FROM", "FULL", "GROUP", "HAVING", "IN", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET", "TABLE", "THEN", "TO", "UNION", "UNIQUE", "UPDATE", "USING", "VALUES", "WHEN", "WHERE"
    ])
    columns = []
    for field in schema['fields']:
        col_name = field['name']
        col_type = field['type']
        if col_name.upper() in reserved_keywords:
            col_name = f'"{col_name}"'
        if col_type == 'integer':
            col_type = 'INTEGER'
        elif col_type == 'number':
            col_type = 'REAL'
        elif col_type == 'boolean':
            col_type = 'BOOLEAN'
        else:
            col_type = 'TEXT'
        columns.append(f"{col_name} {col_type}")
    columns_str = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
    cursor.execute(create_table_query)

# Função para processar o CSV e inserir os dados no banco de dados
def process_csv_to_db(csv_file, db_file, chunksize=10000):
    print(f"Processando arquivo: {csv_file}")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    delimiter, encoding = try_different_encodings(csv_file)
    for chunk in pd.read_csv(csv_file, chunksize=chunksize, delimiter=delimiter, encoding=encoding, on_bad_lines='skip'):
        if cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone() is None:
            schema = build_table_schema(chunk)
            create_table_schema(cursor, table_name, schema)
        chunk.to_sql(table_name, conn, if_exists='append', index=False)
    
    conn.close()

# Função para executar o trabalho em cada thread
def worker(queue, progress_bar, error_file):
    while not queue.empty():
        csv_file, db_file = queue.get()
        try:
            process_csv_to_db(csv_file, db_file)
        except Exception as e:
            with open(error_file, 'a') as f:
                f.write(f"Erro ao processar arquivo: {csv_file}\n")
                f.write(f"Erro: {str(e)}\n")
        finally:
            queue.task_done()
            progress_bar.update(1)

# Função para monitorar o uso de CPU e RAM
def monitor_system_usage():
    with tqdm(total=100, desc='Progresso', position=2) as progress_bar:
        while True:
            cpu_usage, ram_usage = get_cpu_ram_usage()
            progress_bar.set_description(f"Progresso | CPU: {cpu_usage}% | RAM: {ram_usage}%")
            sleep(1)

# Função principal
def main(input_folder, output_folder, error_file, num_threads=4):
    # Inicializa o monitor de uso de CPU e RAM
    monitor_thread = Thread(target=update_console_title, daemon=True)
    monitor_thread.start()

    # Prepara a fila de arquivos CSV
    queue = Queue()
    csv_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]
    for file in csv_files:
        csv_file = os.path.join(input_folder, file)
        db_file = os.path.join(output_folder, os.path.splitext(file)[0] + '.db')
        queue.put((csv_file, db_file))
    
    # Inicializa a barra de progresso
    total_files = len(csv_files)
    with tqdm(total=total_files, desc=f'Processando ({total_files} arquivos)', position=1) as progress_bar:
        # Inicializa as threads de trabalho
        threads = []
        for _ in range(num_threads):
            thread = Thread(target=worker, args=(queue, progress_bar, error_file))
            thread.start()
            threads.append(thread)
        
        # Espera até que todas as threads terminem
        queue.join()
        for thread in threads:
            thread.join()
        
        # Fecha a barra de progresso
        progress_bar.close()

# Caminhos de entrada e saída
input_folder = '.'
output_folder = './output'
error_file = './errors.txt'

if __name__ == '__main__':
    main(input_folder, output_folder, error_file)
