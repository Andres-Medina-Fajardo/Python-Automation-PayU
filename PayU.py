import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
    
# Load data from Excel files into dataframes
transaccion = pd.read_excel('Assets/data_tabla_transaccion.xlsx')
tarjeta_credito = pd.read_excel('Assets/data_tabla_tarjeta_credito.xlsx')
transaccion_montos_adicionales = pd.read_excel('Assets/data_tabla_transaccion_montos_adicionales.xlsx')
transacciones_alertadas = pd.read_excel('Assets/formato_ingreso_correcto.xlsx')

# Function to clean dataframes by stripping leading/trailing whitespaces
def clean_dataframes(dataframe):
    return dataframe.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    

# Clean each dataframe
transaccion = clean_dataframes(transaccion)
tarjeta_credito = clean_dataframes(tarjeta_credito)
transaccion_montos_adicionales = clean_dataframes(transaccion_montos_adicionales)
transacciones_alertadas = clean_dataframes(transacciones_alertadas)


# Create SQLite database connection and cursor
connection = sqlite3.connect('Database/PayU_DB.sqlite')
cursor = connection.cursor()


# Function to create database tables if they don't exist
def create_tables():
    
    #Create tarjeta_credito table
    cursor.execute(
    '''CREATE TABLE IF NOT EXISTS tarjeta_credito(
        tarjeta_credito_id VARCHAR(36) PRIMARY KEY,
        numero_visible VARCHAR(18)
        )
    '''
    )
    
    #Create transaccion_montos_adicionales table
    cursor.execute(
    '''CREATE TABLE IF NOT EXISTS transaccion_montos_adicionales(
        fecha_creacion INTEGER,
        transaccion_id VARCHAR(64) PRIMARY KEY,
        valor_transaccion_moneda_local REAL,
        valor_transaccion_usd REAL
        )
    '''
    )
    
    #Create transaccion table
    cursor.execute(
    '''CREATE TABLE IF NOT EXISTS transaccion(
        fecha_creacion INTEGER,
        orden_id VARCHAR(16),
        transaccion_id VARCHAR(64) PRIMARY KEY,
        codigo_autorizacion VARCHAR(8),
        tarjeta_credito_id VARCHAR(64),
        usuario_id VARCHAR(16),
        FOREIGN KEY (tarjeta_credito_id) REFERENCES tarjeta_credito(tarjeta_credito_id),
        FOREIGN KEY (transaccion_id) REFERENCES transaccion_montos_adicionales(transaccion_id)
        )
    '''
    )

    connection.commit()
    print("Tables created successfully")    


# Function to convert datetime objects to integer value
def change_type(value):
    
    if isinstance(value, datetime):
        return  int(value.timestamp())
    else:
        return value

# Function to insert dataframes into the database
def insert_dataframe(query, dataframe):
    
    data_to_insert = [tuple(row) for row in dataframe.values]
    data_to_insert = map(lambda row: tuple(map(change_type,row)),data_to_insert)
    
    cursor.executemany(query, data_to_insert)
    

# Function to insert data into tables
def insert_data():
    
    insert_dataframe(
        '''INSERT OR IGNORE INTO tarjeta_credito (tarjeta_credito_id, numero_visible)
            VALUES (?, ?)''', tarjeta_credito)
    
    insert_dataframe('''INSERT OR IGNORE INTO transaccion_montos_adicionales (fecha_creacion, transaccion_id, 
                     valor_transaccion_moneda_local, valor_transaccion_usd) VALUES (?,?,?,?)''', transaccion_montos_adicionales)

    insert_dataframe('''INSERT OR IGNORE INTO transaccion (fecha_creacion, orden_id, transaccion_id, 
                     codigo_autorizacion, tarjeta_credito_id, usuario_id) VALUES (?,?,?,?,?,?)''', transaccion)
    
    connection.commit()
    print("Data inserted successfully")

# Define dataclasses for storing transaction information
@dataclass
class AlertedTransaction:
    numero_autorizacion: str
    fecha_transaccion: datetime
    numero_visible: str
    valor: float

@dataclass
class TransactionFound:
    numero_autorizacion: str
    transacción_id: str
    orden_id: str
    usuario_id: str
    cuenta_id: str

# Set percentage and days difference allowed for matching transactions
allowed_percentage_difference = 0.05
days_difference_allowed = 2

# Function to flatten nested lists
def flatten(nested_list):
    return [item for sublist in nested_list if sublist for item in sublist]


# Function to analyze transactions and find matches
def analyze_transactions():
    
    create_tables()
    insert_data()
    
    print("Analyzing transactions...")
    alerted_transactions = list(map(lambda alerted_transaction: AlertedTransaction(**alerted_transaction[1].to_dict()),transacciones_alertadas.iterrows()))
    transactions = list(map(analyze_transaction, alerted_transactions))

    transactions_found = list(map(lambda transaction: TransactionFound(*transaction),flatten(transactions)))
    
    transacciones_encontradas = pd.DataFrame(transactions_found , columns=['numero_autorizacion', 'transacción_id', 'orden_id', 'usuario_id', 'cuenta_id'])
    
    print("Transactions analysis completed")
    transacciones_encontradas.to_excel('Results/transacciones_encontradas.xlsx', index=False)
    print("Results saved to 'Results/transacciones_encontradas.xlsx'")
    
# Function to analyze a single transaction and find matches
def analyze_transaction(transaction: AlertedTransaction):
    
    
    lower_value = transaction.valor * (1-allowed_percentage_difference)
    upper_value = transaction.valor * (1+allowed_percentage_difference)

    
    matching_transactions = cursor.execute(f'''SELECT codigo_autorizacion, transaccion.transaccion_id, orden_id, usuario_id, transaccion.tarjeta_credito_id FROM transaccion 
                                    LEFT OUTER JOIN tarjeta_credito ON transaccion.tarjeta_credito_id = tarjeta_credito.tarjeta_credito_id 
                                    LEFT OUTER JOIN transaccion_montos_adicionales ON transaccion.transaccion_id = transaccion_montos_adicionales.transaccion_id
                                    WHERE codigo_autorizacion LIKE "%{transaction.numero_autorizacion}%"''').fetchall()
    
    
    
    if len(matching_transactions) == 1:
          
        matching_transactions = pd.DataFrame(matching_transactions, columns=['numero_autorizacion', 'transaccion_id', 'orden_id', 'usuario_id', 'tarjeta_credito_id'])
        matching_transactions['numero_autorizacion'] = transaction.numero_autorizacion
        matching_transactions = [tuple(row) for row in matching_transactions.itertuples(index=False, name=None)]
        
        return matching_transactions
        
    elif len(matching_transactions)> 1:
        
        matching_transactions = cursor.execute(f'''
                                    SELECT codigo_autorizacion, transaccion_id, orden_id, usuario_id, tarjeta_credito_id FROM
                                    (SELECT MIN(ABS(valor_transaccion_moneda_local-{transaction.valor}),ABS(valor_transaccion_usd-{transaction.valor})) AS price_difference, 
                                    codigo_autorizacion, transaccion.transaccion_id, orden_id, usuario_id, transaccion.tarjeta_credito_id FROM transaccion 
                                    LEFT OUTER JOIN tarjeta_credito ON transaccion.tarjeta_credito_id = tarjeta_credito.tarjeta_credito_id 
                                    LEFT OUTER JOIN transaccion_montos_adicionales ON transaccion.transaccion_id = transaccion_montos_adicionales.transaccion_id
                                    WHERE codigo_autorizacion LIKE "%{transaction.numero_autorizacion}%"
                                    ORDER BY price_difference LIMIT 1)''').fetchall()

        matching_transactions = pd.DataFrame(matching_transactions, columns=['numero_autorizacion', 'transaccion_id', 'orden_id', 'usuario_id', 'tarjeta_credito_id'])
        matching_transactions['numero_autorizacion'] = transaction.numero_autorizacion
        matching_transactions = [tuple(row) for row in matching_transactions.itertuples(index=False, name=None)]
        
        return matching_transactions
    
    else:
        
        lower_date = change_type(transaction.fecha_transaccion - timedelta(days=2))
        upper_date = change_type(transaction.fecha_transaccion + timedelta(days=2))

        first_visible_numbers = transaction.numero_visible[:6]
        last_visible_numbers = transaction.numero_visible[-4:]
    
        
        matching_transactions = cursor.execute(f'''SELECT codigo_autorizacion, transaccion.transaccion_id, orden_id, usuario_id, transaccion.tarjeta_credito_id FROM transaccion 
                                    LEFT OUTER JOIN tarjeta_credito ON transaccion.tarjeta_credito_id = tarjeta_credito.tarjeta_credito_id 
                                    LEFT OUTER JOIN transaccion_montos_adicionales ON transaccion.transaccion_id = transaccion_montos_adicionales.transaccion_id
                                    WHERE (transaccion.fecha_creacion BETWEEN {lower_date} AND {upper_date}
                                    AND numero_visible LIKE "{first_visible_numbers}%" AND numero_visible LIKE "%{last_visible_numbers}"
                                    AND valor_transaccion_moneda_local BETWEEN {lower_value} AND {upper_value}) 
                                    OR (transaccion.fecha_creacion BETWEEN {lower_date} AND {upper_date}
                                    AND numero_visible LIKE "{first_visible_numbers}%" AND numero_visible LIKE "%{last_visible_numbers}"
                                    AND valor_transaccion_usd BETWEEN {lower_value} AND {upper_value})''').fetchall()
        
        if len(matching_transactions) == 1:
            
            matching_transactions = pd.DataFrame(matching_transactions, columns=['numero_autorizacion', 'transaccion_id', 'orden_id', 'usuario_id', 'tarjeta_credito_id'])
            matching_transactions['numero_autorizacion'] = transaction.numero_autorizacion
            matching_transactions = [tuple(row) for row in matching_transactions.itertuples(index=False, name=None)]
        
            
            return matching_transactions
        
        elif len(matching_transactions)> 1:
            
            matching_transactions = cursor.execute(f'''SELECT codigo_autorizacion, transaccion_id, orden_id, usuario_id, tarjeta_credito_id FROM
                                    (SELECT MIN(ABS(valor_transaccion_moneda_local-{transaction.valor}),ABS(valor_transaccion_usd-{transaction.valor})) AS price_difference,
                                    codigo_autorizacion, transaccion.transaccion_id, orden_id, usuario_id, transaccion.tarjeta_credito_id FROM transaccion 
                                    LEFT OUTER JOIN tarjeta_credito ON transaccion.tarjeta_credito_id = tarjeta_credito.tarjeta_credito_id 
                                    LEFT OUTER JOIN transaccion_montos_adicionales ON transaccion.transaccion_id = transaccion_montos_adicionales.transaccion_id
                                    WHERE (transaccion.fecha_creacion BETWEEN {lower_date} AND {upper_date}
                                    AND numero_visible LIKE "{first_visible_numbers}%" AND numero_visible LIKE "%{last_visible_numbers}"
                                    AND valor_transaccion_moneda_local BETWEEN {lower_value} AND {upper_value}) 
                                    OR (transaccion.fecha_creacion BETWEEN {lower_date} AND {upper_date}
                                    AND numero_visible LIKE "{first_visible_numbers}%" AND numero_visible LIKE "%{last_visible_numbers}"
                                    AND valor_transaccion_usd BETWEEN {lower_value} AND {upper_value})
                                    ORDER BY price_difference LIMIT 1)''').fetchall()

            matching_transactions = pd.DataFrame(matching_transactions, columns=['numero_autorizacion', 'transaccion_id', 'orden_id', 'usuario_id', 'tarjeta_credito_id'])
            matching_transactions['numero_autorizacion'] = transaction.numero_autorizacion
            matching_transactions = [tuple(row) for row in matching_transactions.itertuples(index=False, name=None)]
        
            return matching_transactions

# Call the main function to analyze transactions
analyze_transactions()

# Close cursor and connection
cursor.close()
connection.close()
