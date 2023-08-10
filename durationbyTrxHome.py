import pandas as pd
import os
import re
from datetime import datetime
from pathlib import Path
import logging

def setup_logging():
    logging.basicConfig(filename='/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/result/logDurationHome.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('--- Starting script ---')

def process_log_line(line):
    """
    This function processes a log line and extracts relevant details.
    """
    # Regular expression for parsing the log line
    pattern = r"\[(?P<timestamp>.+?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    
    # Regular expression for finding transaction ID and priority
    transaction_pattern = r"transaction:([^ ]*)"
    priority_pattern = r"pri:(\d+)"
    
    # Parse the log line
    match = re.match(pattern, line)
    
    if match:
        # Extract details
        details = match.groupdict()
        
        # Find transaction ID if present
        transaction_match = re.search(transaction_pattern, details['details'])
        if transaction_match:
            details['transaction_id'] = transaction_match.group(1)
        else:
            details['transaction_id'] = None
        
        # Find priority if present
        priority_match = re.search(priority_pattern, details['details'])
        if priority_match:
            details['priority'] = int(priority_match.group(1))
        else:
            details['priority'] = -1
        
        # Convert timestamp to datetime object
        details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S")
        
        return details

    return None

def process_log_file(file_path):
    """
    This function processes a log file and extracts transaction details.
    """
    global all_transactions_df

    # Read the log file
    with open(file_path, 'r') as file:
        log_lines = file.readlines()

    # Process each log line and store the results in a list
    log_data = [process_log_line(line) for line in log_lines if process_log_line(line) is not None]

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(log_data)

    # Drop rows without transaction_id
    df = df.dropna(subset=['transaction_id'])

    # Sort DataFrame by timestamp
    df = df.sort_values('timestamp')

    # Define a dictionary to store transaction details
    transactions = {}

    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        # Get transaction details
        transaction_id = row['transaction_id']
        timestamp = row['timestamp']
        action = row['action']
        subcomponent = row['subcomponent']
        priority = row['priority']

        # Update transaction details
        if transaction_id not in transactions:
            # If the transaction is not in the dictionary, add it
            transactions[transaction_id] = {
                'date_min': timestamp,
                'date_max': timestamp,
                'priority': priority if priority != -1 else None,
                'first_action': action,
                'last_action': action,
                'first_subcomponent': subcomponent,
                'last_subcomponent': subcomponent,
                'send': False,  # Whether the SEND action has been encountered
                'newtrans': False  # Whether the NEWTRANS or MNEWTRANS action has been encountered
            }
        else:
            # If the transaction is in the dictionary, update it
            transaction = transactions[transaction_id]

            # Update date_min, first_action, and first_subcomponent if NEWTRANS or MNEWTRANS is encountered
            if action in ['NEWTRANS', 'MNEWTRANS']:
                transaction['date_min'] = timestamp
                transaction['first_action'] = action
                transaction['first_subcomponent'] = subcomponent
                transaction['newtrans'] = True

            # Update date_max, last_action, and last_subcomponent only if SEND has not been encountered
            if not transaction['send']:
                transaction['date_max'] = timestamp
                transaction['last_action'] = action
                transaction['last_subcomponent'] = subcomponent

            # Update priority if it has not been set
            if transaction['priority'] is None and priority != -1:
                transaction['priority'] = priority

            # Update SEND status
            if action in ['SEND','REJECTED']:
                transaction['send'] = True

    # Convert the dictionary to a DataFrame
    transactions_df = pd.DataFrame(transactions.values(), index=transactions.keys())
    transactions_df.index.name = 'Transaction ID'

    # Calculate duration in seconds
    transactions_df['Duration'] = (transactions_df['date_max'] - transactions_df['date_min']).dt.total_seconds()

    # Replace NaN priorities with -1
    transactions_df['priority'] = transactions_df['priority'].fillna(-1)

    # Rearrange columns
    transactions_df = transactions_df[['date_min', 'date_max', 'priority', 'first_action', 'last_action', 'first_subcomponent', 'last_subcomponent', 'Duration']]

    # Append to the main DataFrame
    all_transactions_df = pd.concat([all_transactions_df, transactions_df])

def process_log_files(directory_path, file_pattern):
    """
    This function processes all log files in a directory and its subdirectories that match a given pattern.
    """
    logging.info('Starting processing log Files...')
    # Get the directory path
    directory_path = Path(directory_path)
    
    # Get a list of all files that match the patter
    matching_files = list(directory_path.rglob(file_pattern))
    logging.info(f'File search results: {len(matching_files)} files...')
    count = 0


    # Iterate over each file in the directory and its subdirectories
    for file_path in directory_path.rglob(file_pattern):
        # Process the log file
        process_log_file(file_path)
        count+=1
        if count % 10 == 0:
            logging.info(f'Ha terminado de procesar {count} archivos...')
        

setup_logging()
# Initialize a DataFrame to store the results from all files
all_transactions_df = pd.DataFrame()

# Process all log files in the given directory and its subdirectories that match the given pattern
# Call the function with your directory path and file pattern
process_log_files("/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/SCL", "Limsp_act_*.log")
logging.info('Processing completed...')

logging.info('Guardando resultado...')
# Save the DataFrame to a CSV file
all_transactions_df.to_csv("/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/result/resultallNodes1.1.csv")
logging.info('Resultado almacenado')
