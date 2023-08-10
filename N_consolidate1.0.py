import pandas as pd
from datetime import datetime
import logging

def setup_logging():
    logging.basicConfig(filename='/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/result/N_consolidation2.0.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('--- Starting script ---')
    


def get_first_last_dates(group):

    first_date = group.iloc[0]['date_min']
    first_action = group.iloc[0]['first_action']
    first_subcomponent = group.iloc[0]['first_subcomponent']
    first_priority = group.iloc[0]['priority']
    
    last_date = group.iloc[-1]['date_max']
    last_action = group.iloc[-1]['last_action']
    last_subcomponent = group.iloc[-1]['last_subcomponent']
    
    if 'NEWTRANS' in group['first_action'].values:
        first_action_row = group[group['first_action'] == 'NEWTRANS'].iloc[0]
        first_date = first_action_row['date_min']
        first_action = first_action_row['first_action']
        first_subcomponent = first_action_row['first_subcomponent']
        first_priority = first_action_row['priority']
        
    elif 'MNEWTRANS' in group['first_action'].values:
        first_action_row = group[group['first_action'] == 'MNEWTRANS'].iloc[0]
        first_date = first_action_row['date_min']
        first_action = first_action_row['first_action']
        first_subcomponent = first_action_row['first_subcomponent']
        first_priority = first_action_row['priority']

    if 'SEND' in group['last_action'].values:
        last_action_row = group[group['last_action'] == 'SEND'].iloc[-1]
        last_date = last_action_row['date_max']
        last_action = last_action_row['last_action']
        last_subcomponent = last_action_row['last_subcomponent']
        
    elif 'REJECTED' in group['last_action'].values:
        last_action_row = group[group['last_action'] == 'REJECTED'].iloc[-1]
        last_date = last_action_row['date_max']
        last_action = last_action_row['last_action']
        last_subcomponent = last_action_row['last_subcomponent']
      

    duration = (last_date - first_date).seconds
    
    get_first_last_dates.count += 1
    
    # Si el contador es múltiplo de 5000, escribe en el log
    if get_first_last_dates.count % 50000 == 0:
        logging.info(f'Ha terminado de procesar {get_first_last_dates.count} transacciones...')  
        

    return pd.Series({
        'date_min': first_date,
        'date_max': last_date,
        'priority': first_priority,
        'first_action': first_action,
        'first_subcomponent': first_subcomponent,
        'last_action': last_action,
        'last_subcomponent': last_subcomponent,
        'Duration': duration
    })

def process_csv(input_file, output_file):
    logging.info('Starting processing...')
    data = pd.read_csv(input_file)
    logging.info(f'Finished reading input file: {input_file}')
    data['date_min'] = pd.to_datetime(data['date_min'])
    data['date_max'] = pd.to_datetime(data['date_max'])
    

    data_sorted = data.sort_values(by=['Transaction ID', 'date_min'])
    logging.info('Sorting done...')
    consolidated_data = data_sorted.groupby('Transaction ID').apply(get_first_last_dates).reset_index()
    logging.info('Groupby done...')
    '''
    escape_mapping = {
        '*': '\\*',
        '/': '\\/',
        '=': '\\=',
        '+': '\\+'
    }
 
    for char, replacement in escape_mapping.items():
        consolidated_data['Transaction ID'] = consolidated_data['Transaction ID'].str.replace(char, replacement)
    '''
    consolidated_data.to_csv(output_file, index=False)
    logging.info('Processing completed.')

if __name__ == "__main__":
    setup_logging()
    # Inicializa el contador
    get_first_last_dates.count = 0
    
    input_file_path = "/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/result/resultallNodes1.1.csv"
    output_file_path = "/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/result/N_consolidated_results1.9.csv"
    process_csv(input_file_path, output_file_path)
    logging.info('--- Script finished ---')