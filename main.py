# import sys
# import os
import logging
import files_processor

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(funcName)s => %(message)s')

####### nao cria o dataset.
if __name__ == '__main__':
    try:
        files_processor.files_cleaning(path_source='csv_files_from_anac', path_destination='tarifas_df_ajustada')
    except Exception as erro:
        print('unexpected error: ')
        print(erro)
        logging.error(erro)