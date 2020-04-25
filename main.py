# import sys
# import os
import logging
import os
import pandas as pd
import files_processor
import statistics_processor

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(funcName)s => %(message)s')

####### nao cria o dataset.
if __name__ == '__main__':
    try:
        if len(os.listdir('tarifas_df_ajustada')) > 0:
            aeroportos = pd.read_csv('aeroportos.csv',';')
            origens = ['SBCT','SBRJ'] # aeroportos.ICAO.unique()
            destinos = ['SBCT','SBRJ'] # aeroportos.ICAO.unique()
            results = []
            for origem in origens:
                for destino in destinos:
                    if origem != destino:
                        tarifas_df = statistics_processor.importing_data(source_path='tarifas_df_ajustada',origem_icao=origem, destino_icao=destino)
                        df_sts = statistics_processor.apllying_basic_statistics(dataframe=tarifas_df, origem_icao=origem, destino_icao=destino, bs_size=100)
                        tarifas_df = None
                        df_sts = None
                        results.append(df_sts)
                    else:
                        pass
            df_tarifas_sts = pd.concat(results)
            df_tarifas_sts.to_csv('df_tarifas_sts')
            results = None
        else:
            files_processor.files_cleaning(path_source='csv_files_from_anac', path_destination='tarifas_df_ajustada')
    except Exception as erro:
        print('unexpected error: ')
        print(erro)
        logging.error(erro)