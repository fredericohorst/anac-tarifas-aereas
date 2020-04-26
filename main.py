# import sys
# import os
import logging
import os
import pandas as pd
import files_processor
import statistics_processor

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(funcName)s => %(message)s')

logging.info('iniciando')
if __name__ == '__main__':
    try:
        if len(os.listdir('tarifas_df_ajustada')) > 2:
            logging.info('arquivos encontrados: ' + str(len(os.listdir('tarifas_df_ajustada'))))
            logging.info('iniciando cálculos estatísticos')
            aeroportos = pd.read_csv('aeroportos.csv',';')
            origens = aeroportos.ICAO.unique() #['SBCT','SBRJ'] # aeroportos.ICAO.unique()
            destinos = aeroportos.ICAO.unique() #['SBCT','SBRJ'] # aeroportos.ICAO.unique()
            results = []
            for origem in origens:
                for destino in destinos:
                    if origem != destino:
                        logging.info('iniciando biblioteca de importação dos dados')
                        logging.info('origem: ' + origem , 'destino: ' + destino)
                        tarifas_df = statistics_processor.importing_data(source_path='tarifas_df_ajustada',origem_icao=origem, destino_icao=destino)
                        logging.info('dados importados: ', len(tarifas_df))
                        logging.info('iniciando biblioteca de cálculos estatísticos')
                        df_sts = statistics_processor.apllying_basic_statistics(dataframe=tarifas_df, origem_icao=origem, destino_icao=destino, bs_size=1000)
                        logging.info('cálculos terminados')
                        tarifas_df = None
                        results.append(df_sts)
                        df_sts = None
                    else:
                        pass
            logging.info('salvando arquivo')
            df_tarifas_sts = pd.concat(results)
            df_tarifas_sts.to_csv('df_tarifas_sts.csv')
            logging.info('arquivo df_tarifas_sts.csv salvo com sucesso')
            results = None
        else:
            logging.info('iniciando limpeza dos dados')
            files_processor.files_cleaning(path_source='csv_files_from_anac', path_destination='tarifas_df_ajustada')
    except Exception as erro:
        print('unexpected error: ')
        print(erro)
        logging.error(erro)