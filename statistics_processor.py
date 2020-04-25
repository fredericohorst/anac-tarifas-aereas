
import os
import numpy as np
import pandas as pd 
import basic_statistics

# importando o dicionário de dados dos aeroportos:
# aeroportos = pd.read_csv('aeroportos.csv',';')

# lista_icao = aeroportos.ICAO.unique()

def importing_data(source_path, origem_icao, destino_icao):
    """
    FUNCTION TO IMPORT THE ALREADY CLEANED FILES
    IT IMPORTS PER FLIGHT ORIGIN AND DESTINATION
    RETURNS TARIFFS DATAFRAME
    """

    print('###########################################################################')
    print('INICIANDO O PROCESSO DE IMPORTAÇÃO DOS DADOS')
    
    # importando as séries de tarifas aéreas já ajustadas:
    files = os.listdir(source_path)
    # listando os arquivos com o nome correto dentro da lista de arquivos
    files_list = []
    for file in files:
        if len(file) == 21:
            files_list.append(file)
        else:
            pass

    import_data = []

    for file in files_list:
        temp_df = pd.read_csv(source_path+'/'+file, ',')
        # filtrando apenas as cidades selecionadas:
        temp_df = temp_df[temp_df['ORIGEM'] == origem_icao]
        temp_df = temp_df[temp_df['DESTINO'] == destino_icao]
        # ajustando campo do período para string
        temp_df['PERIODO'] = temp_df.PERIODO.astype(str)
        import_data.append(temp_df)
        temp_df = None
    print('importação concluída.')
    print('criando dataframe...')
    # criando dataframe
    tarifas_df = pd.concat(import_data)

    print('concluído')
    
    print('IMPORTAÇÃO CONCLUÍDA')
    print('###########################################################################')

    return tarifas_df


def apllying_basic_statistics(dataframe, origem_icao, destino_icao, bs_size):
    """
    FUNCTION TO CALCULATE THE STATISTICS:
        - FREQUENCY
        - MEAN
        - MEAN CONFIDENCE INTERVAL
    RETURNS GROUPED STATISTIC DATAFRAME
    """
    
    print('###########################################################################')
    print('INICIANDO OS CÁLCULOS DE ESTATÍSTICA BÁSICA')

    # calculando o volume de passagens vendidas
    print('calculando o volume de passagens vendidas')
    tarifas_count = dataframe.copy()
    tarifas_count = tarifas_count.groupby(['ANO','MES','PERIODO','ORIGEM','DESTINO']).count().reset_index()
    tarifas_count = tarifas_count[['ANO','MES','PERIODO','ORIGEM','DESTINO','TARIFA']]
    tarifas_count.columns = ['ANO','MES','PERIODO','ORIGEM','DESTINO','FREQUÊNCIA']
    # calculando a tarifa média simples 
    print('calculando a tarifa média simples')
    tarifas_sts = dataframe.copy()
    tarifas_sts = tarifas_sts.groupby(['ANO','MES','PERIODO','ORIGEM','DESTINO']).mean().reset_index()
    tarifas_sts = tarifas_sts[['ANO','MES','PERIODO','ORIGEM','DESTINO','TARIFA']]
    tarifas_sts.columns = ['ANO','MES','PERIODO','ORIGEM','DESTINO','TARIFA MÉDIA']
    tarifas_sts = pd.merge(tarifas_sts,tarifas_count, how = 'left', on = ['ANO','MES','PERIODO','ORIGEM','DESTINO'])
    tarifas_count = None

    # criando listas de dados para colocar no dataframe
    tarifa_media_percentil_025 = []
    tarifa_media_percentil_500 = []
    tarifa_media_percentil_975 = []
    periodos = tarifas_sts.PERIODO.unique()
    
    # calculando o intervalo de confiança da média da tarifa: ######
    print('calculando o intervalo de confiança da média da tarifa')
    for periodo in periodos:
        df = dataframe.copy()
        df = df[df.PERIODO == periodo]
        ic_tarifa_media = basic_statistics.draw_bs_reps(df.TARIFA, np.mean, size = bs_size)
        print('ics calculados para o período de ' + str(periodo))
        tarifa_media_percentil_025.append(np.percentile(ic_tarifa_media, 2.5))
        tarifa_media_percentil_975.append(np.percentile(ic_tarifa_media, 97.5))
        tarifa_media_percentil_500.append(np.percentile(ic_tarifa_media, 50.0)) # mediana
        df = None

    tarifas_sts['TARIFA MÉDIA - PERCENTIL 2,5'] = tarifa_media_percentil_025
    tarifas_sts['TARIFA MÉDIA - PERCENTIL 50'] = tarifa_media_percentil_500
    tarifas_sts['TARIFA MÉDIA - PERCENTIL 97,5'] = tarifa_media_percentil_975

    print('CÁLCULOS CONCLUÍDOS')
    print('###########################################################################')

    return tarifas_sts

