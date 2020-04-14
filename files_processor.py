##################################################################
# objetivo: limpar e ajustar os arquivos necessários para análise
##################################################################

import os
import pandas as pd

# importando as séries de tarifas dos arquivos da ANAC:
# definir o caminho para a pasta dos arquivos:
path = 'csv_files_from_anac/' 
files = os.listdir(path)

files_list = []

for file in files:
    if len(file) == 10:
        files_list.append(file)
    else:
        pass

import_data = []
# pd.DataFrame(data=None, columns=columns)

for file in files_list:
    print('#########################')
    print(path+file)
    file_df = pd.read_csv(path+file, ';')
    # corrigindo a tarifa para float:
    file_df['TARIFA'] = file_df.TARIFA.str.replace(',','.').astype(float)
    print('tarifa corrigida para float')
    # adicionando coluna com o período (ano + mes)
    file_df['PERIODO'] = file_df.ANO.astype(str)+file_df.MES.astype(str).str.zfill(2)
    print('coluna PERIODO adicionada com sucesso')
    # desagrupando os valores por assento:
    file_df = file_df.loc[file_df.index.repeat(file_df.ASSENTOS)]
    # excluindo coluna ASSENTOS:
    file_df = file_df[['PERIODO','ANO','MES','EMPRESA', 'ORIGEM','DESTINO','TARIFA']]
    print('ajustes finalizados com sucesso')
    # gravando arquivo ajustado:
    file_df.to_csv('tarifas_df_ajustada/tarifas_df' + file + '.csv')
    file_df = None
    print('arquivo salvo com sucesso na pasta tarifas_df_ajustada')
    

    # import_data.append(pd.read_csv(path+file, ';'))
