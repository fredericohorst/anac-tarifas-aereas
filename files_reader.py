import os
import pandas as pd


# importando as séries de tarifas dos arquivos da ANAC:
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
#     print(path+file)
    import_data.append(pd.read_csv(path+file, ';'))

tarifas_df = pd.concat(import_data)

# corrigindo a tarifa para float:
tarifas_df['TARIFA'] = tarifas_df.TARIFA.str.replace(',','.').astype(float)
# adicionando coluna com o período (ano + mes)
tarifas_df['PERIODO'] = tarifas_df.ANO.astype(str)+tarifas_df.MES.astype(str).str.zfill(2)
# ordenando
tarifas_df = tarifas_df.sort_values(['PERIODO'])

tarifas_df.head()


periodo = bkp_tarifas_df.PERIODO.unique()

# ajustando repeticoes do dataset e salvando os arquivos ajustados:

bkp_tarifas_df = tarifas_df.copy()

for i in periodo:
    print(i)
    df = bkp_tarifas_df[bkp_tarifas_df.PERIODO == i]
    repeated_fields = df.loc[df.index.repeat(df.ASSENTOS)]
    repeated_fields.to_csv('tarifas_df_ajustada/tarifas_df' + i + '.csv')
    df = None
    repeated_fields = None
    print('tarifas_df_ajustada/tarifas_df' + i + '.csv -- saving complete')
    
