# no need for this.


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


