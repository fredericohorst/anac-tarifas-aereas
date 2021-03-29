##################################################################
# objetivo: limpar e ajustar os arquivos necessários para análise
##################################################################

import os
# import logging

import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]

# Enable Arrow-based columnar data transfers
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder \
        .master("local") \
        .appName("anac-prices") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sqlContext = SQLContext(spark)

# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(levelname)s %(funcName)s => %(message)s')

def files_cleaning(path_source,inflation_file): #, path_destination):
    """
    FUNCTION TO CLEAN THE FILES DATA, PERFORMING:
        - TARIFA AS FLOAT AND WITH . AS DECIMAL SEPARATOR;
        - YEAR AND MONTH IN ONE SINGLE COLUMN ('PERIODO');
        - UNGROUPING DATAFRAME FOR THE QUANTITY OF SEATS SOLD;
        - DEFLATING PRICES WITH CONSUMER INDEX (IPCA series) TO 12/1993=100.
    """
    print('###########################################################################')
    print('BEGGINING DATA CLEANING PROCESS')

    # listing files inside the path_source
    files = os.listdir(path_source)
    # selecting only the files with the correct name inside the path_source
    files_list = []
    for file in files:
        if len(file) == 10:
            files_list.append(file.strip('.CSV'))
        else:
            pass


    # reading IPCA inflation series:
    print('importing IPCA file')
    ipca = sqlContext.read.csv(inflation_file, sep=";", inferSchema="true", header="true")
    ipca.registerTempTable('ipca')
    ipca_handling = """
        SELECT
            CAST(`Período` AS STRING) AS year_month, 
            `Ano` AS year,
            `Mês` AS month,
            `Número Índice (Dez/93 = 100)` AS ipca_index,
            `Variação no Mês` AS var_month,
            `Variação 3 Meses` AS var_3_months,
            `Variação 6 Meses` AS var_6_months, 
            `Variação no Ano` AS var_year,
            `Variação 12 Meses` AS var_12_months
        FROM ipca
        """
    ipca = sqlContext.sql(ipca_handling)
    ipca = ipca.filter(ipca.year_month.isin(files_list))
    print('Inflation series imported successfully')

    # reading, correcting data and saving results in other file
    print('#########################')
    print('beginning data cleaning')

    path_files_list = []
    for file in files_list:
        path = path_source+'/'+file+'.CSV'
        path_files_list.append(path)

    anac_source = sqlContext.read.csv(path_files_list,sep=";", inferSchema="true", header="true")
    anac_source.registerTempTable('anac_source')
    anac_handling1 = """
        SELECT 
            ANO AS year,
            MES AS month,
            CONCAT(CAST(ANO AS STRING), LPAD(CAST(MES AS STRING),2, '0')) AS year_month,
            EMPRESA AS company,
            ORIGEM AS origin,
            DESTINO AS destination,
            CAST(REPLACE(TARIFA, ',', '.') AS FLOAT) AS tariff,
            ASSENTOS AS seats
        FROM anac_source
        """
    anac_handling1 = sqlContext.sql(anac_handling1)

    anac_handling1.registerTempTable('anac_handling1')
    ipca.registerTempTable('ipca')

    anac_handling2 = """
        SELECT 
            anac_handling1.*,
            anac_handling1.tariff * 100 / ipca.ipca_index AS deflated_tariff
        FROM anac_handling1
        LEFT JOIN ipca ON ipca.year_month = anac_handling1.year_month
        """

    anac_table = sqlContext.sql(anac_handling2)

    print('THIS IS THE END')
    print('###########################################################################')
    return anac_table