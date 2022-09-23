import yfinance as yf
from datetime import date
import logging as log
import requests, os, sys
from google.cloud import bigquery
from config_ibge import *
import pandas as pd

def config_log():
    """
    Configure logging level and output format
    """

    logging_level = 20
    log.basicConfig(
        level=logging_level,
        format='[%(asctime)s.%(msecs)03d] %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

def save_csv(df, path):
    print(f'Transform and save data into {path}')
    df.to_csv(
        path,
        #mode = 'a',
        index = False,
        header = True
        )
    return f'Data was save in {path}'

def get_closes_yf(tickers, date_begin):
    """
    Get list of index and gold prices at close in US$

    Returns:
        closes {dataframe}
    """
    closes = yf.download(
        tickers,
        date_begin,
        date.today().strftime("%Y-%m-%d"), 
        auto_adjust=True
        )['Close']
    closes.reset_index(inplace = True)

    return closes

def get_price_index():
    """
    Makes several requests to the API updating the offset.

    Arguments:
        token {string} -- the API access token

    Returns:
        all_leads {list of dict} -- list of all leads from the day before
            contains the preselected fields in dict form
    """
    inpc_report = requests.get(
        url=INPC_URL,
        params=INPC_PARAMS)
    ipca_report = requests.get(
        url=IPCA_URL,
        params=IPCA_PARAMS)
    all_indices = []
    if ipca_report.status_code == 200 and inpc_report.status_code == 200:
        ipca_json = ipca_report.json()
        inpc_json = inpc_report.json()
        all_indices += ipca_json
        all_indices += inpc_json
        log.info(f"Successfully retrieved data")#changed
    else:
        log.error("Something went wrong. Aborting...")
        log.error(f"IPCA Finished with status code {ipca_json.status_code}: {ipca_json.text}")
        log.error(f"INPC Finished with status code {inpc_json.status_code}: {inpc_json.text}")
        sys.exit()
    
    return all_indices

def get_igpm_index():

    
    client = bigquery.Client(project='annular-welder-353913')
    query_job = client.query(
        """
        SELECT * FROM `basedosdados.br_fgv_igp.igp_10_mes` WHERE ano >= 2020 ORDER BY ano DESC, mes DESC
        """
    )
    igpm = query_job.to_dataframe().drop(
        columns=["indice","variacao_12_meses", "variacao_acumulada_ano", "indice_fechamento_mensal"], 
        axis=1
        ).rename(
            columns={
                "variacao_mensal": "IGP-M"
                }
                )

    return igpm
    
def transform(data):
    """
    Cleans the data, removing not wanted fields and values

    Arguments:
        data {list of dict} -- all indices retrieved from the IBGE API

    Returns:
        df_indices {dataframe} -- sanitized leads,
            ready to be uploaded to the database
    """

    log.info("Transforming leads")
    
    results_ipca = []
    ipca = data[0]["resultados"][0]["series"][0]["serie"]
    for mes in ipca:
        index = {}
        index["mes"] = mes
        index["IPCA"] = ipca[mes] 
        results_ipca.append(index)
    
    results_inpc = []
    inpc = data[1]["resultados"][0]["series"][0]["serie"]
    for mes in inpc:
        index = {}
        index["mes"] = mes
        index["INPC"] = inpc[mes] 
        results_inpc.append(index)    
    
    df_indices = pd.merge(
        pd.DataFrame.from_records(results_ipca),
        pd.DataFrame.from_records(results_inpc),
        on = ["mes"]
    )

    df_indices["ano"] = pd.to_datetime(df_indices["mes"], format="%Y%m").dt.year
    df_indices["mes"] = pd.to_datetime(df_indices["mes"], format="%Y%m").dt.month

    return df_indices

def merge_index(igpm, df_indices):

    merged_index = pd.merge(igpm, df_indices, how="right", on = ["ano", "mes"])

    return merged_index

def main():
    
    
    date_begin = "2022-01-01"
    tickers = ['^IXIC', "^BVSP", "^GSPC", "GC=F", "BRL=X", "IRFM11.SA", "IBRX"]
    closes = get_closes_yf(tickers, date_begin)

    save_csv(
        closes, 
        f"C:/Users/User/Documents/Indices_Extraction/Tabela_de_cotacoes.csv"
        )
    
    prices = get_price_index()
    igpm = get_igpm_index()
    indices = transform(
        prices
        )
    df_all = merge_index(
        igpm, 
        indices
        )

    save_csv(
        df_all, 
        f"C:/Users/User/Documents/Indices_Extraction/Tabela_de_indices.csv"
        )

    return ("ok", 200)

if __name__== "__main__":
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bq_cred.json"
    main()

