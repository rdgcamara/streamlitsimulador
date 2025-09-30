import pandas as pd
import yfinance as yf
import datetime

def carregar_empresas():
    empresas = pd.read_csv('ativos_b3.csv')
    empresas = empresas['symbol']
    empresas_filtradas = empresas[~empresas.str.contains(r"F\.SA$")]
    return empresas_filtradas

def carregar_dados():
    ontem = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    tickers = carregar_empresas()
    
    all_data = []

    for ticker in tickers:
        ativo = yf.Ticker(ticker)

        # histórico de preços de fechamento
        hist = ativo.history(start='2000-01-01', end=ontem)
        if hist.empty:
            continue
        df_close = hist[['Close']].copy()
        df_close.reset_index(inplace=True)  # cria coluna Date
        df_close['Ticker'] = ticker

        # dividendos
        divs = ativo.dividends
        df_divs = divs.to_frame(name='Dividends')
        df_divs.reset_index(inplace=True)  # cria coluna Date
        df_divs['Ticker'] = ticker

        # juntar fechamento e dividendos por data e ticker
        df = pd.merge(df_close, df_divs, on=['Date','Ticker'], how='left')
        # preencher dividendos ausentes com 0
        df['Dividends'] = df['Dividends'].fillna(0.0)

        all_data.append(df)

    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        # salvar em parquet
        df_final.to_parquet('dados_dividendos.parquet', index=False)


    else:
        print("Nenhum dado baixado.")


def carregar_tickers():
    ontem = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    empresas = carregar_empresas()
    texto_tickers = " ".join(empresas)
    cotacao_acao = yf.download(
            texto_tickers, 
            start="2000-01-01",
            end=datetime.datetime.now().strftime("%Y-%m-%d"),
            progress=True,
            group_by='ticker'
        )
    fechamento = cotacao_acao.xs("Close", axis=1, level=1)
    fechamento.to_parquet('dados_close.parquet')

if __name__ == "__main__":
    carregar_dados()    
    carregar_tickers()