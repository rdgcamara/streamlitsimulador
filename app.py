import streamlit as st
import pandas as pd
import datetime


st.set_page_config(layout="wide", page_title="Simulador de Ações")


@st.cache_data
def load_parquet(path):
    df = pd.read_parquet(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    return df

DF = load_parquet("dados_close.parquet")
DF = DF.reindex(sorted(DF.columns), axis=1)

meta = pd.read_csv("ativos_b3.csv")

meta["symbol"] = meta["symbol"].astype(str)
meta = meta[~meta["symbol"].str.contains(r"F\.SA$")]
meta["symbol_clean"] = meta["symbol"].str.replace(".SA", "", regex=False)
meta["display"] = meta["symbol"] + " - " + meta["name"].fillna("")
display_to_symbol = dict(zip(meta["display"], meta["symbol"]))
opcoes_display = sorted(display_to_symbol.keys())
pd.set_option('display.precision', 2)

tickers = list(DF.columns)
min_date = DF.index.min().date()
max_date = DF.index.max().date()

if "step" not in st.session_state:
    st.session_state["step"] = 1
if "selected" not in st.session_state:
    st.session_state["selected"] = []
if "start_date" not in st.session_state:
    st.session_state["start_date"] = None
if "end_date" not in st.session_state:
    st.session_state["end_date"] = None
if "investments" not in st.session_state:
    st.session_state["investments"] = {}

def go_to(step):
    st.session_state["step"] = step



if st.session_state["step"] >= 1:

    container = st.container(border=True)

    container.markdown("<h1 style='text-align: center;'>Simulador Ações</h1>", unsafe_allow_html=True)

    container.markdown("<h5 style='text-align: center;'>Está página tem objetivo de simluar uma carteira de ações por um período de tempo</h5>", unsafe_allow_html=True)

    selecionados = container.multiselect("Escolha as ações", options=opcoes_display, placeholder="Ações", key="ui_selected")

    selecionados_tickers = [display_to_symbol[d] for d in selecionados]

    col1, col2 = container.columns(2)
    with col1:
        start_date = st.date_input(
            "Data inicial da simulação",
            min_value=min_date,
            max_value=max_date,
            format="DD/MM/YYYY",
            value=max_date - datetime.timedelta(days=365),
            key="ui_start"
        )
    with col2:
        end_date = st.date_input(
            "Data final da simulação",
            min_value=min_date,
            max_value=max_date,
            format="DD/MM/YYYY",
            value=max_date,
            key="ui_end"
        )
    
    if container.button("Proximo"):
        if not selecionados:
            st.warning("Selecione pelo menos uma ação para continuar.")
        elif start_date > end_date:
            st.warning("Data início não pode ser maior que a data fim.")
        else:
            st.session_state["selected"] = selecionados_tickers
            st.session_state["start_date"] = start_date
            st.session_state["end_date"] = end_date
            go_to(2)

if st.session_state["step"] >= 2:

    with st.form("form_investments"):

        container = st.container(border=True)

        container.markdown("<h3 style='text-align: center;'>Valores investidos nas ações</h3>", unsafe_allow_html=True)

        selecionados = st.session_state["selected"]

        max_cols = 3
        n = len(selecionados)

        cols_per_row = min(max_cols, n)

        inputs = {}

        for i, ativo in enumerate(selecionados):

            col_idx = i % cols_per_row
            if col_idx == 0:
                row_cols = container.columns(cols_per_row)
            with row_cols[col_idx]:
                default_val = float(st.session_state["investments"].get(ativo, 0.0))

                key = f"form_inv_{ativo}"
                inputs[ativo] = st.number_input(
                            label=f"{ativo}",
                            min_value=0.0,
                            value=default_val,
                            step=100.0,
                            format="%.2f",
                            key=key
                        )       
        submitted = container.form_submit_button("Avançar")

        if submitted:
            for ativo, val in inputs.items():
                st.session_state["investments"][ativo] = float(val)
            go_to(3)

if st.session_state["step"] >= 3:
    
    import numpy as np  # usado para caso precise np.nan

    container = st.container(border=True)

    selecionados = st.session_state["selected"]
    start_date = st.session_state["start_date"]
    end_date = st.session_state["end_date"]

    df_sel = DF.loc[start_date:end_date, selecionados].copy()

    df_plot = df_sel.copy()
    if len(selecionados) == 1:
        df_plot = df_plot.rename(columns={selecionados[0]: "Close"})

    container.markdown("<h2 style='text-align: center;'>Grafico das ações</h2>", unsafe_allow_html=True)
    container.line_chart(df_plot)

    container.markdown("<h2 style='text-align: center;'>Resultados das ações</h2>", unsafe_allow_html=True)

    # carregar dividendos
    df_divs = pd.read_parquet("dados_dividendos.parquet")
    df_divs['Date'] = pd.to_datetime(df_divs['Date']).dt.tz_localize(None)  # remove timezone
    df_divs = df_divs[
        (df_divs['Date'] >= pd.to_datetime(start_date)) &
        (df_divs['Date'] <= pd.to_datetime(end_date)) &
        (df_divs['Ticker'].isin(selecionados))
    ]

    rows = []
    total_investido = 0.0
    total_atual = 0.0
    total_dividends = 0.0
    total_qtde = 0  # soma das quantidades

    for ativo in selecionados:
        serie = df_sel[ativo].dropna()
        if serie.empty:
            preco_inicio = 0.0
            preco_fim = 0.0
        else:
            preco_inicio = float(serie.iloc[0])
            preco_fim = float(serie.iloc[-1])

        investido = float(st.session_state["investments"].get(ativo, 0.0))

        # quantidade inteira de ações compradas no início (arredonda para baixo)
        if preco_inicio > 0 and investido > 0:
            qtde = int(investido // preco_inicio)
        else:
            qtde = 0

        total_qtde += qtde  # acumula no total

        # variação percentual do ativo baseada em preço
        perf_pct = (preco_fim / preco_inicio - 1) if preco_inicio != 0 else 0.0

        if investido <= 0:
            valor_atual = 0.0
            lucro = 0.0
            lucro_pct_display = perf_pct
        else:
            valor_atual = qtde * preco_fim
            lucro = valor_atual - investido
            lucro_pct_display = lucro / investido if investido != 0 else 0.0

            # arredondar valores
            valor_atual = round(valor_atual, 2)
            lucro = round(lucro, 2)
            lucro_pct_display = round(lucro_pct_display, 4)

            total_investido += investido
            total_atual += valor_atual

        # calcular dividendos no período
        divs_por_ativo = df_divs.loc[df_divs['Ticker'] == ativo, 'Dividends'].sum()
        div_total = float(divs_por_ativo) * qtde
        div_total = round(div_total, 2)
        total_dividends += div_total

        rows.append({
            "Ativo": ativo,
            "Quantidade": int(qtde),
            "Valor Inicial (R$)": round(investido, 2),
            "Lucro (%)": lucro_pct_display,
            "Lucro (R$)": lucro,
            "Valor Atual (R$)": valor_atual,
            "Dividendos (R$)": div_total
        })

    # calcular totais e percentuais da carteira
    total_lucro = total_atual - total_investido
    total_lucro_pct = total_lucro / total_investido if total_investido != 0 else 0.0

    # montar DataFrame
    df_result = pd.DataFrame(rows).set_index("Ativo")

    # linha TOTAL
    total_row = {
        "Quantidade": total_qtde,  # agora soma das ações
        "Valor Inicial (R$)": round(total_investido, 2),
        "Lucro (%)": round(total_lucro_pct, 4),
        "Lucro (R$)": round(total_lucro, 2),
        "Valor Atual (R$)": round(total_atual, 2),
        "Dividendos (R$)": round(total_dividends, 2)
    }
    df_result.loc["TOTAL"] = total_row

    # Formatação segura com funções
    def fmt_quantidade(x):
        if pd.isna(x):
            return ""
        try:
            return f"{int(x):d}"
        except Exception:
            return ""

    def fmt_moeda(x):
        if pd.isna(x):
            return ""
        try:
            return f"R${float(x):,.2f}"
        except Exception:
            return ""

    def fmt_percent(x):
        if pd.isna(x):
            return ""
        try:
            return f"{float(x):.2%}"
        except Exception:
            return ""

    def color_pos_neg(x, **kwargs):
        try:
            v = float(x)
        except Exception:
            return ""
        if v > 0:
            return "color: green;"
        if v < 0:
            return "color: red;"
        return ""

    df_styled = df_result.style.format({
        "Quantidade": fmt_quantidade,
        "Valor Inicial (R$)": fmt_moeda,
        "Lucro (R$)": fmt_moeda,
        "Valor Atual (R$)": fmt_moeda,
        "Dividendos (R$)": fmt_moeda,
        "Lucro (%)": fmt_percent
    }).applymap(
        color_pos_neg,
        subset=["Lucro (R$)", "Lucro (%)", "Dividendos (R$)"]
    )

    container.dataframe(df_styled, use_container_width=True)

    container.write("*A quantidade mostra quantas ações foram compradas na data inicial  \n" \
    "*O lucro % mostra quanto subiu ou caiu no periodo selecionado  \n" \
    "*O lucro R$ mostra quantos reais subiu ou caiu da ação  \n" \
    "*O Valor atual mostra o valor inicial mais lucro ou prejuizo  \n" \
    "*O Dividendo mostra o valor pago pelas ações no periodo de tempo selecionado")
