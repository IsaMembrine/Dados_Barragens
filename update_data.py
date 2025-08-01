# app.py
import streamlit as st
import pandas as pd
import requests
import io
import zipfile
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitoramento de Barragens", layout="wide")

# 🔐 Autenticação via secrets do Streamlit Cloud
auth = (
    st.secrets["GATEWAY_USERNAME"],
    st.secrets["GATEWAY_PASSWORD"]
)

base_url = 'https://loadsensing.wocs3.com'
node_ids = [1006, 1007, 1008, 1010, 1011, 1012]
urls = [f'{base_url}/27920/dataserver/node/view/{nid}' for nid in node_ids]

# 📥 Coleta de links
def coletar_links():
    all_file_links = {}
    for url in urls:
        try:
            r = requests.get(url, auth=auth)
            soup = BeautifulSoup(r.text, 'html.parser')
            node_id = re.search(r'/view/(\d+)$', url).group(1)
            file_links = [a['href'] for a in soup.find_all('a', href=True)
                          if a['href'].endswith(('.csv', '.zip'))]
            if file_links:
                all_file_links[node_id] = file_links
        except Exception as e:
            st.error(f"Erro ao coletar links de {url}: {e}")
    return all_file_links

# 🧲 Download dos arquivos
def baixar_arquivos(all_file_links):
    hoje = datetime.now()
    limite_data = hoje.replace(day=1)
    meses = [(limite_data.year, limite_data.month)]
    for i in range(1, 3):
        m = limite_data.month - i
        y = limite_data.year
        if m <= 0:
            y -= 1
            m += 12
        meses.append((y, m))

    downloaded_files = {}
    for node_id, links in all_file_links.items():
        downloaded_files[node_id] = []
        for link in links:
            filename = link.split('/')[-1]
            if 'current' in filename.lower():
                baixar = True
            else:
                try:
                    partes = filename.split('-')
                    ano = int(partes[-2])
                    mes = int(partes[-1].split('.')[0])
                    baixar = (ano, mes) in meses
                except:
                    continue
            if not baixar:
                continue
            full_url = base_url + link
            response = requests.get(full_url, auth=auth)
            if response.status_code == 200:
                downloaded_files[node_id].append((filename, response.content))
    return downloaded_files

# 🧹 Processamento dos arquivos
def processar_arquivos(downloaded_files):
    all_dataframes = {}
    for node_id, files in downloaded_files.items():
        dfs_node = []
        for filename, content in files:
            if 'health' in filename.lower():
                continue
            if filename.endswith('.csv'):
                try:
                    df = pd.read_csv(io.BytesIO(content), skiprows=9)
                    dfs_node.append(df)
                except:
                    continue
            elif filename.endswith('.zip'):
                try:
                    with zipfile.ZipFile(io.BytesIO(content), 'r') as zf:
                        for fn in zf.namelist():
                            if 'health' in fn.lower() or not fn.endswith('.csv'):
                                continue
                            with zf.open(fn) as f:
                                df = pd.read_csv(io.TextIOWrapper(f, 'utf-8'), skiprows=9)
                                dfs_node.append(df)
                except:
                    continue
        if dfs_node:
            df_concat = pd.concat(dfs_node, ignore_index=True)
            all_dataframes[node_id] = df_concat
    return all_dataframes

# 📊 Análise e visualização
def analisar_e_visualizar(all_dataframes):
    first_node = list(all_dataframes.keys())[0]
    todos_nos = all_dataframes[first_node].copy()
    for node_id, df in all_dataframes.items():
        if node_id != first_node and 'Date-and-time' in df.columns:
            todos_nos = pd.merge(todos_nos, df, on='Date-and-time', how='outer',
                                 suffixes=('', f'_{node_id}'))

    todos_nos['Date-and-time'] = pd.to_datetime(todos_nos['Date-and-time'], errors='coerce')
    todos_nos.dropna(subset=['Date-and-time'], inplace=True)
    todos_nos['Date'] = todos_nos['Date-and-time'].dt.date
    todos_nos['Time_Rounded'] = todos_nos['Date-and-time'].dt.round('h').dt.time

    df_cleaned = todos_nos.drop_duplicates(subset=['Date', 'Time_Rounded'])

    p_cols = [c for c in df_cleaned.columns if c.startswith('p-')]
    df_selected = df_cleaned[['Date-and-time', 'Time_Rounded'] + p_cols].copy()

    melted = df_selected.melt(
        id_vars=['Date-and-time', 'Time_Rounded'],
        value_vars=p_cols,
        var_name='Node_p_Column',
        value_name='Value'
    )
    melted.dropna(subset=['Value'], inplace=True)
    melted['Month'] = melted['Date-and-time'].dt.to_period('M')
    melted['Node_ID'] = melted['Node_p_Column'].apply(lambda x: x.split('-')[1])
    counts = melted.groupby(['Month', 'Node_ID']).size().reset_index(name='Monthly_Data_Count')
    counts['Days_in_Month'] = counts['Month'].dt.days_in_month
    counts['Max_Data'] = counts['Days_in_Month'] * 24
    counts['Monthly_Attendance_Percentage'] = (counts['Monthly_Data_Count'] / counts['Max_Data']) * 100
    monthy_selecionado = counts[['Month', 'Node_ID', 'Monthly_Attendance_Percentage']]
    monthy_selecionado['Month'] = monthy_selecionado['Month'].astype(str)

    st.subheader("📈 Presença Mensal de Dados por Nó")
    st.dataframe(monthy_selecionado)

    st.bar_chart(monthy_selecionado.pivot(index="Month", columns="Node_ID",
                                          values="Monthly_Attendance_Percentage"))

# ▶️ Interface principal
st.title("Monitoramento de Barragens 🌊")
st.write("Esse aplicativo coleta e analisa dados de sensores via Gateway Loadsensing.")

if st.button("Executar análise dos dados agora"):
    with st.spinner("Coletando links..."):
        links = coletar_links()
    with st.spinner("Baixando arquivos..."):
        arquivos = baixar_arquivos(links)
    with st.spinner("Processando dados..."):
        dfs = processar_arquivos(arquivos)
    with st.spinner("Analisando e exibindo resultados..."):
        analisar_e_visualizar(dfs)
    st.success("Análise finalizada com sucesso! ✅")

