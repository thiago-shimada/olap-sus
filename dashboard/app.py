import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px


# DB connection from environment
DB_USER = os.getenv('DB_USER', 'olap')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'olap')
DB_HOST = os.getenv('DB_HOST', 'postgres-olap')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'olap_db')


# SQLAlchemy engine
password_quoted = quote_plus(DB_PASSWORD)
DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server


# Helper to run query and return DataFrame
def run_query(q, params=None):
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn, params=params)
        return df


# Corrected and parameterized queries based on your schema
FIRST_ROLL_UP = """
SELECT
    d_ocp.familia AS familia_ocupacao,
    d_dem.escolaridade AS escolaridade,
    SUM(f.quantidade_obitos) AS quantidade_obitos
FROM factObitos f
LEFT JOIN dimOcupacao d_ocp ON d_ocp.chave_ocupacao = f.chave_ocupacao
LEFT JOIN dimDemografia d_dem ON d_dem.chave_demografia = f.chave_demografia
GROUP BY d_ocp.familia, d_dem.escolaridade
ORDER BY d_ocp.familia, d_dem.escolaridade;
"""


SECOND_ROLL_UP = """
SELECT
    d_mun.estado AS estado,
    d_dem.faixa_etaria AS faixa_etaria_mae,
    SUM(f.quantidade_nascimentos) AS quantidade_nascimentos
FROM factNascimentos f
LEFT JOIN dimMunicipio d_mun ON d_mun.chave_municipio = f.chave_municipio_nascimento
LEFT JOIN dimDemografia d_dem ON d_dem.chave_demografia = f.chave_demografia
GROUP BY d_mun.estado, d_dem.faixa_etaria
ORDER BY d_mun.estado, d_dem.faixa_etaria;
"""


SLICE_AND_DICE = """
SELECT
    SUM(f.valor) AS custo
FROM factInternacoes f
JOIN dimData d_dat ON d_dat.chave_data = f.chave_data_entrada
JOIN dimMunicipio d_mun ON d_mun.chave_municipio = f.chave_municipio
WHERE d_mun.nome_municipio = :city
AND d_dat.ano BETWEEN :start_year AND :end_year;
"""


PIVOT_BASE = """
SELECT
    d_dat.ano AS ano,
    d_mun.estado AS estado,
    SUM(f.valor) AS valor_total,
    SUM(f.quantidade_procedimentos) AS quantidade_internacoes
FROM factInternacoes f
JOIN dimData d_dat ON d_dat.chave_data = f.chave_data_entrada
JOIN dimMunicipio d_mun ON d_mun.chave_municipio = f.chave_municipio
GROUP BY d_dat.ano, d_mun.estado
ORDER BY d_dat.ano, d_mun.estado;
"""


DRILL_ACROSS_BIRTHS = """
SELECT
    d_dat.ano AS ano,
    d_mun.nome_municipio AS municipio,
    SUM(f.quantidade_nascimentos) AS quantidade_nascimentos
FROM factNascimentos f
JOIN dimData d_dat ON d_dat.chave_data = f.chave_data
JOIN dimMunicipio d_mun ON d_mun.chave_municipio = f.chave_municipio_nascimento
GROUP BY d_dat.ano, d_mun.nome_municipio
"""


DRILL_ACROSS_DEATHS = """
SELECT
    d_dat.ano AS ano,
    d_mun.nome_municipio AS municipio,
    SUM(f.quantidade_obitos) AS quantidade_obitos
FROM factObitos f
JOIN dimData d_dat ON d_dat.chave_data = f.chave_data_obito
JOIN dimMunicipio d_mun ON d_mun.chave_municipio = f.chave_municipio_obito
GROUP BY d_dat.ano, d_mun.nome_municipio
"""

app.layout = html.Div([
    html.H2('OLAP Dashboard - Nascimentos / Óbitos / Internações'),

    dcc.Tabs([
        dcc.Tab(label='1) Óbitos por família ocupação × escolaridade', children=[
            html.Button('Atualizar', id='btn-first-roll', n_clicks=0),
            dcc.Graph(id='first-roll-graph'),
            dcc.Store(id='first-roll-data')
        ]),

        dcc.Tab(label='2) Nascimentos por estado × faixa etária (mãe)', children=[
            html.Button('Atualizar', id='btn-second-roll', n_clicks=0),
            dcc.Graph(id='second-roll-graph')
        ]),

        dcc.Tab(label='3) Slice & Dice - Custo internações por cidade / período', children=[
            html.Div([
                html.Label('Cidade (nome_municipio):'),
                dcc.Input(id='slice-city', value='', type='text'),
                html.Label('Ano início:'),
                dcc.Input(id='slice-start', value=2020, type='number'),
                html.Label('Ano fim:'),
                dcc.Input(id='slice-end', value=2023, type='number'),
                html.Button('Executar', id='btn-slice', n_clicks=0)
            ], style={'display':'flex', 'gap':'10px', 'align-items':'center'}),
            html.Div(id='slice-result')
        ]),

        dcc.Tab(label='4) Pivot - Internações por estado por ano', children=[
            html.Button('Atualizar Pivot', id='btn-pivot', n_clicks=0),
            dcc.Graph(id='pivot-heatmap')
        ]),

        dcc.Tab(label='5) Drill-across - Nascimentos x Óbitos por cidade/ano', children=[
            html.Button('Atualizar Drill', id='btn-drill', n_clicks=0),
            dcc.Graph(id='drill-graph')
        ])
    ])
])

# Callbacks
@app.callback(Output('first-roll-graph', 'figure'), Input('btn-first-roll', 'n_clicks'))
def update_first_roll(n):
    df = run_query(FIRST_ROLL_UP)
    if df.empty:
        fig = px.bar(title='Sem dados')
        return fig
    fig = px.bar(df, x='familia_ocupacao', y='quantidade_obitos', color='escolaridade', barmode='group', title='Óbitos por família de ocupação e escolaridade')
    return fig


@app.callback(Output('second-roll-graph', 'figure'), Input('btn-second-roll', 'n_clicks'))
def update_second_roll(n):
    df = run_query(SECOND_ROLL_UP)
    if df.empty:
        return px.bar(title='Sem dados')
    fig = px.bar(df, x='estado', y='quantidade_nascimentos', color='faixa_etaria_mae', barmode='group', title='Nascimentos por estado e faixa etária da mãe')
    return fig

@app.callback(Output('slice-result', 'children'), Input('btn-slice', 'n_clicks'), State('slice-city', 'value'), State('slice-start', 'value'), State('slice-end', 'value'))
def run_slice(n, city, start, end):
    if not city:
        return html.Div('Digite o nome da cidade (campo nome_municipio).')
    params = {'city': city, 'start_year': int(start), 'end_year': int(end)}
    df = run_query(SLICE_AND_DICE, params=params)
    custo = df['custo'].iloc[0] if not df.empty else 0
    return html.Div(f'Custo total de internações em {city} ({start}–{end}): R$ {custo:.2f}')


@app.callback(Output('pivot-heatmap', 'figure'), Input('btn-pivot', 'n_clicks'))
def update_pivot(n):
    df = run_query(PIVOT_BASE)
    if df.empty:
        return px.imshow([[0]], labels=dict(x='estado', y='ano'), title='Sem dados')
    pivot = df.pivot_table(index='ano', columns='estado', values='quantidade_internacoes', aggfunc='sum', fill_value=0)
    fig = px.imshow(pivot.values, x=pivot.columns, y=pivot.index, aspect='auto', labels=dict(x='Estado', y='Ano'), title='Quantidade de internações por Estado por Ano')
    return fig

@app.callback(Output('drill-graph', 'figure'), Input('btn-drill', 'n_clicks'))
def update_drill(n):
    births = run_query(DRILL_ACROSS_BIRTHS)
    deaths = run_query(DRILL_ACROSS_DEATHS)
    if births.empty and deaths.empty:
        return px.scatter(title='Sem dados')
    df = pd.merge(births, deaths, on=['ano', 'municipio'], how='outer')
    df['quantidade_nascimentos'] = df['quantidade_nascimentos'].fillna(0)
    df['quantidade_obitos'] = df['quantidade_obitos'].fillna(0)
    # compute natural growth
    df['crescimento_natural'] = df['quantidade_nascimentos'] - df['quantidade_obitos']
    # aggregated by municipio: let user inspect via scatter (x=ano, y=crescimento)
    fig = px.bar(df, x='municipio', y='crescimento_natural', color='ano', title='Crescimento natural (Nascimentos - Óbitos) por Município e Ano')
    return fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=False)