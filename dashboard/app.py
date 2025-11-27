import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px
import plotly.graph_objects as go


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

## Queries

GET_OCUPACAO_FAMILIAS = """
SELECT DISTINCT descricao_familia
FROM dimOcupacao
WHERE descricao_familia IS NOT NULL
ORDER BY descricao_familia;
"""


TOP_10_CAUSES_BY_OCUPACAO = """ 
WITH ClassificacaoCausas AS (
    SELECT 
        d2.descricao_familia, 
        d.descricao_subcategoria, 
        SUM(f.quantidade_obitos) as total_obitos,
        ROW_NUMBER() OVER(
            PARTITION BY d2.descricao_familia 
            ORDER BY SUM(f.quantidade_obitos) DESC
        ) as ranking
    FROM factobitos f
    JOIN pontegrupocausas p ON p.chave_grupo_causa = f.chave_grupo_causa 
    JOIN dimcausa d ON d.chave_causa = p.chave_causa
    JOIN dimocupacao d2 ON d2.chave_ocupacao = f.chave_ocupacao 
    WHERE d2.descricao_familia = :selected_familia and p.ordem_causa = 1 and d.codigo_cid != '0000'
    GROUP BY 1, 2
)
SELECT 
    descricao_familia,
    descricao_subcategoria,
    total_obitos
FROM ClassificacaoCausas
WHERE ranking <= 10
ORDER BY descricao_familia, total_obitos DESC;
"""


SECOND_ROLL_UP = """
SELECT
    d_mun.estado AS estado,
    d_dem.faixa_etaria AS faixa_etaria_mae,
    SUM(f.quantidade_nascimentos) AS quantidade_nascimentos
FROM factNascimentos f
JOIN dimMunicipio d_mun ON d_mun.chave_municipio = f.chave_municipio_nascimento
JOIN dimDemografia d_dem ON d_dem.chave_demografia = f.chave_demografia
GROUP BY d_mun.estado, d_dem.faixa_etaria
ORDER BY d_mun.estado, d_dem.faixa_etaria;
"""


SLICE_AND_DICE: str = """
SELECT
    d_dat.mes,
    d_dat.ano,
    SUM(f_int.quantidade_obitos ) AS "obitos"
    FROM factobitos f_int
    JOIN (
        SELECT
          ano,
          mes,
          numero_mes,
          chave_data
        FROM dimData
        WHERE ano BETWEEN :start_year AND :end_year
    ) AS d_dat
    ON d_dat.chave_data = f_int.chave_data_obito
    JOIN (
        SELECT
            chave_municipio
        FROM dimMunicipio dm
        WHERE dm.nome_municipio = :city
    ) AS d_mun
    ON d_mun.chave_municipio = f_int.chave_municipio_obito
    GROUP BY 1, 2, d_dat.numero_mes 
    ORDER BY 2, d_dat.numero_mes;
"""


PIVOT: str ="""
    SELECT * FROM CROSSTAB('
        SELECT
                d_dat.ano as "ANO",
                d_mun.uf as "ESTADO",
                SUM(f_int.quantidade_obitos) as quantidade_obitos
            FROM factobitos f_int
            JOIN dimData d_dat
                ON d_dat.chave_data = f_int.chave_data_obito
            JOIN dimMunicipio d_mun
                ON d_mun.chave_municipio = f_int.chave_municipio_obito
            GROUP BY d_dat.ano, d_mun.uf
			ORDER BY d_dat.ano, d_mun.uf
    ',
    '
        SELECT DISTINCT uf
        FROM dimMunicipio
        ORDER BY uf
    ')
AS ct (
    "ANO" INTEGER,
    "AC" BIGINT, "AL" BIGINT, "AP" BIGINT, "AM" BIGINT,
    "BA" BIGINT, "CE" BIGINT, "DF" BIGINT, "ES" BIGINT,
    "GO" BIGINT, "IG" BIGINT, "MA" BIGINT, "MT" BIGINT, 
    "MS" BIGINT, "MG" BIGINT, "PA" BIGINT, "PB" BIGINT, 
    "PR" BIGINT, "PE" BIGINT, "PI" BIGINT, "RJ" BIGINT,
    "RN" BIGINT, "RS" BIGINT, "RO" BIGINT, "RR" BIGINT, 
    "SC" BIGINT, "SE" BIGINT, "SP" BIGINT, "TO" BIGINT 
);
"""


DRILL_ACROSS: str = """
SELECT
        nasc.ano AS "ano",
        nasc.municipio AS "municipio",
        COALESCE(nasc.quantidade_nascimentos,0) AS "quantidade_nascimentos",
        COALESCE(obit.quantidade_obitos,0) AS "quantidade_obitos"
    FROM (
            SELECT
                    d_dat.ano AS ano,
                    d_mun.nome_municipio AS municipio,
                    SUM(f_nas.quantidade_nascimentos) AS quantidade_nascimentos
                FROM factNascimentos f_nas
                JOIN dimData d_dat
                    ON d_dat.chave_data = f_nas.chave_data
                JOIN dimMunicipio d_mun
                    ON d_mun.chave_municipio = f_nas.chave_municipio_nascimento
                WHERE d_mun.regiao_saude in ('Coração do DRS III', 'Central do DRS III', 'Rio Claro')
                GROUP BY d_dat.ano, d_mun.nome_municipio
        ) AS nasc
    JOIN (
            SELECT
                    d_dat.ano AS ano,
                    d_mun.nome_municipio AS municipio,
                    SUM(f_obt.quantidade_obitos) AS quantidade_obitos
                FROM factObitos f_obt
                JOIN dimData d_dat
                    ON d_dat.chave_data = f_obt.chave_data_obito
                JOIN dimMunicipio d_mun
                    ON d_mun.chave_municipio = f_obt.chave_municipio_obito
                WHERE d_mun.regiao_saude in ('Coração do DRS III', 'Central do DRS III', 'Rio Claro')
                GROUP BY d_dat.ano, d_mun.nome_municipio
        ) AS obit
    ON nasc.ano = obit.ano AND nasc.municipio = obit.municipio
	ORDER BY municipio, ano ;
"""


app.layout = html.Div([
    html.H2('OLAP Dashboard - Nascimentos / Óbitos'),

    dcc.Tabs([
        dcc.Tab(label='1) Top 10 Causas de Morte por Família de Ocupação', children=[
            html.Div([
                html.Label('Selecione a Família de Ocupação:'),
                dcc.Dropdown(
                    id='ocupacao-familia-dropdown',
                    options=[],
                    value=None,
                    placeholder='Selecione uma família de ocupação'),
            ], style={'width': '50%', 'margin-bottom': '20px'}),
            dcc.Graph(id='top-causes-pie-chart')
        ]),

        dcc.Tab(label='2) Nascimentos por estado × faixa etária (mãe)', children=[
            html.Button('Atualizar', id='btn-second-roll', n_clicks=0),
            dcc.Graph(id='second-roll-graph')
        ]),

        dcc.Tab(label='3) Slice & Dice - Óbitos por cidade / período', children=[
            html.Div([
                html.Label('Cidade (nome_municipio):'),
                dcc.Input(id='slice-city', value='', type='text'),
                html.Label('Ano início:'),
                dcc.Input(id='slice-start', value=2020, type='number'),
                html.Label('Ano fim:'),
                dcc.Input(id='slice-end', value=2023, type='number'),
                html.Button('Executar', id='btn-slice', n_clicks=0)
            ], style={'display':'flex', 'gap':'10px', 'align-items':'center'}),
            dcc.Graph(id='slice-result')
        ]),

        dcc.Tab(label='4) Pivot - Óbitos por estado por ano', children=[
            html.Button('Atualizar Pivot', id='btn-pivot', n_clicks=0),
            dcc.Graph(id='pivot-heatmap')
        ]),

        dcc.Tab(label='5) Drill-across - Nascimentos x Óbitos por cidade/ano', children=[
            html.Button('Atualizar Drill', id='btn-drill', n_clicks=0),
            dcc.Graph(id='drill-graph')
        ]),
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

@app.callback(Output('slice-result', 'figure'), Input('btn-slice', 'n_clicks'), State('slice-city', 'value'), State('slice-start', 'value'), State('slice-end', 'value'))
def run_slice(n, city, start, end):
    if not city:
        return px.line(title='Digite o nome da cidade (campo nome_municipio).')
    params = {'city': city, 'start_year': int(start), 'end_year': int(end)}
    df = run_query(SLICE_AND_DICE, params=params)
    
    # Criar coluna temporária juntando mês e ano
    if not df.empty:
        df['mes_ano'] = df['mes'].astype(str).str.zfill(2) + '/' + df['ano'].astype(str)
    
    fig = px.line(df, x='mes_ano', y='obitos', title=f'Óbitos em {city} entre {start} e {end}', markers=True)
    return fig


@app.callback(Output('pivot-heatmap', 'figure'), Input('btn-pivot', 'n_clicks'))
def update_pivot(n):
    df = run_query(PIVOT)
    if df.empty:
        return px.imshow([[0]], labels=dict(x='estado', y='ano'), title='Sem dados')
    df = df.set_index('ANO')
    fig = px.imshow(df.values, x=df.columns, y=df.index, aspect='auto', labels=dict(x='Estado', y='Ano', color='Óbitos'), title='Quantidade de Óbitos por Estado por Ano')
    return fig

@app.callback(Output('drill-graph', 'figure'), Input('btn-drill', 'n_clicks'))
def update_drill(n):
    df = run_query(DRILL_ACROSS)
    if df.empty:
        return px.scatter(title='Sem dados')
    # compute natural growth
    df['crescimento_natural'] = df['quantidade_nascimentos'] - df['quantidade_obitos']
    # aggregated by municipio: let user inspect via scatter (x=ano, y=crescimento)
    fig = px.bar(df, x='municipio', y='crescimento_natural', color='ano', title='Crescimento natural (Nascimentos - Óbitos) por Município e Ano')
    return fig


@app.callback(Output('ocupacao-familia-dropdown', 'options'), Output('ocupacao-familia-dropdown', 'value'), Input('ocupacao-familia-dropdown', 'id'))
def update_dropdown_options(dropdown_id):
    df = run_query(GET_OCUPACAO_FAMILIAS)
    if df.empty:
        return [], None
    options = [{'label': familia, 'value': familia} for familia in df['descricao_familia'].tolist()]
    first_value = df['descricao_familia'].iloc[0] if not df.empty else None
    return options, first_value


@app.callback(Output('top-causes-pie-chart', 'figure'), Input('ocupacao-familia-dropdown', 'value'))
def update_top_causes_chart(selected_familia):
    if not selected_familia:
        return px.pie(title='Selecione uma família de ocupação')
    
    params = {'selected_familia': selected_familia}
    df = run_query(TOP_10_CAUSES_BY_OCUPACAO, params=params)
    
    if df.empty:
        return px.pie(title=f'Sem dados para {selected_familia}')
    fig = go.Figure(data=[go.Pie(labels=df['descricao_subcategoria'], values=df['total_obitos'], hole=.5)])
    #fig = px.pie(df, values='total_obitos', names='descricao_subcategoria', title=f'Top 10 Causas de Morte - Família: {selected_familia}')
    return fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)