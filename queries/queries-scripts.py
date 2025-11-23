# 1ª roll-up/drill-down
# Quantidade de óbitos de acordo com família de ocupação por escolaridade.
# Essa consulta permite analisar a mortalidade considerando o nível de
# escolaridade em diferentes grupos ocupacionais. É possível, por exemplo,
# identificar se certas ocupações apresentam maior incidência de óbitos entre
# pessoas com menor ou maior escolaridade, ou ainda observar padrões que
# relacionam a formação educacional com condições de trabalho e vulnerabilidade
# social.
FIRST_ROLL_UP: str = """
SELECT
        d_ocp.familia AS 'familia_ocupacao',
        d_dem.escolaridade AS 'escolaridade',
        SUM(f_obt.quantidade_obitos) AS 'quantidade_obitos'
    FROM fctObitos f_obt
    JOIN dimOcupacao d_ocp
        ON d_ocp.chave_ocupacaoPK = f_obt.chave_ocupacaoFK
    JOIN dimDemografia d_dem
        ON d_dem.chave_demografiaPK = f_obt.chave_demografiaFK
    GROUP BY d_ocp.familia, d_dem.escolaridade
"""

# 2ª roll-up/drill-down
# Quantidade de nascimentos por estado dividido em faixas etárias da mãe.
# Essa análise permite observar como a natalidade se distribui entre diferentes
# faixas etárias em cada unidade da federação. Com ela é possível identificar,
# por exemplo, estados onde predominam nascimentos de mães mais jovens ou de
# mães em idade mais avançada, o que contribui para o planejamento de políticas
# públicas voltadas à saúde materna e de educação sexual e permanência escolar.
SECOND_ROLL_UP: str = """
SELECT
        d_mun.estado AS 'estado',
        d_mae.faixa_etaria AS 'faixa_etaria_mae',
        SUM(f_nas.quantidade_nascimentos) AS 'quantidade_nascimentos'
    FROM fctNascimentos f_nas
    JOIN dimMunicipio d_mun
        ON d_mun.chave_municipioPK = f_nas.chave_municioFK
    JOIN dimDemografiaMae d_mae
        ON d_mae.chave_demografia_maePK = f_nas.chave_demografia_maeFK
    GROUP BY d_mun.estado, d_mae.faixa_etaria
"""

# Slice and dice
# Custo de internações na cidade de São Carlos entre os anos de 2017 a 2022.
# É realizada uma operação de slice para filtrar apenas as internações na cidade
# de São Carlos, e em seguida um dice para restringir o período entre 2017 e 2022.
# Essa consulta fornece uma visão direcionada do custo hospitalar em São Carlos,
# e no período de interesse que contempla os anos de 2017 a 2022.
# É uma avaliação útil porque o intervalo inclui tanto anos afetados diretamente
# pela pandemia da COVID-19 quanto anos anteriores, permitindo comparar o
# impacto orçamentário entre diferentes contextos.
SLICE_AND_DICE: str = """
SELECT
    SUM(f_int.valor_ato_profissional) AS 'custo'
    JOIN (
            SELECT
                    chave_dataPK,
                FROM dimData
                WHERE ano BETWEEN 2017 AND 2022
        ) AS d_dat
        ON d_dat.chave_dataPK = f_int.chave_dataFK
    JOIN (
            SELECT
                chave_municipioPK,
            FROM dimMunicipio
            WHERE municipio = 'São Carlos'
        ) AS d_mun
        ON d_mun.chave_municipioPK = f_int.chave_municipioFK
"""

# Pivot
# Quantidade de internações por estado por ano, com estados nas colunas e
# anos nas linhas. Essa consulta facilita a comparação direta entre estados
# em diferentes anos, evidenciando as unidades de federação que necessitam de
# mais serviços hospitalares.
PIVOT: str ="""
    SELECT * FROM CROSSTAB('
        SELECT
                d_dat.ano AS ano,
                d_mun.estado as estado,
                SUM(f_int.quantidade_internacoes) as quantidade_internacoes
            FROM fctInternacoes f_int
            JOIN dimData d_dat
                ON d_dat.chave_dataPK = f_int.chave_dataFK
            JOIN dimMunicipios d_mun
                ON d_mun.chave_municipioPK = f_int.chave_municipioFK
            GROUP BY d_dat.ano, d_mun.estado
    ',
    '
        SELECT
                d_mun.estado
            FROM dimMunicipio
    ')
"""

# Drill-across
# Total de óbitos e nascimentos por cidade por ano. A análise conjunta permite
# observar a dinâmica populacional local, identificando cidades com crescimento
# natural positivo (mais nascimentos que óbitos) ou negativo. Essa informação
# é crucial para o planejamento da alocação de verbas para educação, saúde e
# infraestrutura
DRILL_ACROSS: str = """
SELECT
        nasc.ano AS 'ano',
        nasc.municipio AS 'municipio',
        nasc.quantidade_nascimentos AS 'quantidade_nascimentos',
        obit.quantidade_obitos AS 'quantidade_obitos
    FROM (
            SELECT
                    d_dat.ano AS ano,
                    d_mun.municipio AS municipio,
                    SUM(f_nas.quantidade_nascimentos) AS quantidade_nascimentos
                FROM fctNascimentos f_nas
                JOIN dimData d_dat
                    ON d_dat.chave_dataPK = f_nas.chave_dataFK
                JOIN dimMunicipio d_mun
                    ON d_mun.chave_municipioPK = f_nas.chave_municipioFK
                GROUP BY d_dat.ano, d_mun.municipio
        ) AS nasc
    JOIN (
            SELECT
                    d_dat.ano AS ano,
                    d_mun.municipio AS municipio,
                    SUM(f_obt.quantidade_obitos) AS quantidade_obitos
                FROM fctObitos f_obt
                JOIN dimData d_dat
                    ON d_dat.chave_dataPK = f_obt.chave_dataFK
                JOIN dimMunicipio d_mun
                    ON d_mun.chave_municipioPK = f_obt.chave_municipioFK
                GROUP BY d_dat.ano, d_mun.municipio
        ) AS obit
        ON nasc.ano = obit.ano AND nasc.municipio = obit.municipio
"""