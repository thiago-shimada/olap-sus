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
        d_ocp.descricao_familia as "familia",
        d_dem.escolaridade as "escolaridade",
        SUM(f_obt.quantidade_obitos)
    FROM factobitos f_obt
    JOIN dimocupacao d_ocp
        ON d_ocp.chave_ocupacao = f_obt.chave_ocupacao  
    JOIN dimdemografia d_dem
        ON d_dem.chave_demografia  = f_obt.chave_demografia
    GROUP BY d_ocp.descricao_familia, d_dem.escolaridade;
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
        d_mun.estado AS "estado",
        d_mae.faixa_etaria AS "faixa_etaria_mae",
        SUM(f_nas.quantidade_nascimentos) AS "quantidade_nascimentos"
    FROM factNascimentos f_nas
    JOIN dimMunicipio d_mun
        ON d_mun.chave_municipio = f_nas.chave_municipio_nascimento 
    JOIN dimDemografia d_mae
        ON d_mae.chave_demografia = f_nas.chave_demografia
    GROUP BY d_mun.estado, d_mae.faixa_etaria;
"""

# Slice and dice
# Custo de internações na cidade de ${city} entre os anos de ${start_year} a ${end_year}.
# É realizada uma operação de slice para filtrar apenas as internações na cidade
# de ${city}, e em seguida um dice para restringir o período entre ${start_year} a ${end_year}.
# Essa consulta fornece uma visão direcionada do custo hospitalar em ${city},
# e no período de interesse que contempla os anos de ${start_year} a ${end_year}.
# É uma avaliação útil porque o intervalo inclui tanto anos afetados diretamente
# pela pandemia da COVID-19 quanto anos anteriores, permitindo comparar o
# impacto orçamentário entre diferentes contextos.
def SLICE_AND_DICE(city: str, start_year: int, end_year: int) -> str: f"""
SELECT
    SUM(f_int.quantidade_obitos ) AS "óbitos"
    FROM factobitos f_int  
    JOIN (
        SELECT
        	chave_data
        FROM dimData
        WHERE ano BETWEEN {start_year} AND {end_year}
    ) AS d_dat
    ON d_dat.chave_data = f_int.chave_data_obito
    JOIN (
        SELECT
            chave_municipio
        FROM dimMunicipio dm
        WHERE dm.nome_municipio = '{city}'
    ) AS d_mun
    ON d_mun.chave_municipio = f_int.chave_municipio_obito ;
"""

# Pivot
# Quantidade de internações por estado por ano, com estados nas colunas e
# anos nas linhas. Essa consulta facilita a comparação direta entre estados
# em diferentes anos, evidenciando as unidades de federação que necessitam de
# mais serviços hospitalares.
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
    "IG" BIGINT, "GO" BIGINT, "MA" BIGINT, "MT" BIGINT, 
    "MS" BIGINT, "MG" BIGINT, "PA" BIGINT, "PB" BIGINT, 
    "PR" BIGINT, "PE" BIGINT, "PI" BIGINT, "RJ" BIGINT,
    "RN" BIGINT, "RS" BIGINT, "RO" BIGINT, "RR" BIGINT, 
    "SC" BIGINT, "SE" BIGINT, "SP" BIGINT, "TO" BIGINT 
);
"""

# Drill-across
# Total de óbitos e nascimentos por cidade por ano. A análise conjunta permite
# observar a dinâmica populacional local, identificando cidades com crescimento
# natural positivo (mais nascimentos que óbitos) ou negativo. Essa informação
# é crucial para o planejamento da alocação de verbas para educação, saúde e
# infraestrutura
DRILL_ACROSS: str = """
SELECT
        nasc.ano AS "ano",
        nasc.municipio AS "municipio",
        nasc.quantidade_nascimentos AS "quantidade_nascimentos",
        obit.quantidade_obitos AS "quantidade_obitos"
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
                GROUP BY d_dat.ano, d_mun.nome_municipio
        ) AS obit
    ON nasc.ano = obit.ano AND nasc.municipio = obit.municipio;
"""