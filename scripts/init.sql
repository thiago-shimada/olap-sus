CREATE EXTENSION IF NOT EXISTS tablefunc;

DO $$
DECLARE
    ano_inicio INTEGER := 1900;
    ano_fim    INTEGER := 2030;

BEGIN
    RAISE NOTICE 'Criando dimensões de % até %', ano_inicio, ano_fim;

    -- ============================
    -- CRIAÇÃO DAS TABELAS
    -- ============================

    CREATE TABLE IF NOT EXISTS dimData (
        chave_data      SERIAL PRIMARY KEY,
        data            DATE NOT NULL,
        numero_dia_semana INTEGER,
        dia_semana        VARCHAR(20),
        numero_dia        INTEGER,
        dia_ano           INTEGER,
        numero_mes        INTEGER,
        mes               VARCHAR(20),
        ano               INTEGER,
        bimestre          INTEGER,
        trimestre         INTEGER,
        semestre          INTEGER
    );

    CREATE TABLE IF NOT EXISTS dimHorario (
        chave_tempo   SERIAL PRIMARY KEY,
        tempo         TIME,
        hora          INTEGER,
        minutos       INTEGER,
        segundos      INTEGER,
        periodo       VARCHAR(20)
    );

    -- ============================
    -- LIMPA AS TABELAS (OPCIONAL)
    -- ============================

    TRUNCATE TABLE dimData RESTART IDENTITY;
    TRUNCATE TABLE dimHorario RESTART IDENTITY;

    -- ============================
    -- POPULAÇÃO DA DIMDATA
    -- ============================

    INSERT INTO dimData (
        data,
        numero_dia_semana,
        dia_semana,
        numero_dia,
        dia_ano,
        numero_mes,
        mes,
        ano,
        bimestre,
        trimestre,
        semestre
    )
    SELECT
        d::date AS data,
        EXTRACT(ISODOW FROM d)::int AS numero_dia_semana,
        CASE EXTRACT(ISODOW FROM d)
            WHEN 1 THEN 'Segunda-feira'
            WHEN 2 THEN 'Terça-feira'
            WHEN 3 THEN 'Quarta-feira'
            WHEN 4 THEN 'Quinta-feira'
            WHEN 5 THEN 'Sexta-feira'
            WHEN 6 THEN 'Sábado'
            WHEN 7 THEN 'Domingo'
        END AS dia_semana,
        EXTRACT(DAY FROM d)::int AS numero_dia,
        EXTRACT(doy FROM d)::int AS dia_ano,
        EXTRACT(MONTH FROM d)::int AS numero_mes,
        CASE EXTRACT(MONTH FROM d)
            WHEN 1 THEN 'Janeiro'
            WHEN 2 THEN 'Fevereiro'
            WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril'
            WHEN 5 THEN 'Maio'
            WHEN 6 THEN 'Junho'
            WHEN 7 THEN 'Julho'
            WHEN 8 THEN 'Agosto'
            WHEN 9 THEN 'Setembro'
            WHEN 10 THEN 'Outubro'
            WHEN 11 THEN 'Novembro'
            WHEN 12 THEN 'Dezembro'
        END AS mes,
        EXTRACT(YEAR FROM d)::int AS ano,
        CEIL(EXTRACT(MONTH FROM d)::numeric / 2) AS bimestre,
        CEIL(EXTRACT(MONTH FROM d)::numeric / 3) AS trimestre,
        CEIL(EXTRACT(MONTH FROM d)::numeric / 6) AS semestre
    FROM generate_series(
        make_date(ano_inicio, 1, 1),
        make_date(ano_fim, 12, 31),
        interval '1 day'
    ) AS d;

    -- ============================
    -- POPULAÇÃO DA DIMHORARIO
    -- ============================

    INSERT INTO dimHorario (chave_tempo, tempo, hora, minutos, segundos, periodo)
    VALUES (-1, NULL, NULL, NULL, NULL, 'Ignorado');

    INSERT INTO dimHorario (
        tempo,
        hora,
        minutos,
        segundos,
        periodo
    )
    SELECT
        make_time(h, m, s) AS tempo,
        h AS hora,
        m AS minutos,
        s AS segundos,
        CASE
            WHEN h BETWEEN 0 AND 5  THEN 'madrugada'
            WHEN h BETWEEN 6 AND 11 THEN 'manhã'
            WHEN h BETWEEN 12 AND 17 THEN 'tarde'
            ELSE 'noite'
        END AS periodo
    FROM generate_series(0,23) AS h
    CROSS JOIN generate_series(0,59) AS m
    CROSS JOIN generate_series(0,59) AS s
    ORDER BY tempo;

END;
$$;

CREATE TABLE IF NOT EXISTS dimMunicipio (
    chave_municipio SERIAL PRIMARY KEY,
    codigo_ibge INTEGER UNIQUE,
    nome_municipio VARCHAR(100),
    uf VARCHAR(2),
    estado VARCHAR(50),
    regiao VARCHAR(20),
    regiao_saude VARCHAR(300),
    regiao_metropolitana VARCHAR(300),
    is_capital BOOLEAN
);

CREATE TABLE IF NOT EXISTS dimOcupacao (
    chave_ocupacao SERIAL PRIMARY KEY,
    cbo_2002 VARCHAR(10) UNIQUE,
    descricao TEXT,
    familia VARCHAR(10),
    descricao_familia TEXT,
    subgrupo VARCHAR(10),
    descricao_subgrupo TEXT,
    subgrupo_principal VARCHAR(10),
    descricao_subgrupo_principal TEXT,
    grande_grupo VARCHAR(10),
    descricao_grande_grupo TEXT,
    indicador_cbo_2002_ativa INTEGER
);

CREATE TABLE IF NOT EXISTS dimCausa (
    chave_causa SERIAL PRIMARY KEY,
    codigo_CID VARCHAR(10) NOT NULL UNIQUE,
    subcategoria VARCHAR(10),
    descricao_subcategoria VARCHAR(500),
    categoria VARCHAR(10),
    descricao_categoria VARCHAR(500),
    capitulo VARCHAR(50),
    descricao_capitulo VARCHAR(500),
    causa_violencia BOOLEAN DEFAULT FALSE,
    causa_overdose BOOLEAN DEFAULT FALSE
);

CREATE TABLE dimDemografia (
    chave_demografia SERIAL PRIMARY KEY,

    raca VARCHAR(50),

    faixa_etaria VARCHAR(50),
    idade_minima INT,
    idade_maxima INT,

    sexo CHAR(1),
    descricao_sexo VARCHAR(20),

    escolaridade VARCHAR(100),
    nivel_escolaridade INT,

    estado_civil VARCHAR(50)
);

CREATE TABLE dimInfoNascimento (
    chave_info_nascimento SERIAL PRIMARY KEY,

    sexo CHAR(1),
    descricao_sexo VARCHAR(20),

    raca_cor VARCHAR(50),

    faixa_peso VARCHAR(50),
    peso_min_gramas INT,
    peso_max_gramas INT,

    tipo_parto VARCHAR(50),

    tempo_gestacao VARCHAR(50),
    semanas_gestacao_min INT,
    semanas_gestacao_max INT,

    tipo_gravidez VARCHAR(50)
);

CREATE TABLE ponteGrupoCausas (
    chave_grupo_causa INT NOT NULL, 
    
    chave_causa INT NOT NULL,
    
    ordem_causa INT NOT NULL,

    PRIMARY KEY (chave_grupo_causa, ordem_causa),

    CONSTRAINT fk_ponte_grupo_causas
        FOREIGN KEY (chave_causa)
        REFERENCES dimCausa (chave_causa)
);

CREATE TABLE factNascimentos (
    chave_data INT REFERENCES dimData(chave_data),
    chave_tempo INT REFERENCES dimHorario(chave_tempo),
    chave_municipio_nascimento INT REFERENCES dimMunicipio(chave_municipio),
    chave_municipio_residencia INT REFERENCES dimMunicipio(chave_municipio),
    chave_demografia INT REFERENCES dimDemografia(chave_demografia),
    chave_info_nascimento INT REFERENCES dimInfoNascimento(chave_info_nascimento),
    quantidade_nascimentos INT DEFAULT 1,
    PRIMARY KEY (chave_data, chave_tempo, chave_municipio_nascimento, chave_municipio_residencia, chave_demografia, chave_info_nascimento)
);

CREATE TABLE factObitos (
    chave_data_nascimento INT REFERENCES dimData(chave_data),
    chave_data_obito INT REFERENCES dimData(chave_data),
    chave_tempo_obito INT REFERENCES dimHorario(chave_tempo),
    chave_municipio_residencia INT REFERENCES dimMunicipio(chave_municipio),
    chave_municipio_obito INT REFERENCES dimMunicipio(chave_municipio),
    chave_demografia INT REFERENCES dimDemografia(chave_demografia),
    chave_grupo_causa INT, -- Relaciona com ponteGrupoCausas
    chave_ocupacao INT REFERENCES dimOcupacao(chave_ocupacao),
    quantidade_obitos INT DEFAULT 1,
    PRIMARY KEY (chave_data_nascimento, chave_data_obito, chave_tempo_obito, chave_municipio_residencia, chave_municipio_obito, chave_demografia, chave_grupo_causa, chave_ocupacao)
);

CREATE TABLE factInternacoes (
    chave_data_entrada INT REFERENCES dimData(chave_data),
    chave_data_saida INT REFERENCES dimData(chave_data),
    chave_municipio INT REFERENCES dimMunicipio(chave_municipio),
    chave_causa_primaria INT REFERENCES dimCausa(chave_causa),
    chave_causa_secundaria INT REFERENCES dimCausa(chave_causa),
    chave_ocupacao INT REFERENCES dimOcupacao(chave_ocupacao),
    valor NUMERIC(15, 2),
    quantidade_procedimentos INT,
    PRIMARY KEY (chave_data_entrada, chave_data_saida, chave_municipio, chave_causa_primaria, chave_causa_secundaria, chave_ocupacao)
);

-- ============================
-- FUNÇÕES DE POPULAÇÃO
-- ============================

CREATE OR REPLACE FUNCTION popular_dim_municipio() RETURNS void AS $$
BEGIN
    -- Cria tabela temporária para carga
    -- A estrutura deve corresponder exatamente às colunas do CSV
    CREATE TEMP TABLE IF NOT EXISTS stg_municipio (
        id_municipio text,
        id_municipio_6 text,
        id_municipio_tse text,
        id_municipio_rf text,
        id_municipio_bcb text,
        nome text,
        capital_uf text,
        id_comarca text,
        id_regiao_saude text,
        nome_regiao_saude text,
        id_regiao_imediata text,
        nome_regiao_imediata text,
        id_regiao_intermediaria text,
        nome_regiao_intermediaria text,
        id_microrregiao text,
        nome_microrregiao text,
        id_mesorregiao text,
        nome_mesorregiao text,
        id_regiao_metropolitana text,
        nome_regiao_metropolitana text,
        ddd text,
        id_uf text,
        sigla_uf text,
        nome_uf text,
        nome_regiao text,
        amazonia_legal text,
        centroide text
    );

    -- Limpa tabela temporária
    TRUNCATE stg_municipio;

    -- Copia dados do CSV
    -- O arquivo br_bd_diretorios_brasil_municipio.csv já contém as informações de UF e Região
    EXECUTE 'COPY stg_municipio FROM ''/data_files/br_bd_diretorios_brasil_municipio.csv'' WITH (FORMAT CSV, HEADER)';

    -- Insere na tabela dimensão
    INSERT INTO dimMunicipio (
        codigo_ibge,
        nome_municipio,
        uf,
        estado,
        regiao,
        regiao_saude,
        regiao_metropolitana,
        is_capital
    )
    SELECT
        NULLIF(id_municipio, '')::integer,
        nome,
        sigla_uf,
        nome_uf,
        nome_regiao,
        nome_regiao_saude,
        nome_regiao_metropolitana,
        CASE WHEN capital_uf = '1' THEN TRUE ELSE FALSE END
    FROM stg_municipio
    WHERE id_municipio IS NOT NULL AND id_municipio != ''
    ON CONFLICT (codigo_ibge) DO NOTHING;

    -- Remove tabela temporária
    DROP TABLE stg_municipio;
    
    RAISE NOTICE 'Tabela dimMunicipio populada com sucesso.';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION popular_dim_ocupacao() RETURNS void AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS stg_ocupacao (
        cbo_2002 text,
        descricao text,
        familia text,
        descricao_familia text,
        subgrupo text,
        descricao_subgrupo text,
        subgrupo_principal text,
        descricao_subgrupo_principal text,
        grande_grupo text,
        descricao_grande_grupo text,
        indicador_cbo_2002_ativa text
    );

    TRUNCATE stg_ocupacao;

    EXECUTE 'COPY stg_ocupacao FROM ''/data_files/br_bd_diretorios_brasil_cbo_2002.csv'' WITH (FORMAT CSV, HEADER)';

    INSERT INTO dimOcupacao (
        cbo_2002,
        descricao,
        familia,
        descricao_familia,
        subgrupo,
        descricao_subgrupo,
        subgrupo_principal,
        descricao_subgrupo_principal,
        grande_grupo,
        descricao_grande_grupo,
        indicador_cbo_2002_ativa
    )
    SELECT
        TRIM(cbo_2002),
        TRIM(descricao),
        TRIM(familia),
        TRIM(descricao_familia),
        TRIM(subgrupo),
        TRIM(descricao_subgrupo),
        TRIM(subgrupo_principal),
        TRIM(descricao_subgrupo_principal),
        TRIM(grande_grupo),
        TRIM(descricao_grande_grupo),
        CASE WHEN TRIM(indicador_cbo_2002_ativa) = '1' THEN 1 ELSE 0 END
    FROM stg_ocupacao
    ON CONFLICT (cbo_2002) DO NOTHING;

    DROP TABLE stg_ocupacao;

    RAISE NOTICE 'Tabela dimOcupacao populada com sucesso.';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION popular_dim_causa() RETURNS void AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS stg_causa (
        subcategoria text,
        descricao_subcategoria text,
        categoria text,
        descricao_categoria text,
        capitulo text,
        descricao_capitulo text,
        causa_violencia text,
        causa_overdose text,
        cid_datasus text
    );

    TRUNCATE stg_causa;

    EXECUTE 'COPY stg_causa FROM ''/data_files/br_bd_diretorios_brasil_cid_10.csv'' WITH (FORMAT CSV, HEADER)';

    INSERT INTO dimCausa (
        codigo_CID,
        subcategoria,
        descricao_subcategoria,
        categoria,
        descricao_categoria,
        capitulo,
        descricao_capitulo,
        causa_violencia,
        causa_overdose
    )
    SELECT
        subcategoria,
        subcategoria,
        descricao_subcategoria,
        categoria,
        descricao_categoria,
        capitulo,
        descricao_capitulo,
        CASE WHEN causa_violencia = '1' THEN TRUE ELSE FALSE END,
        CASE WHEN causa_overdose = '1' THEN TRUE ELSE FALSE END
    FROM stg_causa
    ON CONFLICT (codigo_CID) DO NOTHING;

    DROP TABLE stg_causa;

    RAISE NOTICE 'Tabela dimCausa populada com sucesso.';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION popular_dim_demografia() RETURNS void AS $$
BEGIN
    INSERT INTO dimDemografia (
        raca,
        escolaridade,
        nivel_escolaridade,
        estado_civil,
        sexo,
        descricao_sexo,
        faixa_etaria,
        idade_minima,
        idade_maxima
    )
    SELECT
        r.raca,
        e.escolaridade,
        e.nivel,
        ec.estado_civil,
        s.sexo,
        s.descricao,
        f.faixa,
        f.min_idade,
        f.max_idade
    FROM
        (VALUES 
            ('Branca'), ('Preta'), ('Amarela'), ('Parda'), ('Indígena')
        ) AS r(raca)
    CROSS JOIN
        (VALUES 
            ('Nenhuma', 0), 
            ('1 a 3 anos', 1), 
            ('4 a 7 anos', 2), 
            ('8 a 11 anos', 3), 
            ('12 e mais', 4), 
            ('Ignorado', 9)
        ) AS e(escolaridade, nivel)
    CROSS JOIN
        (VALUES 
            ('Solteiro'), 
            ('Casado'), 
            ('Viúvo'), 
            ('Separado judicialmente/divorciado'), 
            ('União estável'), 
            ('Ignorado')
        ) AS ec(estado_civil)
    CROSS JOIN
        (VALUES 
            ('M', 'Masculino'), 
            ('F', 'Feminino'), 
            ('I', 'Ignorado')
        ) AS s(sexo, descricao)
    CROSS JOIN
        (
            SELECT '0 a 5 anos' AS faixa, 0 AS min_idade, 5 AS max_idade
            UNION ALL
            SELECT 
                n::text || ' a ' || (n+4)::text || ' anos',
                n,
                n+4
            FROM generate_series(6, 96, 5) AS n
            UNION ALL
            SELECT 'Mais de 100 anos', 101, NULL
        ) AS f(faixa, min_idade, max_idade);

    RAISE NOTICE 'Tabela dimDemografia populada com sucesso.';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION popular_dim_info_nascimento() RETURNS void AS $$
BEGIN
    INSERT INTO dimInfoNascimento (
        sexo,
        descricao_sexo,
        raca_cor,
        faixa_peso,
        peso_min_gramas,
        peso_max_gramas,
        tipo_parto,
        tempo_gestacao,
        semanas_gestacao_min,
        semanas_gestacao_max,
        tipo_gravidez
    )
    SELECT
        s.sexo,
        s.descricao,
        r.raca,
        p.faixa,
        p.min_peso,
        p.max_peso,
        tp.tipo,
        tg.faixa,
        tg.min_sem,
        tg.max_sem,
        tgr.tipo
    FROM
        (VALUES 
            ('M', 'Masculino'), 
            ('F', 'Feminino'), 
            ('I', 'Ignorado')
        ) AS s(sexo, descricao)
    CROSS JOIN
        (VALUES 
            ('Branca'), ('Preta'), ('Amarela'), ('Parda'), ('Indígena')
        ) AS r(raca)
    CROSS JOIN
        (VALUES 
            ('Extremo Baixo Peso', 0, 999),
            ('Muito Baixo Peso', 1000, 1499),
            ('Baixo Peso', 1500, 2499),
            ('Normal', 2500, 3999),
            ('Macrossômico', 4000, NULL)
        ) AS p(faixa, min_peso, max_peso)
    CROSS JOIN
        (VALUES 
            ('Vaginal'), ('Cesário'), ('Ignorado')
        ) AS tp(tipo)
    CROSS JOIN
        (VALUES 
            ('Menos de 22 semanas', 0, 21),
            ('22 a 27 semanas', 22, 27),
            ('28 a 31 semanas', 28, 31),
            ('32 a 36 semanas', 32, 36),
            ('37 a 41 semanas', 37, 41),
            ('42 semanas e mais', 42, NULL),
            ('Ignorado', NULL, NULL)
        ) AS tg(faixa, min_sem, max_sem)
    CROSS JOIN
        (VALUES 
            ('Única'), ('Dupla'), ('Tripla ou mais'), ('Ignorado')
        ) AS tgr(tipo);

    RAISE NOTICE 'Tabela dimInfoNascimento populada com sucesso.';
END;
$$ LANGUAGE plpgsql;

-- ============================
-- INSERÇÃO DE REGISTROS "IGNORADO" (ID 0)
-- ============================

INSERT INTO dimMunicipio (chave_municipio, codigo_ibge, nome_municipio, uf, estado, regiao, is_capital)
VALUES (0, 0, 'Ignorado', 'IG', 'Ignorado', 'Ignorado', FALSE)
ON CONFLICT (codigo_ibge) DO NOTHING;

INSERT INTO dimOcupacao (chave_ocupacao, cbo_2002, descricao)
VALUES (0, '000000', 'Ignorado')
ON CONFLICT (cbo_2002) DO NOTHING;

INSERT INTO dimCausa (chave_causa, codigo_CID, descricao_subcategoria)
VALUES (0, '0000', 'Causa Ignorada')
ON CONFLICT (codigo_CID) DO NOTHING;

-- Grupo de Causa 0 -> Causa 0
INSERT INTO ponteGrupoCausas (chave_grupo_causa, chave_causa, ordem_causa)
VALUES (0, 0, 1)
ON CONFLICT (chave_grupo_causa, ordem_causa) DO NOTHING;

-- Executa as funções de população
SELECT popular_dim_municipio();
SELECT popular_dim_ocupacao();
SELECT popular_dim_causa();
SELECT popular_dim_demografia();
SELECT popular_dim_info_nascimento();
