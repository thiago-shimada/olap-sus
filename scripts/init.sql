CREATE EXTENSION IF NOT EXISTS tablefunc;

DO $$
DECLARE
    ano_inicio INTEGER := 2020;
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
        tempo         TIME NOT NULL,
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