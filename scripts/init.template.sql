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
        segundos      INTEGER
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
        TO_CHAR(d, 'TMDay') AS dia_semana,
        EXTRACT(DAY FROM d)::int AS numero_dia,
        EXTRACT(doy FROM d)::int AS dia_ano,
        EXTRACT(MONTH FROM d)::int AS numero_mes,
        TO_CHAR(d, 'TMMonth') AS mes,
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
        segundos
    )
    SELECT
        make_time(h, m, s) AS tempo,
        h AS hora,
        m AS minutos,
        s AS segundos
    FROM generate_series(0,23) AS h
    CROSS JOIN generate_series(0,59) AS m
    CROSS JOIN generate_series(0,59) AS s;

END;
$$;