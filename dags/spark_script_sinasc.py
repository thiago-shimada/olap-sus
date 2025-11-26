import argparse
import glob
import sys
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================
# UTILITÁRIOS
# ==========================================

def _list_paths_with_glob(spark: SparkSession, glob_path: str) -> List[str]:
    try:
        jvm = spark._jvm
        sc = spark.sparkContext
        hconf = sc._jsc.hadoopConfiguration()
        Path = jvm.org.apache.hadoop.fs.Path
        path = Path(glob_path)
        fs = path.getFileSystem(hconf)
        statuses = fs.globStatus(path)
        return [status.getPath().toString() for status in statuses] if statuses else []
    except Exception:
        return [glob_path]

def get_jdbc_df(spark, table, opts):
    """Lê uma tabela do Postgres como DataFrame Spark."""
    return spark.read.format("jdbc").options(**opts).option("dbtable", table).load()

# ==========================================
# TRANSFORMAÇÃO E LIMPEZA - SINASC
# ==========================================

def transform_sinasc_raw(df):
    """
    Limpeza e tipagem dos dados do SINASC (Nascimentos).
    Mapeia códigos para descrições compatíveis com as dimensões do init.sql.
    """
    # 1. Datas
    # Formato SINASC geralmente é DDMMAAAA
    df = df.withColumn("data_nascimento", F.to_date(F.col("DTNASC").cast("string"), "ddMMyyyy"))

    # 2. Hora (HH:MM) -> Converter para HH:MM:SS
    # SINASC as vezes vem com 4 digitos (HHMM).
    df = df.withColumn("hora_clean", F.lpad(F.col("HORANASC").cast("string"), 4, "0"))
    df = df.withColumn("tempo_nascimento", 
        F.when(
            (F.col("hora_clean").isNull()) | (F.col("hora_clean") == "") | (F.col("hora_clean") > "2359"), 
            F.lit("00:00:00") # Default para ignorado
        ).otherwise(
            F.concat(
                F.substring(F.col("hora_clean"), 1, 2), F.lit(":"),
                F.substring(F.col("hora_clean"), 3, 2), F.lit(":00")
            )
        )
    )

    # 3. Tratamento de Municípios (6 dígitos para Join)
    df = df.withColumn("cod_mun_nasc", 
        F.when((F.col("CODMUNNASC").isNull()) | (F.trim(F.col("CODMUNNASC")) == ""), None)
         .otherwise(F.substring(F.col("CODMUNNASC").cast("string"), 1, 6).cast("int"))
    )
    df = df.withColumn("cod_mun_res", 
        F.when((F.col("CODMUNRES").isNull()) | (F.trim(F.col("CODMUNRES")) == ""), None)
         .otherwise(F.substring(F.col("CODMUNRES").cast("string"), 1, 6).cast("int"))
    )

    # ==========================================
    # MAPEAMENTOS PARA DIMENSÃO DEMOGRAFIA (MÃE)
    # ==========================================

    # Idade da Mãe (Numérico)
    df = df.withColumn("idade_mae", F.col("IDADEMAE").cast("int"))

    # Raça Mãe (Campo RACACORMAE no layout novo ou RACACOR se antigo, assumindo layout > 2010 base pdf)
    # 1-Branca, 2-Preta, 3-Amarela, 4-Parda, 5-Indígena
    df = df.withColumn("raca_mae_desc", 
        F.when(F.col("RACACORMAE") == "1", "Branca")
         .when(F.col("RACACORMAE") == "2", "Preta")
         .when(F.col("RACACORMAE") == "3", "Amarela")
         .when(F.col("RACACORMAE") == "4", "Parda")
         .when(F.col("RACACORMAE") == "5", "Indígena")
         .otherwise("Ignorado")
    )

    # Escolaridade Mãe (ESCMAE - Escala 1 a 5 conforme init.sql)
    # 1: Nenhuma, 2: 1 a 3 anos, 3: 4 a 7 anos, 4: 8 a 11 anos, 5: 12 e mais
    df = df.withColumn("esc_mae_desc",
        F.when(F.col("ESCMAE") == "1", "Nenhuma")
         .when(F.col("ESCMAE") == "2", "1 a 3 anos")
         .when(F.col("ESCMAE") == "3", "4 a 7 anos")
         .when(F.col("ESCMAE") == "4", "8 a 11 anos")
         .when(F.col("ESCMAE") == "5", "12 e mais")
         .otherwise("Ignorado")
    )

    # Estado Civil Mãe
    # 1- Solteira, 2- Casada, 3-Viúva, 4-Separada, 5-União estável
    df = df.withColumn("estciv_mae_desc",
        F.when(F.col("ESTCIVMAE") == "1", "Solteiro") # init.sql usa masculino no termo
         .when(F.col("ESTCIVMAE") == "2", "Casado")
         .when(F.col("ESTCIVMAE") == "3", "Viúvo")
         .when(F.col("ESTCIVMAE") == "4", "Separado judicialmente/divorciado")
         .when(F.col("ESTCIVMAE") == "5", "União estável")
         .otherwise("Ignorado")
    )

    # ==========================================
    # MAPEAMENTOS PARA DIMENSÃO INFO NASCIMENTO (BEBÊ/PARTO)
    # ==========================================

    # Sexo do RN
    df = df.withColumn("sexo_rn_desc", 
        F.when(F.col("SEXO") == "1", "M")
        .when(F.col("SEXO") == "M", "M")
        .when(F.col("SEXO") == "1", "M")
        .when(F.col("SEXO") == "F", "F")
        .when(F.col("SEXO") == "2", "F")
        .otherwise("I")
    )

    # Raça do RN (Item 19)
    df = df.withColumn("raca_rn_desc", 
        F.when(F.col("RACACOR") == "1", "Branca")
         .when(F.col("RACACOR") == "2", "Preta")
         .when(F.col("RACACOR") == "3", "Amarela")
         .when(F.col("RACACOR") == "4", "Parda")
         .when(F.col("RACACOR") == "5", "Indígena")
         .otherwise("Ignorado")
    )

    # Peso (Gramas)
    df = df.withColumn("peso_gramas", F.col("PESO").cast("int"))

    # Tipo de Parto (Item 13)
    # 1-Vaginal, 2-Cesário
    df = df.withColumn("parto_desc",
        F.when(F.col("PARTO") == "1", "Vaginal")
         .when(F.col("PARTO") == "2", "Cesário")
         .otherwise("Ignorado")
    )

    # Semanas de Gestação (Item 11)
    # 1: <22, 2: 22-27, 3: 28-31, 4: 32-36, 5: 37-41, 6: >42
    df = df.withColumn("gestacao_desc",
        F.when(F.col("GESTACAO") == "1", "Menos de 22 semanas")
         .when(F.col("GESTACAO") == "2", "22 a 27 semanas")
         .when(F.col("GESTACAO") == "3", "28 a 31 semanas")
         .when(F.col("GESTACAO") == "4", "32 a 36 semanas")
         .when(F.col("GESTACAO") == "5", "37 a 41 semanas")
         .when(F.col("GESTACAO") == "6", "42 semanas e mais")
         .otherwise("Ignorado")
    )

    # Tipo de Gravidez (Item 12)
    # 1- Única, 2-Dupla, 3- Tripla ou mais
    df = df.withColumn("gravidez_desc",
        F.when(F.col("GRAVIDEZ") == "1", "Única")
         .when(F.col("GRAVIDEZ") == "2", "Dupla")
         .when(F.col("GRAVIDEZ") == "3", "Tripla ou mais")
         .otherwise("Ignorado")
    )

    return df

# ==========================================
# MAIN
# ==========================================

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True, help="Nome do dataset (ex: sinasc)")
    p.add_argument("--date", required=True, help="Data de partição (dt=YYYY-MM-DD)")
    p.add_argument("--bucket", default="landing")
    p.add_argument("--prefix", default="source_sus")
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres-olap:5432/olap_db")
    p.add_argument("--pg-user", default="olap")
    p.add_argument("--pg-password", default="olap")
    p.add_argument("--table-prefix", default="")
    args = p.parse_args()

    spark = SparkSession.builder.appName("ETL_SINASC_FactNascimentos").getOrCreate()

    # 1. Listar arquivos
    if args.bucket == "local":
        base_path = f"{args.prefix}/{args.dataset}/dt={args.date}/*.csv"
        files = glob.glob(base_path)
    else:
        base_path = f"s3a://{args.bucket}/{args.prefix}/{args.dataset}/dt={args.date}/*.csv"
        files = _list_paths_with_glob(spark, base_path)

    if not files:
        print(f"Sem arquivos em {base_path}.")
        return

    print(f"Encontrados {len(files)} arquivo(s) para processar")

    # 2. Ler Dimensões
    jdbc_opts = {
        "url": args.pg_url, "user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"
    }
    
    # Broadcast das dimensões pequenas
    dim_data = F.broadcast(get_jdbc_df(spark, "dimData", jdbc_opts))
    
    # Prepara dimHorario com string HH:MM:SS para join
    dim_horario = F.broadcast(
        get_jdbc_df(spark, "dimHorario", jdbc_opts) \
        .withColumn("tempo_str", F.format_string("%02d:%02d:%02d", F.col("hora"), F.col("minutos"), F.col("segundos")))
    )

    dim_municipio = F.broadcast(get_jdbc_df(spark, "dimMunicipio", jdbc_opts))
    # Cria coluna de join com 6 digitos
    dim_municipio = dim_municipio.withColumn("cod_mun_6", F.floor(F.col("codigo_ibge") / 10).cast("int"))

    dim_demografia = F.broadcast(get_jdbc_df(spark, "dimDemografia", jdbc_opts))
    dim_info_nasc = F.broadcast(get_jdbc_df(spark, "dimInfoNascimento", jdbc_opts))

    # 3. Processar arquivos
    print("\n" + "="*60)
    print("INICIANDO PROCESSAMENTO DOS ARQUIVOS (SINASC)")
    print("="*60 + "\n")

    for file_idx, file_path in enumerate(files, 1):
        print(f"Processando arquivo {file_idx}/{len(files)}: {file_path}")
        
        # Leitura
        df_raw = spark.read.option("header", "true").option("sep", ";").option("inferSchema", "false").csv(file_path)
        if df_raw.head(1) == 0: continue
        
        # Transformação
        df_clean = transform_sinasc_raw(df_raw)
        df_clean = df_clean.repartition(2)

        # ----------------------------------------
        # JOINS COM DIMENSÕES
        # ----------------------------------------

        # 1. Data Nascimento
        df_joined = df_clean.join(
            dim_data.select(F.col("data").alias("data_nascimento"), F.col("chave_data")),
            on="data_nascimento", how="left"
        )

        # 2. Tempo Nascimento
        df_joined = df_joined.join(
            dim_horario.select("tempo_str", "chave_tempo"),
            df_joined.tempo_nascimento == F.col("tempo_str"),
            how="left"
        ).drop("tempo_str")

        # 3. Municípios (Nascimento e Residência)
        # Reutiliza o DF de municipio filtrado
        dim_mun_lookup = dim_municipio.select(F.col("cod_mun_6"), F.col("chave_municipio"))

        # Mun Nascimento
        df_joined = df_joined.join(
            dim_mun_lookup.withColumnRenamed("chave_municipio", "chave_municipio_nascimento"),
            df_joined.cod_mun_nasc == F.col("cod_mun_6"),
            how="left"
        ).drop("cod_mun_6")

        # Mun Residencia
        df_joined = df_joined.join(
            dim_mun_lookup.withColumnRenamed("chave_municipio", "chave_municipio_residencia"),
            df_joined.cod_mun_res == F.col("cod_mun_6"),
            how="left"
        ).drop("cod_mun_6")

        # 4. Demografia (MÃE)
        # O fato nascimento liga-se à demografia da MÃE. 
        # Portanto, o sexo na dimensão deve ser 'F' e usamos os dados da mãe.
        df_joined = df_joined.join(
            dim_demografia.withColumnRenamed("sexo", "sexo_dim"),
            (F.lit("F") == F.col("sexo_dim")) & # Força sexo feminino (Mãe)
            (df_joined.raca_mae_desc == dim_demografia.raca) &
            (df_joined.estciv_mae_desc == dim_demografia.estado_civil) &
            (df_joined.esc_mae_desc == dim_demografia.escolaridade) &
            (
                (df_joined.idade_mae.isNotNull() & (df_joined.idade_mae >= dim_demografia.idade_minima)) |
                (df_joined.idade_mae.isNull() & dim_demografia.idade_minima.isNull())
            ) &
            (
                 dim_demografia.idade_maxima.isNull() | 
                (df_joined.idade_mae <= dim_demografia.idade_maxima)
            ),
            how="left"
        )

        # 5. Info Nascimento (BEBÊ + GESTAÇÃO)
        df_joined = df_joined.join(
            dim_info_nasc.withColumnRenamed("sexo", "sexo_rn_dim"),
            (df_joined.sexo_rn_desc == F.col("sexo_rn_dim")) &
            (df_joined.raca_rn_desc == dim_info_nasc.raca_cor) &
            (df_joined.parto_desc == dim_info_nasc.tipo_parto) &
            (df_joined.gestacao_desc == dim_info_nasc.tempo_gestacao) &
            (df_joined.gravidez_desc == dim_info_nasc.tipo_gravidez) &
            (
                (df_joined.peso_gramas.isNotNull() & (df_joined.peso_gramas >= dim_info_nasc.peso_min_gramas)) |
                (df_joined.peso_gramas.isNull() & dim_info_nasc.peso_min_gramas.isNull())
            ) &
            (
                dim_info_nasc.peso_max_gramas.isNull() |
                (df_joined.peso_gramas <= dim_info_nasc.peso_max_gramas)
            ),
            how="left"
        )

        # ----------------------------------------
        # TRATAMENTO DE NULOS E AGREGAÇÃO
        # ----------------------------------------
        
        # Preencher chaves nulas com 0 (Ignorado) ou manter NULL se preferir descartar
        # Assumindo estratégia de não perder dados:
        fill_values = {
            "chave_municipio_nascimento": 0,
            "chave_municipio_residencia": 0,
            "chave_demografia": 0, # Assumindo que existe ID 0 na dimensão (se script de população criar)
            "chave_info_nascimento": 0,
            "chave_tempo": -1 # dimHorario tem -1 para nulo no init.sql
        }
        df_joined = df_joined.fillna(fill_values)
        
        # Remove registros onde a data é inválida (chave_data null) pois é PK
        df_joined = df_joined.filter(F.col("chave_data").isNotNull())

        keys = [
            "chave_data",
            "chave_tempo",
            "chave_municipio_nascimento",
            "chave_municipio_residencia",
            "chave_demografia",
            "chave_info_nascimento"
        ]

        # Agregação
        df_agg = df_joined.groupBy(*keys).count().withColumnRenamed("count", "quantidade_nascimentos")

        # Escrita
        print(f"Escrevendo {df_agg.count()} registros na factNascimentos...")
        df_agg.write.format("jdbc").options(**jdbc_opts).option("dbtable", "factNascimentos").mode("append").save()
        
        # Limpeza memória
        df_raw.unpersist()
        df_clean.unpersist()

    print("\nPROCESSAMENTO SINASC CONCLUÍDO!")
    spark.stop()

if __name__ == "__main__":
    main()