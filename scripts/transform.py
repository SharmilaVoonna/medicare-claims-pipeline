"""Step 2 - Transform with PySpark.

Reads raw CSVs, applies types, parses dates, removes duplicates,
separates bad rows into a rejects folder, and writes clean Parquet to data/staged.
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import RAW_DIR, STAGED_DIR


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("claims-transform")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def to_date(col: str):
    # CMS stores dates as YYYYMMDD numbers, e.g. 20090115
    return F.to_date(F.col(col).cast("string"), "yyyyMMdd")


def transform_beneficiary(spark: SparkSession) -> DataFrame:
    df = (
        spark.read.option("header", True).csv(str(RAW_DIR / "beneficiary" / "*.csv"))
        .withColumn("source_file", F.input_file_name())
    )
    return (
        df.select(
            F.col("DESYNPUF_ID").alias("member_id"),
            # Snapshot year comes from the file name, e.g. DE1_0_2009_Beneficiary...
            F.regexp_extract("source_file", r"(?i)DE1_0_(\d{4})_", 1).cast("int").alias("snapshot_year"),
            to_date("BENE_BIRTH_DT").alias("birth_date"),
            to_date("BENE_DEATH_DT").alias("death_date"),
            F.col("BENE_SEX_IDENT_CD").cast("int").alias("sex_code"),
            F.col("SP_STATE_CODE").alias("state_code"),
            F.col("BENE_COUNTY_CD").alias("county_code"),
            F.col("BENE_HI_CVRAGE_TOT_MONS").cast("int").alias("part_a_months"),
            F.col("BENE_SMI_CVRAGE_TOT_MONS").cast("int").alias("part_b_months"),
            (F.col("SP_DIABETES").cast("int") == 1).alias("has_diabetes"),
            (F.col("SP_CHF").cast("int") == 1).alias("has_chf"),
            (F.col("SP_COPD").cast("int") == 1).alias("has_copd"),
        )
        .filter(F.col("member_id").isNotNull())
        .dropDuplicates(["member_id", "snapshot_year"])
    )


def read_claims(spark: SparkSession, dataset: str, claim_type: str) -> DataFrame:
    df = spark.read.option("header", True).csv(str(RAW_DIR / dataset / "*.csv"))
    return df.select(
        F.col("CLM_ID").alias("claim_id"),
        F.col("SEGMENT").cast("int").alias("segment"),
        F.lit(claim_type).alias("claim_type"),
        F.col("DESYNPUF_ID").alias("member_id"),
        F.col("PRVDR_NUM").alias("provider_id"),
        F.col("AT_PHYSN_NPI").alias("attending_npi"),
        to_date("CLM_FROM_DT").alias("claim_from_date"),
        to_date("CLM_THRU_DT").alias("claim_thru_date"),
        F.col("ICD9_DGNS_CD_1").alias("primary_diagnosis"),
        F.col("CLM_PMT_AMT").cast("decimal(12,2)").alias("paid_amount"),
    )


def transform_claims(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    claims = read_claims(spark, "inpatient", "INPATIENT").unionByName(
        read_claims(spark, "outpatient", "OUTPATIENT")
    )

    # Rows that break basic rules go to rejects instead of silently disappearing
    reject_reason = (
        F.when(F.col("claim_id").isNull(), "missing claim_id")
        .when(F.col("member_id").isNull(), "missing member_id")
        .when(F.col("claim_from_date").isNull(), "invalid from date")
        .when(F.col("claim_thru_date") < F.col("claim_from_date"), "thru date before from date")
    )
    flagged = claims.withColumn("reject_reason", reject_reason)

    rejects = flagged.filter(F.col("reject_reason").isNotNull())
    clean = (
        flagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        .dropDuplicates(["claim_id", "segment", "claim_type"])
        .withColumn("claim_year", F.year("claim_from_date"))
    )
    return clean, rejects


def main() -> int:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bene = transform_beneficiary(spark)
    bene.write.mode("overwrite").parquet(str(STAGED_DIR / "beneficiary"))
    print(f"beneficiary rows staged: {bene.count():,}")

    claims, rejects = transform_claims(spark)
    claims.write.mode("overwrite").partitionBy("claim_year").parquet(str(STAGED_DIR / "claims"))
    rejects.write.mode("overwrite").parquet(str(STAGED_DIR / "claims_rejects"))
    print(f"claims rows staged:      {claims.count():,}")
    print(f"claims rows rejected:    {rejects.count():,}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
