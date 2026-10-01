from pyspark.sql.functions import col


def clean_data(df):
    """
    - Remove rows where amount <= 0
    - Remove rows where name is NULL
    - Add amount_with_tax = amount * 1.20
    """
    return (
        df.withColumn("amount", col("amount").cast("double"))
        .filter(col("amount") > 0)
        .filter(col("name").isNotNull())
        .withColumn("amount_with_tax", col("amount") * 1.20)
    )
