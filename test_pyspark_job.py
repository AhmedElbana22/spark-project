import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-clean-data")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def result(spark):
    data = [
        (1, "Alice", 100.0),   # valid
        (2, "Bob", 50.0),      # valid
        (3, "Carol", 0.0),     # amount = 0 -> removed
        (4, "Dave", -20.0),    # negative -> removed
        (5, None, 80.0),       # NULL name -> removed
        (6, "Frank", 10.0),    # valid
    ]
    df = spark.createDataFrame(data, ["id", "name", "amount"])
    return clean_data(df)


def test_valid_records_are_kept(result):
    ids = sorted(r["id"] for r in result.collect())
    assert ids == [1, 2, 6]


def test_amount_less_or_equal_zero_removed(result):
    assert result.filter("amount <= 0").count() == 0
    ids = [r["id"] for r in result.collect()]
    assert 3 not in ids and 4 not in ids


def test_null_names_removed(result):
    assert result.filter("name IS NULL").count() == 0
    assert 5 not in [r["id"] for r in result.collect()]


def test_amount_with_tax_calculated_correctly(result):
    rows = {r["id"]: r for r in result.collect()}
    assert rows[1]["amount_with_tax"] == pytest.approx(120.0)
    assert rows[2]["amount_with_tax"] == pytest.approx(60.0)
    assert rows[6]["amount_with_tax"] == pytest.approx(12.0)
