from pyspark.sql.functions import mean, stddev, col, when
from pyspark.sql import DataFrame
from pyspark.sql import DataFrameWriter
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

##load data
def load_source_data(spark, metadata_df):

    dataframes = {}

    # Extract unique folder names from the metadata DataFrame
    folder_names = metadata_df.select("destination_filename").distinct().rdd.flatMap(lambda x: x).collect()

    # Load files for each folder name
    for folder_name in folder_names:
        file_path = f"wasbs://dlbikestorelanding@adlsbikestoreinterns.blob.core.windows.net/Mostafa_Landing/{folder_name}/"
        df = spark.read.parquet(file_path, inferSchema=True)
        dataframes[folder_name] = df
    for source_filename, df in dataframes.items():
     globals()[f"{source_filename}_df"] = df
    
    return dataframes

##Handling Outliers using Z-Score and IQR methods 
def handle_outliers(df: DataFrame, columns: list, method: str = "zscore", z_thresh: float = 3.0) -> DataFrame:

    if method == "zscore" or "Zscore" or "z score" or "Z score" or "z-score" or "Z-score":
        for column in columns:
            mean_value = df.select(mean(col(column))).collect()[0][0]
            stddev_value = df.select(stddev(col(column))).collect()[0][0]
            df = df.withColumn(
                column,
                when(
                    abs((col(column) - mean_value) / stddev_value) > z_thresh,
                    None 
                ).otherwise(col(column))
            )

    elif method == "iqr" or "IQR" or "Iqr":
        for column in columns:
            q1 = df.approxQuantile(column, [0.25], 0.01)[0]
            q3 = df.approxQuantile(column, [0.75], 0.01)[0]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            df = df.withColumn(
                column,
                when(
                    (col(column) < lower_bound) | (col(column) > upper_bound),
                    None  # Mark outliers as None (can be replaced or dropped later)
                ).otherwise(col(column))
            )

    else:
        raise ValueError("Unsupported method! Use 'zscore' or 'iqr'.")
    
    return df

##Standardize the case of a column in a DataFrame.

def standardize_name_case(df, column_name):

    lower_count = df.filter(F.lower(F.col(column_name)) == F.col(column_name)).count()
    upper_count = df.filter(F.upper(F.col(column_name)) == F.col(column_name)).count()

    if lower_count > upper_count:

        standardized_df = df.withColumn(column_name, F.lower(F.col(column_name)))
    else:

        standardized_df = df.withColumn(column_name, F.upper(F.col(column_name)))

    return standardized_df


##Handling Duplicates in a DataFrame 
def remove_duplicates(df, subset=None):
    return df.drop_duplicates(subset = subset)

##Handling nulls
def handle_nulls(df: DataFrame, metadata: DataFrame, strategy='drop', fill_value=None):
    metadata_dict = metadata.rdd.flatMap(lambda row: [(col, row[col]) for col in row.asDict().keys()]).collectAsMap()

    if strategy not in ['drop', 'fill']:
        raise ValueError("Strategy not recognized. Use 'drop' or 'fill'.")

    if strategy == 'drop':
        drop_columns = [col for col, is_PK in metadata_dict.items() if is_PK == 'Y']
        if drop_columns:

            df = df.na.drop(subset=drop_columns)
    elif strategy == 'fill':
    
        fill_columns = [col for col, is_PK in metadata_dict.items() if is_PK != 'Y']
        if fill_value is not None:
            fill_dict = {col: fill_value for col in fill_columns}
            df = df.na.fill(fill_dict)
        else:
            raise ValueError("fill_value must be specified when using 'fill' strategy.")

    return df

#validating data types and changing them if neccesary
def change_data_type(df: DataFrame, column: str, new_type: str) -> DataFrame:
  return df.withColumn(column, df[column].cast(new_type))


# Initialize SparkSession
spark = SparkSession.builder.appName("DataTypeCheck").getOrCreate()

def check_column_data_type(df, column_name):

    expected_data_type = df.schema[column_name].dataType
    
    # Define a UDF to check data type
    def is_valid_type(value):
        try:
            # Check if the value can be cast to the expected data type
            if expected_data_type == StringType():
                str(value)  # Check if it can be cast to String
            elif expected_data_type == IntegerType():
                int(value)  # Check if it can be cast to Integer
            elif expected_data_type == FloatType():
                float(value)  # Check if it can be cast to Float
            elif expected_data_type == DoubleType():
                float(value)
            elif expected_data_type == DateType():
                 float(value)
            elif expected_data_type == BooleanType():
                if str(value).lower() in ["true", "false"]:
                    return True
                else:
                    return False
            # Add more type checks as needed...
            return True
        except:
            return False
    
    # Register the UDF

    check_type_udf = udf(is_valid_type, BooleanType())

    # Apply the UDF and create a new column 'is_valid_type'
    df_with_type_check = df.withColumn("is_valid_type", check_type_udf(df[column_name]))
    
    return df_with_type_check


df = spark.createDataFrame(data, schema=schema)

# Checking data type for the 'age' column
result_df = check_column_data_type(df, "age")
result_df.show()


##Validation is not done yet, further research is needed. ex(is the start date really before the end date)
