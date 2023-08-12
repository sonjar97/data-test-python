"""
You are a Data Engineer at an online retail company that has a significant number of daily users on its website.
The company captures several types of user interaction data: including user clicks, views, and purchases.

You are given a CSV file named data.csv, which contains this information. The columns in the file are as follows: user_id, timestamp, event_type, and duration.
Your task is to perform an analysis to better understand user behavior on the website.
Specifically, your manager wants to understand the average duration of a ‘click’ event for each user.
This means you need to consider only those events where users have clicked on something on the website.

Finally, your analysis should be presented in the form of a Parquet file named output.parquet that contains two columns: user_id and avg_duration.
The challenge here is to devise the most efficient and accurate solution using PySpark to read, process, and write the data. Please also keep in mind the potential size and scale of the data while designing your solution.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import IntegerType


class TransformData:

    def __init__(self, file_path_or_data=None, columns=None):
        self.spark = SparkSession.builder.appName("TransformData").getOrCreate()

        if file_path_or_data is not None and columns is None:
            self.df = self.spark.read.csv(file_path_or_data, header=True, inferSchema=True) \
                .filter(col('event_type') == 'click') \
                .select('user_id', 'duration')

        elif file_path_or_data is not None and columns is not None:
            self.df = self.spark.createDataFrame(file_path_or_data, columns) \
                .filter(col('event_type') == 'click') \
                .select('user_id', 'duration')

    def data_cleaning(self):
        self.df = self.df.filter(
            col('user_id').cast(IntegerType()).isNotNull() &
            col('duration').cast(IntegerType()).isNotNull()
            )
        
        self.df = self.df.withColumn('user_id', col('user_id').cast(IntegerType())) \
                         .withColumn('duration', col('duration').cast(IntegerType()))

        self.df = self.df.filter(
            (col('user_id') > 0) & (col('duration') > 0)
            )

    def calculate_avg_click_duration(self):
        self.df = self.df.groupBy('user_id').agg(avg('duration').alias('avg_duration'))

    def data_transformation(self):
        self.data_cleaning()
        self.calculate_avg_click_duration()

        self.df.write.parquet('output.parquet', mode='overwrite')
