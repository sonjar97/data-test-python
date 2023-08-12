"""
As a Data Engineer at a digital marketing agency, your team uses an in-house analytics tool that tracks user activity across various campaigns run by the company.
Each user interaction, whether a click or a view, is registered as an event. The collected data, stored in a JSON file named data.json, contains information about the date of the event (event_date) and the count of events that happened on that date (event_count).

The company wants to understand the total number of user interactions that occurred each day to identify trends in user engagement.
As such, your task is to analyze this data and prepare a summary report.
Your report should include the following information:
- The date of the events (event_date).
- The total number of events that occurred on each date (total_events).
The output should be sorted in descending order based on the total number of events, and the results should be saved in a CSV file named output.csv.
"""

'''
Data loading:
- file_path_or_data: path to file or manually entered data
- by allowing the user to manually enter data, the user can more easily test different scenarios
- the user_id field is not required to solve this exercise, no need to load it into the dataframe
    - could potentially check for valid user_id

Data cleaning:
- for this exercise I have assumed that the columns do not necessarily contain only one data type
- also, event_counts that contain non-zero decimal places are filtered out
    - 4.0 --> not filtered
    - 4.1 --> filtered
'''


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import col, sum, expr


class AggregateData:

    def __init__(self, file_path_or_data=None, columns=None):

        self.spark = SparkSession.builder.appName("AggregateData").getOrCreate()

        if file_path_or_data is not None and columns is None:
            self.df = self.spark.read.option('multiline','true').json(file_path_or_data) \
                                .select('event_date', 'event_count')
        elif file_path_or_data is not None and columns is not None:
            self.df = self.spark.read.json(
                self.spark.sparkContext.parallelize(file_path_or_data)
            ).select('event_date', 'event_count')


    def data_cleaning(self):
        self.df = self.df \
            .filter(
                col('event_date').cast(DateType()).isNotNull() &
                col('event_count').cast(IntegerType()).isNotNull()) \
            .filter(
                expr('event_count = floor(event_count)')
            )
        
        self.df = self.df.withColumn('event_date', col('event_date').cast(DateType())) \
                         .withColumn('event_count', col('event_count').cast(IntegerType()))
        
        self.df = self.df.filter(col('event_count') > 0)


    def calculate_total_events_per_day(self):
        self.df = self.df.groupBy('event_date') \
                         .agg(sum('event_count').alias('total_events'))


    def data_aggregation(self):
        self.data_cleaning()
        self.calculate_total_events_per_day()

        self.df.orderBy(col('total_events').desc()) \
               .write.mode('overwrite').csv('output.csv')
