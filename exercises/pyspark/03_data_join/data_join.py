"""
As a Data Engineer at a rapidly growing e-commerce company, you are given two CSV files: users.csv and purchases.csv.
The users.csv file contains details about your users, and the purchases.csv file holds records of all purchases made.

Your team is interested in gaining a deeper understanding of customer behavior.
A question they’re particularly interested in is: "What is the total spending of each customer?"

Your task is to extract meaningful information from these data sets to answer this question.
The output of your work should be a JSON file, output.json.
"""

'''
Data loading:
- for this exercise, I will assume data type consistency and demonstrate the use of schemas
'''


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, avg, sum, count


class JoinData:

    users_schema = StructType([
        StructField('user_id', IntegerType(), False),
        StructField('name', StringType(), True)
    ])
    purchases_schema = StructType([
        StructField('user_id', IntegerType(), False),
        StructField('price', FloatType(), False)
    ])

    def __init__(self, file_path_users, file_path_purchases):
        self.spark = SparkSession.builder.appName('JoinData').getOrCreate()

        self.df_users = self.spark.read.csv(file_path_users, header=True).dropDuplicates(['user_id'])
        self.df_purchases = self.spark.read.csv(file_path_purchases, header=True)


    def data_join(self):
        self.df_join = self.df_users.join(
            self.df_purchases,
            how='inner',
            on=['user_id']
            ).select(self.df_users['user_id'], self.df_users['name'], self.df_purchases['price'])


    def data_join_aggregation(self):
        self.data_join()

        self.df_join = self.df_join.groupBy('user_id', 'name').agg(
            sum('price').alias('total_spending'),
            count('user_id').alias('purchase_count'),
            avg('price').alias('average_spending')
        )

        self.df_join.write.mode('overwrite').json('output.json')
