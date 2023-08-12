from data_join import JoinData


def test_data_join(spark_session):
    assert spark_session is not None

file_path_users = 'exercises/pyspark/03_data_join/users.csv'
file_path_purchases = 'exercises/pyspark/03_data_join/purchases.csv'

def test_data_load(spark_session):
    file_join = JoinData(file_path_users, file_path_purchases)

    assert file_join.df_users.count() == 3
    assert file_join.df_purchases.count() == 4

def test_data_join(spark_session):
    file_join = JoinData(file_path_users, file_path_purchases)

    file_join.data_join()
    
    assert file_join.df_join.count() == 4
    assert file_join.df_join.filter(col('user_id') == 2).collect()[0]['name'] == 'Alice'
    assert file_join.df_join.filter(col('name') == 'John').count() == 2

def test_data_join_aggregation(spark_session):
    file_join = JoinData(file_path_users, file_path_purchases)

    file_join.data_join_aggregation()

    assert file_join.df_join.count() == 3
    assert file_join.df_join.filter(col('user_id') == 1).collect()[0]['total_spending'] == 75
    assert file_join.df_join.filter(col('user_id') == 2).collect()[0]['purchase_count'] == 1
    assert file_join.df_join.filter(col('user_id') == 3).collect()[0]['average_spending'] == 30
