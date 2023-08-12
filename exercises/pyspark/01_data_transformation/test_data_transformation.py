'''
- I am testing both the csv file and the manually entered data
- I have chosen an incremental testing approach
    - test load
    - test load, cleaning
    - test load, cleaning, avg click duration
    - test load, cleaning, avg click duration, overall solution 
'''

from data_transfomration import TransformData


def test_data_transformation(spark_session):
    assert spark_session is not None

data = [
    ('', "2023-01-01 12:00:00", "click", 10), # invalid user_id
    (2, "2023-01-01 12:00:15", "view", 0), # invalid duration
    (1, "2023-01-01 12:00:30", "click", 'n/a'), # invalid duration
    (3, "2023-01-01 12:01:00", "click", 5), # valid
    (-1, "2023-01-01 12:01:10", "view", 25) # invalid user_id
]

columns = ["user_id", "timestamp", "event_type", "duration"]

file_path = 'exercises/pyspark/01_data_transformation/data.csv'

def test_data_load(spark_session):
    file_csv = TransformData(file_path)
    data_csv = TransformData(data, columns)

    assert file_csv.df.count() == 3
    assert data_csv.df.count() == 3

def test_data_cleaning(spark_session):
    file_csv = TransformData(file_path)
    data_csv = TransformData(data, columns)

    file_csv.data_cleaning()
    data_csv.data_cleaning()

    assert file_csv.df.count() == 3
    assert data_csv.df.count() == 1

def test_calculate_avg_click_duration(spark_session):
    file_csv = TransformData(file_path)
    data_csv = TransformData(data, columns)

    file_csv.calculate_avg_click_duration()
    data_csv.calculate_avg_click_duration()

    assert file_csv.df.filter(col('user_id') == 1).collect()[0]['avg_duration'] == 6.5
    assert data_csv.df.filter(col('user_id') == 3).collect()[0]['avg_duration'] == 5

def test_data_transformation(spark_session):
    file_csv = TransformData(file_path)
    data_csv = TransformData(data, columns)

    file_csv.data_transformation()
    data_csv.data_transformation()

    assert file_csv.df.count() == 3
    assert data_csv.df.count() == 1
    assert file_csv.df.filter(col('user_id') == 1).collect()[0]['avg_duration'] == 6.5
    assert data_csv.df.filter(col('user_id') == 3).collect()[0]['avg_duration'] == 5
