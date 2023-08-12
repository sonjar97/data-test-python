'''
- I am testing both the json file and the manually entered data
- I have chosen an incremental testing approach
    - test load
    - test load, cleaning
    - test load, cleaning, total events per day
    - test load, cleaning, total events per day, overall solution
'''


from data_aggregation import AggregateData


def test_spark_session(spark_session):
    assert spark_session is not None

file_path = 'exercises/pyspark/02_data_aggregation/data.json'

data = [
    '{"user_id": "1", "event_date": "2023-08-01", "event_count": 10}', # valid
    '{"user_id": "2", "event_date": "2023-13-02", "event_count": 20}', # invalid event_date
    '{"user_id": "3", "event_date": "2023-08-03", "event_count": 2.9}', # invalid event_count
    '{"user_id": "4", "event_date": "2023-08-01", "event_count": 4.0}' # valid
]

columns = ['user_id', 'event_date', 'event_count']

def test_data_load(spark_session):
    file_json = AggregateData(file_path)
    data_json = AggregateData(data, columns)

    assert file_json.df.count() == 4
    assert data_json.df.count() == 4

def test_data_cleaning(spark_session):
    file_json = AggregateData(file_path)
    data_json = AggregateData(data, columns)

    file_json.data_cleaning()
    data_json.data_cleaning()

    assert file_json.df.count() == 4
    assert data_json.df.count() == 2

def test_calculate_total_events_per_day(spark_session):
    file_json = AggregateData(file_path)
    data_json = AggregateData(data, columns)

    file_json.data_cleaning()
    file_json.calculate_total_events_per_day()
    data_json.data_cleaning()
    data_json.calculate_total_events_per_day()
    
    assert file_json.df.filter(col('event_date') == '2022-01-01').collect()[0]['total_events'] == 18
    assert data_json.df.filter(col('event_date') == '2023-08-01').collect()[0]['total_events'] == 14

def test_data_aggregation(spark_session):
    file_json = AggregateData(file_path)
    data_json = AggregateData(data, columns)

    file_json.data_aggregation()
    data_json.data_aggregation()

    assert file_json.df.count() == 2
    assert file_json.df.filter(col('event_date') == '2022-01-02').collect()[0]['total_events'] == 17
    assert data_json.df.count() == 1
    assert data_json.df.filter(col('event_date') == '2023-08-01').collect()[0]['total_events'] == 14
