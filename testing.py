import os

path = '/usr/local/airflow/test/larson/feedwatch'

print(os.path.join(path.split('/')[-2:][0], path.split('/')[-2:][1]))

