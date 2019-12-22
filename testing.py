#
# from plugins.operators import feedwatch_parser
#
# feedwatch_parser.parse_file(test=True,
#                             farm_id=123,
#                             filename='test/test_input.csv',
#                             db_engine=None)
import psycopg2

try:
    connection = psycopg2.connect(
        user="postgres",
        password="****",
        host="127.0.0.1",
        port="***",
    )
    cursor = connection.cursor()
    query = "SELECT * FROM farm_datasource;"
    cursor.execute(query)
    print("Executed")
    records = cursor.fetchall()
    print(records)
    '''
    row () () () ()
    reference
    (<id>, <farm_id>, <software_id>, <software_version>, <file_location>, <datasource_type>, <script_name>, <script_arguments>, <active>)
    '''
except (Exception, psycopg2.Error) as err:
    print(err)