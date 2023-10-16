import mysql.connector
import sys
sys.path.append('/Users/max/Drive/VS_Projects/cap_project/con')
#from config import db_config
from app_config import db_config


def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    

