import mysql.connector
import json
from datetime import datetime
from decimal import Decimal
from utils.token_extractor import get_event_body

global connector
global cursor

# Function to initialize SQL database connection
def initialise_sql_database_connection():
    global connector
    global cursor

    credentials = {}
    with open("database_credentials.json", 'r') as file:
        credentials = json.load(file)

    DB_HOST = credentials['endpoint']
    DB_USER = credentials['admin_name']  
    DB_PASSWORD = credentials['password']  
    DB_NAME = credentials['db_name']  

    try:
        connector = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connector.cursor()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Function to retrieve data from the test_connection table
def test_connection_to_database():  

    try:
        query = "SELECT * FROM test_connection"  
        cursor.execute(query) 
        result = cursor.fetchall()
        
        return {
            'statusCode': 200,
            'body': json.dumps({'data from sql db': result})
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }



# Function to check the status of a ride request
def get_user_data(user_id):

    try:
        query = """
        SELECT * FROM Users WHERE id = %s
    """
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if result:
            # Get column names from cursor.description
            column_names = [desc[0] for desc in cursor.description]

            # Convert result tuple to dictionary
            user_data = dict(zip(column_names, result))

            # Convert Decimal values to float (if any)
            if 'CO2_savings' in user_data and user_data['CO2_savings'] is not None:
                user_data['CO2_savings'] = float(user_data['CO2_savings'])

            for key, value in user_data.items():
                if isinstance(value, datetime):
                    user_data[key] = value.isoformat()  # Converts to "YYYY-MM-DDTHH:MM:SS"

                if isinstance(value, Decimal):
                    user_data[key] = float(value)  # Converts to "YYYY-MM-DDTHH:MM:SS"


            return {
                'statusCode': 200,
                'body': json.dumps({'user_data': user_data})
            }
        


        else:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'User not found'})
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e) + 'occured in the LF'})
        }


# Lambda handler function
def lambda_handler(event, context):
    #return {'message': 'in get_user_data function'}

    try:
        print("Event received:", event)

        # Extract and parse body
        body = event.get('body', '{}')
        if isinstance(body, str):
            body_as_str = get_event_body(event)
            body = json.loads(body_as_str)
            body['requester_id'] = body['user_id']

        # Extract user_id
        user_id = body.get('user_id')
        if not user_id:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Missing user_id parameter'})}

        # Fetch CO2 savings
        return get_user_data(user_id)

    except Exception as e:
        print("Error:", e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

# Initialize database connection at Lambda cold start
initialise_sql_database_connection()
