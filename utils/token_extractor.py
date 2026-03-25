import json
import base64


#returns event body as a string
def get_event_body(event):

    
    event['body'] = json.loads(event['body'])
    extract_data_from_token = False

    try: 
        extract_data_from_token = event['body']['extract_user_id_from_token']
    except Exception as e:
        print("error while getting the field extract_data_from_token: ", e)
        return json.dumps(event['body'])

    try:
        if (extract_data_from_token):  
            body = extract_token_data(event)
            return body
        else:
            return json.dumps(event['body'])

    except Exception as e:
        #returning a string will break any further code execution
        print("error while extracting user id from token: ", e)
        return "error while extracting user id from token"
    


# Print the event data
def extract_token_data(event):
    print("event:")
    print(event)
    print()
    try:
        auth_header = event['headers'].get('authorization')
        id_token = auth_header.split("Bearer ")[1]  # Extract token


        # Split the JWT token into its parts (header, payload, signature)
        parts = id_token.split('.')

        # The payload is the second part (index 1), which is Base64 encoded
        payload = parts[1]

        # Decode the payload from Base64
        decoded_payload = base64.urlsafe_b64decode(payload + '==')  # Padding can be necessary

        # Convert the decoded payload into a JSON object
        decoded_payload_json = json.loads(decoded_payload)

        # Extract the email (assuming the email is stored in the "email" field)
        email = decoded_payload_json.get("email")
        sub = decoded_payload_json.get("sub")

        # Print the email
        print(f"Email: {email}")
        print(f"sub: {sub}")

        #add user id to event body, with sub = id
        event['body']['user_id'] = sub
        event['body']['email'] = email

        event['body'] = json.dumps(event['body'])

        return event['body']
    except Exception as e:
        return f"error in extracting user id from token: {e}"