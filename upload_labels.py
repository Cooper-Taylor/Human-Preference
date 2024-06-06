import labelbox as lb
import pandas as pd
import os

'''
Use this function to upload the data with appended LLM responses to labelbox using conversation text data row formats.
Link to Doc: https://docs.labelbox.com/reference/import-conversation-model-response-data
'''

def upload_data(projectID, api_key_loc, data_path):
    # Find api key using api_key_loc if path exists
    if not os.path.exists(api_key_loc):
        error_message = f'API key not found at {api_key_loc}'
        raise FileNotFoundError(error_message)

    with open(api_key_loc, 'r') as file:
        labelbox_api_key = file.read().replace('\n', '')
    
    # Save to environment variable
    os.environ["LABELBOX_API_KEY"] = labelbox_api_key
    print("LabelBox API key loaded.")

    # Create LabelBox Client and project
    client = lb.Client(api_key=os.environ["LABELBOX_API_KEY"])

    df = pd.read_csv(data_path)

    dataset = client.create_dataset(
        name="Human_Preference",
        iam_integration=None
    )
    
    # Will be list of datarow objects with global key
    data_rows = []
    
    # Iterate through dataframe rows appending 
    for index, row in df.iterrows():
        row_data = {"type": "application/vnd.labelbox.conversational",
                "version": 1,
                "messages": [
                    {"messageId" : f"message-{row["global_key"]}",
                     "content" : row["row_data"],
                     "user": {
                         "userId" : f"User {row["global_key"]}"
                     }}
                ],
                "modelOutputs": []
        }

