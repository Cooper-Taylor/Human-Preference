import labelbox as lb
import pandas as pd
import os
from random import shuffle
import string
import ast

'''
Use this function to upload the data with appended LLM responses to labelbox using conversation text data row formats.
Link to Doc: https://docs.labelbox.com/reference/import-conversation-model-response-data
'''

import string

def normalize_string(s):
    # Remove leading and trailing whitespace
    s = s.strip()
    # Capitalize the first letter and make other letters lowercase
    s = s.capitalize()
    # Ensure there's a period at the end if there's no punctuation
    if (s != "None"):
        if s and s[-1] not in string.punctuation:
            s += '.'

    return s

def upload_dataset(api_key_loc, data_path):
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
    
    # Iterate through dataframe rows appending objecs to data_rows
    for index, row in df.iterrows():
        if len(row["label"][0]) != 0:
            # Unsure if labelbox randomizes order of responses, so shuffle
            responses = [{"labeler" : f"{row["label_author"]}", "data" : ast.literal_eval(row["label"])},
                          {"labeler" : "ChatGPT4o", "data" : ast.literal_eval(row["frames"])}]
            shuffle(responses)

            # Standardize the response strings

            responses[0]["data"] = '\n'.join([normalize_string(s) for s in responses[0]["data"]])
            responses[1]["data"] = '\n'.join([normalize_string(s) for s in responses[1]["data"]])


            row_data = {"type": "application/vnd.labelbox.conversational",
                    "version": 1,
                    "messages": [
                        {"messageId" : f"message-{row["global_key"]}",
                        "content" : row["row_data"],
                        "user": {
                            "userId" : f"User {row["global_key"]}",
                            "name" : row["post_author"]
                        },
                        "align": "left",
                        "canLabel": False
                        }
                    ],
                    "modelOutputs": [
                        {
                            "title" : "Response A",
                            "content" : responses[0]["data"],
                            "modelConfigName" : responses[0]["labeler"]
                        },
                        {
                            "title" : "Response B",
                            "content" : responses[1]["data"],
                            "modelConfigName": responses[1]["labeler"]
                        }
                    ]
            }

            # Append row_data to data_rows
            # Because of the nature of the project global keys, add _hp to download them
            data_rows.append({
                "row_data" : row_data,
                "global_key" : str(row["global_key"]) + '_hp'
            })


    task = dataset.create_data_rows(data_rows)

    task.wait_till_done()
    print("Errors:",task.errors)
    print("Failed data rows:", task.failed_data_rows)