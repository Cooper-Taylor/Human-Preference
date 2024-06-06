import labelbox as lb
import os
import json
import pandas as pd
import re
from random import randint

# Convert json to dataframe
def normalize_json(file_path, project_id):
    """
    Takes raw output from LabelBox API, gathers the key, tweet content, and human labeled
    frames. Stores and saves this information into a .csv file

    Parameters:
    - file_path (string): Path to the name of the JSON file containing the raw output
        from the extract_and_save_labels function.
    - project_id (string): Project ID for the project being worked with. Required to access
        information in JSON file.
    """
    # Parse through file to get necessary information (key, post, human frames)
    df_data = {
        "global_key" : [],
        "post_author" : [],
        "label_author" : [],
        "row_data" : [],
        "label" : []
    }

    with open(file_path, 'r', encoding="utf-8") as f:
        for dictionary in (json.load(f))["data"]:
            df_data["global_key"].append(dictionary["data_row"]["global_key"])
            df_data["post_author"].append(dictionary["metadata_fields"][0]["value"])
            df_data["row_data"].append(dictionary["data_row"]["row_data"])

            human_frames = []

            # Account for consensus-labeled frames
            annotation = dictionary["projects"][project_id]["labels"]

            # Randomly choose one author of consensus labels
            idx = randint(0, len(annotation) - 1)

            # Get label_author
            df_data["label_author"].append(annotation[idx]["label_details"]["created_by"])

            if len(annotation[idx]["annotations"]["classifications"]):
                # As per labeling instructions, multiple frames labeled by an individual should be separated by a new line character
                if len(annotation[idx]["annotations"]["classifications"][0]["text_answer"]["content"]):
                    for frame in annotation[idx]["annotations"]["classifications"][0]["text_answer"]["content"].split('\n'):
                        human_frames.append(frame)

            df_data["label"].append(human_frames)

    # Combine into dataframe
    df = pd.DataFrame(df_data)

    # Normalize the human frames
    df["label"] = df["label"].apply(lambda x: ', '.join(x))
    df["label"] = df["label"].apply(lambda x: re.sub(r'http\S+', 'http:URL', x))
    df["label"] = df["label"].apply(lambda x: re.sub(r'@\S+', '@USER', x))
    df["label"] = df["label"].apply(lambda x: re.sub(r'none', "None", x))
    df["label"] = df["label"].apply(lambda x: x.split(', '))

    # Save df as csv
    print("Saving dataframe...")
    df.to_csv(file_path[:-5] + '_modified.csv', index=False)



def extract_and_save_labels(output_dir,
                            extraction_projectID,
                            api_key_loc,
                            export_params):
    
    print(output_dir)

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

    params = {}
    filters = {"workflow_status": "InReview"}

    # Format export parameters as required by labelbox API
    # https://docs.labelbox.com/reference/label-export#export-v2-methods
    for param in export_params.split(';'):
        params[param] = True
    
    project = client.get_project(extraction_projectID)
    export_task = project.export_v2(params=params, filters=filters)

    export_task.wait_till_done()

    if export_task.errors:
        print(export_task.errors)

    # Define callback function
    def file_stream_handler(output: lb.FileConverterOutput):
        print(
            f"offset: {output.current_offset}, progress: {output.bytes_written}/{output.total_size}, "
            f"path: {output.file_path.absolute().resolve()}"
        )

    # Save json file to specified output directory
    if export_task.has_result():
        export_task.get_stream(
            converter=lb.FileConverter(file_path=output_dir)
        ).start(stream_handler=file_stream_handler)

    # For some reason, the file_stream does not follow .json format
    # Wrap the text file in another dictionary

    with open(output_dir, 'r', encoding="utf-8") as f:
        dicts = []
        for line in f:
            dicts.append(json.loads(line))

        data_dict = {"data":dicts}
    
    # Save new dictionary
    with open(output_dir, "w") as f:
        json.dump(data_dict, f)

    normalize_json(output_dir, extraction_projectID)

    print("Data successfully loaded")

