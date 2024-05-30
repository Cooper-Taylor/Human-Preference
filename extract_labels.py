import labelbox as lb
import os

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
    
    print("Data successfully loaded")


    





    

