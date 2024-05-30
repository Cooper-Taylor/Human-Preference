# Imports
import argparse
import os
import json
import pandas

# Command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Extract Human Labeled data from LabelBox, collapse data into preexisting data, add AI generated frames to data and save.')
    parser.add_argument("output_dir", help="Path to output directory where labels will be stored.")
    parser.add_argument("extraction_projectID", help="ProjectID to access and extract information.")
    parser.add_argument("--uploading_project_name", help="Project name to upload LLM and human-labeled frames")
    parser.add_argument("--api_key_loc", default=os.path.expanduser('~/.apikeys/labelbox_api_key.txt'), help="Location of text file containing LabelBox API key.")
    
    parser.add_argument("--extract_data", action='store_true')
    parser.add_argument("--export_parameters", default="attachments;metadata_fields;embeddings;data_row_details;project_details;label_details;performance_details;interpolated_frames;project_ids;model_run_ids", help="Semicolon-separated list of labelbox export parameters.")

    parser.add_argument("--append_LLM_labels", action='store_true')
    parser.add_argument("--upload_data", action='store_true')

    return parser.parse_args()


def full_pipeline(output_dir,
                  extraction_projectID,
                  api_key_loc,
                  extract_data,
                  export_parameters,
                  append_LLM_labels,
                  upload_data):
    
    if extract_data:
        print("Extracting Data...")
        from extract_labels import extract_and_save_labels
        extract_and_save_labels(output_dir,
                            extraction_projectID,
                            api_key_loc,
                            export_parameters)


if __name__ == "__main__":
    args = parse_args()
    full_pipeline(args.output_dir,
                  args.extraction_projectID,
                  args.api_key_loc,
                  args.extract_data,
                  args.export_parameters,
                  args.append_LLM_labels,
                  args.upload_data)