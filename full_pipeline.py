# Imports
import argparse
import os
import stat
import sys

# Command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Extract Human Labeled data from LabelBox, collapse data into preexisting data, add AI generated frames to data and save.')
    parser.add_argument("output_dir", help="Path to output directory where labels will be stored.")
    parser.add_argument("input_path", help="Path to the input CSV file.")
    parser.add_argument("extraction_projectID", help="ProjectID to access and extract information.")
    parser.add_argument("--labelbox_api_key_loc", default=os.path.expanduser('~/.apikeys/labelbox_api_key.txt'), help="Location of text file containing LabelBox API key.")
    
    parser.add_argument("--extract_data", action='store_true')
    parser.add_argument("--export_parameters", default="attachments;metadata_fields;data_row_details;project_details;label_details;project_ids", help="Semicolon-separated list of labelbox export parameters.")

    parser.add_argument("--append_LLM_labels", action='store_true')

    parser.add_argument("--results_dir", default=os.path.join('.', 'output'), help="Directory where results (and intermediate temp file) will be written.")
    parser.add_argument("--labeled_data_path", default=os.path.join('.', 'data', 'labeled_data.csv'), help="Path for labeled data file.")
    parser.add_argument("--text_col", default='text', help="Name of the text column.")
    parser.add_argument("--openai_api_key_loc", default=os.path.expanduser('~/.apikeys/openai_api_key.txt'), help="Location of text file containing OpenAI API key.")
    parser.add_argument("--raw_csv_or_intermediate", default='c', help="Whether to use the input_path data file (c) or an intermediate file (i). Default (c). Only use (i) if previous frame extraction was interrupted before completion.")
    parser.add_argument("--system_prompt_loc", default=os.path.join('.', 'utils', 'oai_system_message_template.txt'), help="System prompt to give to LLM when extracting frames.")
   
    parser.add_argument("--upload_data", action='store_true')
    parser.add_argument("--labeled_data", default=os.path.join('.', 'data', 'frame_extraction_results.csv'), help="Data path of data to be uploaded to labelbox")

    return parser.parse_args()


def full_pipeline(output_dir,
                  extraction_projectID,
                  labelbox_api_key_loc,
                  extract_data,
                  export_parameters,
                  append_LLM_labels,
                  input_path,
                  results_dir,
                  labeled_data_path,
                  text_col,
                  openai_api_key_loc,
                  raw_csv_or_intermediate,
                  system_prompt_loc,
                  upload_data,
                  labeled_data):
    
    if extract_data:
        print("Extracting Data...")
        from extract_labels import extract_and_save_labels
        extract_and_save_labels(output_dir,
                            extraction_projectID,
                            labelbox_api_key_loc,
                            export_parameters)
        
    if append_LLM_labels:
        print("Append AI Labels")
        sys.path.append(r'C:\Users\coope\InternshipCode\social_media_frame_analysis')

        directory = 'C:\\Users\\coope\\InternshipCode\\human_preference\\data'

        # Check current permissions
        permissions = os.stat(directory).st_mode
        print(oct(permissions))

        # Change permissions
        os.chmod(directory, permissions | stat.S_IRWXU)

        from frame_extraction.extract_frames import process_and_save_posts
        process_and_save_posts(input_path,
                           results_dir,
                           labeled_data_path,
                           text_col,
                           openai_api_key_loc,
                           raw_csv_or_intermediate,
                           system_prompt_loc)
        
    if upload_data:
        print("Uploading Data to LabelBox")
        from upload_labels import upload_dataset
        upload_dataset(labelbox_api_key_loc, 
                       labeled_data)


if __name__ == "__main__":
    args = parse_args()
    full_pipeline(args.output_dir,
                args.extraction_projectID,
                args.labelbox_api_key_loc,
                args.extract_data,
                args.export_parameters,
                args.append_LLM_labels,
                args.input_path,
                args.results_dir,
                args.labeled_data_path,
                args.text_col,
                args.openai_api_key_loc,
                args.raw_csv_or_intermediate,
                args.system_prompt_loc,
                args.upload_data,
                args.labeled_data)