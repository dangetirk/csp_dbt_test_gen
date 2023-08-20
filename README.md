
# DBT Test Generator

This script generates tests YAML files based on CSV input files. It's designed to help automate the process of generating tests for your data transformations.

## Getting Started

1. Clone the repository:
   git clone https://github.com/your-username/dbt-test-generator.git

2. Place your CSV input files in the `Input` folder. The CSV files should follow the format:
   table_name,column_name,datatype,size,mandatory_check,accepted_values

3. Run the script to generate DBT tests YAML files:
   python3 source_test.py

4. The generated DBT tests YAML files will be placed in the `output` folder.

## Requirements

- Python 3.x

## Usage

1. Edit the `source_test.py` script if needed:
   - Modify the `datatype_mapping` and add more datatype mappings if necessary.
   - Customize the default values for `database` and `schema` according to your project.
   - Adjust the input and output folder paths as needed.

2. Run the script by executing the following command:
   python3 source_test.py

3. The script will process the CSV files in the `Input` folder, generate corresponding DBT tests YAML files in the `output` folder, and display messages indicating the progress.
