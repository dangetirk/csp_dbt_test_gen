#python3 source_test.py  
import argparse
import csv
import os
import yaml
import ast

def generate_dbt_tests(input_file, database, schema, source_name):
    default_table_column_map = {
        'table_name': 0,
        'column_name': 1,
        'datatype': 2,
        'size': 3,
        'mandatory_check': 4,
        'accepted_values': 5
    }

    dbt_tests = {
        'version': 2,
        'sources': [
            {
                'name': source_name,
                'database': database,
                'schema': schema,
                'tables': []
            }
        ]
    }

    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)

        tables = {}

        for row in reader:
            table_name = row[default_table_column_map['table_name']]
            column_name = row[default_table_column_map['column_name']]
            datatype = row[default_table_column_map['datatype']]
            size = row[default_table_column_map['size']]
            mandatory_check = row[default_table_column_map['mandatory_check']]
            accepted_values_str = row[default_table_column_map['accepted_values']]
            accepted_values = ast.literal_eval(accepted_values_str) if accepted_values_str.strip() else []

            if table_name not in tables:
                tables[table_name] = {
                    'name': table_name,
                    'columns': []
                }

            column_tests = []

            if mandatory_check.lower() == 'yes':
                column_tests.append({'not_null': {"name": f"Non-Null Check for {column_name}"}})

            datatype_mapping = {
                'STRING': 'String',
                'BYTES': 'Bytes',
                'INTEGER': 'Integer',
                # Add more datatype mappings as needed
            }

            if datatype in datatype_mapping:
                column_tests.append({
                    'dbt_expectations.expect_column_values_to_be_of_type': {
                        'name': f"Datatype Check for {column_name}",
                        'column_type': datatype_mapping[datatype]
                    }
                })

                if datatype == 'STRING' and size:
                    column_tests.append({
                        'length_check': {
                            'name': f"Length Check for {column_name}",
                            'max_length': int(size)
                        }
                    })

            if accepted_values:
                column_tests.append({
                    'accepted_values': {
                        'name': f"Accepted Values Check for {column_name}",
                        'values': accepted_values
                    }
                })

            if column_tests:
                tables[table_name]['columns'].append({
                    'name': column_name,
                    'tests': column_tests
                })

        dbt_tests['sources'][0]['tables'] = list(tables.values())

    return dbt_tests

import argparse
import csv
import os
import yaml
import ast

# ... (rest of the code remains the same)

def main():
    parser = argparse.ArgumentParser(description="Generate dbt tests from CSV input files in a folder.")
    # parser.add_argument("--database", required=True, help="Database name.")
    # parser.add_argument("--schema", required=True, help="Schema name.")
    # parser.add_argument("--source-name", required=True, help="Source name.")
    args = parser.parse_args()

    input_folder = "Input"  # Specify your input folder path here
    database = "dmn01-rsksoi-bld-01-2017"  # Hardcoded database value
    schema = "dmn01_rsksoi_euwe2_rsk_csp_ds_curation"  # Hardcoded schema value
    source_name = "curation"  # Hardcoded source name value

    output_folder = "output"  # Specify your desired output folder here

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_file = os.path.join(input_folder, filename)
            output = generate_dbt_tests(input_file, database, schema, source_name)

            output_file = os.path.join(output_folder, f'{source_name.lower()}_{os.path.splitext(filename)[0]}_dbt_tests.yml')
            with open(output_file, 'w') as file:
                yaml.dump(output, file, default_flow_style=False, sort_keys=False)

            print(f"Generated DBT tests YAML file: {output_file}")

if __name__ == "__main__":
    main()

