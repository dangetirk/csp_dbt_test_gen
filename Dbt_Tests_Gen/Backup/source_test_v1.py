import argparse
import csv
import yaml

def generate_dbt_tests(input_file, database, schema, source_name):
    # Default column mappings
    default_table_column_map = {
        'table_name': 0,
        'column_name': 1,
        'datatype': 2,
        'size': 3,
        'mandatory_check': 4
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
        headers = next(reader)  # Read the header row to get column positions

        tables = {}

        for row in reader:
            # Get column values using default column mappings
            table_name = row[default_table_column_map['table_name']]
            column_name = row[default_table_column_map['column_name']]
            datatype = row[default_table_column_map['datatype']]
            size = row[default_table_column_map['size']]
            mandatory_check = row[default_table_column_map['mandatory_check']]
            
            if table_name not in tables:
                tables[table_name] = {
                    'name': table_name,
                    'columns': []
                }

            column_tests = []

            if mandatory_check == 'N':  # Add 'not_null' test only for MandatoryCheck 'N'
                column_tests.append({'not_null': {"name": f"Not Null Check for {column_name}"}})

            if datatype == 'STRING':
                if size:
                    column_tests.append({
                        'length_check': {
                            'name': f"Length Check for {column_name}",
                            'max_length': int(size)
                        }
                    })
                column_tests.append({
                    'dbt_expectations.expect_column_values_to_be_of_type': {
                        'name': f"Datatype Check for {column_name}",
                        'column_type': 'String'
                    }
                })

            elif datatype == 'INTEGER':
                column_tests.append({
                    'dbt_expectations.expect_column_values_to_be_of_type': {
                        'name': f"Datatype Check for {column_name}",
                        'column_type': 'Integer'
                    }
                })

            elif datatype == 'NUMERIC':
                column_tests.append({
                    'dbt_expectations.expect_column_values_to_be_of_type': {
                        'name': f"Datatype Check for {column_name}",
                        'column_type': 'Numeric'
                    }
                })

            if column_tests:
                tables[table_name]['columns'].append({
                    'name': column_name,
                    'tests': column_tests
                })

        dbt_tests['sources'][0]['tables'] = list(tables.values())

    return dbt_tests

def main():
    parser = argparse.ArgumentParser(description="Generate dbt tests from CSV input file.")
    parser.add_argument("input_file", help="Path to the input CSV file.")
    parser.add_argument("--database", required=True, help="Database name.")
    parser.add_argument("--schema", required=True, help="Schema name.")
    parser.add_argument("--source-name", required=True, help="Source name.")
    args = parser.parse_args()

    input_file = args.input_file
    database = args.database
    schema = args.schema
    source_name = args.source_name

    output = generate_dbt_tests(input_file, database, schema, source_name)

    output_file = 'output.yml'
    with open(output_file, 'w') as file:
        yaml.dump(output, file, sort_keys=False)

if __name__ == "__main__":
    main()
