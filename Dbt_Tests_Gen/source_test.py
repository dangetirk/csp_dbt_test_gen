import csv
import yaml

def generate_dbt_tests(input_file):
    dbt_tests = {
        'version': 2,
        'sources': [
            {
                'name': 'staging',
                'database': 'dmn01-rsksoi-bld-01-2017',
                'schema': 'dmn01_rsksoi_euwe2_rsk_csp_ds_curation',
                'tables': []
            }
        ]
    }

    with open(input_file, 'r') as file:
        reader = csv.DictReader(file)
        tables = {}

        for row in reader:
            table_name = row['table_name']
            column_name = row['column_name']
            datatype = row['datatype']
            size = row['size']
            mandatory_check = row['MandatoryCheck']

            if table_name not in tables:
                tables[table_name] = {
                    'name': table_name,
                    'columns': []
                }

            column_tests = []

            if mandatory_check == 'Yes':
                column_tests.append('not_null')

            if datatype == 'STRING':
                if size:
                    column_tests.append({
                        'length_check': {
                            'max_length': int(size)
                        }
                    })
                column_tests.append({
                    'dbt_expectations.expect_column_values_to_be_of_type': {
                        'column_type': 'String'
                    }
                })

            if column_tests:
                tables[table_name]['columns'].append({
                    'name': column_name,
                    'tests': column_tests
                })

        dbt_tests['sources'][0]['tables'] = list(tables.values())

    return dbt_tests

input_file = 'table.csv'
output = generate_dbt_tests(input_file)

output_file = 'output.yml'
with open(output_file, 'w') as file:
    yaml.dump(output, file, sort_keys=False)
