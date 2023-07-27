# How to Use The Script

This script reads a CSV file that contains information about database tables and their columns. For each column, it generates a list of tests based on the given conditions. The output of these tests is written to a YAML file.

Here are the steps to use this script:

1. Prepare a CSV file with the following columns:
    - `table_name`: The name of the table.
    - `column_name`: The name of the column in the table.
    - `datatype`: The datatype of the column. Currently, the script only handles the STRING datatype.
    - `size`: The maximum length of the column, only applicable if the datatype is STRING.
    - `MandatoryCheck`: A flag indicating whether the column must be not-null. Use 'Yes' for mandatory columns, and 'No' for non-mandatory ones.
   
2. Save the CSV file and note its path. You will need this to run the script.

3. In the Python script, replace `'table.csv'` in the line `input_file = 'table.csv'` with the path to your CSV file.

4. Run the script. It will generate a YAML file named `output.yml` in the same directory. The YAML file will contain the generated dbt tests.

Note: To run this script, you must have the `csv` and `yaml` libraries installed in your Python environment. If not, install them using pip:


pip install pyyaml


# Setup Instructions

Follow the steps below to setup your environment:

1. Install Python. This script has been tested with Python 3, but it should work with Python 2 as well. You can download Python from [here](https://www.python.org/downloads/).

2. Install pip. Pip is a package manager for Python. You can find installation instructions [here](https://pip.pypa.io/en/stable/installation/).

3. Install the necessary libraries. Open a terminal and type in the following command:

pip install pyyaml

4. Save the Python script provided in your question to a file, e.g., python source_test.py.

5. You can now run the script using Python:

python source_test.py

