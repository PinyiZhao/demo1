import csv
import json
import os
import re
from queryParser import SQLParser

class MyDB:
    def __init__(self, metadata_file='metadata.json', data_dir='tables'):
        self.metadata_file = metadata_file
        self.data_dir = data_dir
        self.metadata = self.load_metadata()

    def load_metadata(self):
        """Load existing metadata from the file."""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as file:
                return json.load(file)
        else:
            return {}
    def save_metadata(self):
        """Save the current metadata to the file."""
        with open(self.metadata_file, 'w') as file:
            json.dump(self.metadata, file, indent=4)

    def save_table_data(self, table_name, data):
        """Save data to a table's JSON file."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        with open(table_file, 'w') as file:
            json.dump(data, file, indent=4)

    def create_table(self, table_name, schema):
        """Create a new table with the given schema."""
        if table_name in self.metadata:
            return (f"Table '{table_name}' already exists.")

        # Update metadata with the new table schema
        self.metadata[table_name] = {'schema': schema}
        self.save_metadata()  # Save updated metadata

        # Create an empty JSON file for the new table
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        with open(table_file, 'w') as file:
            json.dump([], file)  # Initialize the table with an empty list

        print(f"Table '{table_name}' created successfully.")
        return f"Table '{table_name}' created successfully."

    def insert(self, table_name, row):
        """Insert a new row into the specified table."""
        if table_name not in self.metadata:
            return f"Table '{table_name}' does not exist."

        # Validate and convert row data based on table schema
        table_schema = self.metadata[table_name]['schema']
        validated_row = self.validate_and_convert_row(row, table_schema)

        # Load existing data, insert the new row, and save
        table_data = self.load_table_data(table_name)
        table_data.append(validated_row)
        self.save_table_data(table_name, table_data)
        print(f"Row inserted into '{table_name}' successfully.")
        return f"Row inserted into '{table_name}' successfully."


    def validate_and_convert_row(self, row, schema):
        """Validate and convert row data based on the schema."""
        validated_row = {}
        for col, data_type in schema.items():
            # Only validate and convert columns that are present in the row
            if col in row:
                try:
                    if data_type == 'int':
                        validated_row[col] = int(row[col])
                    elif data_type == 'float':
                        validated_row[col] = float(row[col])
                    elif data_type == 'string':
                        validated_row[col] = str(row[col])
                    else:
                        raise ValueError(f"Unsupported data type: {data_type}")
                except ValueError:
                    raise ValueError(f"Invalid data type for column '{col}'. Expected {data_type}, got value '{row[col]}'.")

        return validated_row

    def delete(self, table_name, condition):
        if table_name not in self.metadata:
            return f"Table '{table_name}' does not exist."

        table_data = self.load_table_data(table_name)
        schema = self.metadata[table_name]['schema']

        # Filter out rows that match the condition
        new_table_data = [row for row in table_data if not self.evaluate_condition(row, condition, schema)]

        # Save the updated data
        self.save_table_data(table_name, new_table_data)
        print(f"Rows deleted from '{table_name}' based on condition: {condition}")
        return f"Rows deleted from '{table_name}' based on condition: {condition}"

    def update(self, table_name, updates, condition):
        """
        Update rows in the specified table based on a condition.
        updates: a dictionary of column-value pairs for the update.
        condition: a string in a simple format like "column operator value".
        """
        if table_name not in self.metadata:
            return Exception(f"Table '{table_name}' does not exist.")

        table_data = self.load_table_data(table_name)
        schema = self.metadata[table_name]['schema']

        # Validate and convert updates based on schema
        validated_updates = self.validate_and_convert_row(updates, schema)

        # Update rows that match the condition
        for row in table_data:
            if self.evaluate_condition(row, condition, schema):
                for col, new_value in validated_updates.items():
                    if col in row:
                        row[col] = new_value
                    else:
                        return f"Column '{col}' does not exist in the table."

        # Save the updated data
        self.save_table_data(table_name, table_data)
        print(f"Rows updated in '{table_name}' based on condition: {condition}")
        return f"Rows updated in '{table_name}' based on condition: {condition}"



    def query(self, query_statement):
        # Instantiate SQLParser and parse the query statement
        parser = SQLParser()
        parsed_query = parser.parse(query_statement)

        if parsed_query['type'] == 'select':
            table_data = self.load_table_data(parsed_query['table'])
            schema = self.metadata[parsed_query['table']]['schema']
            if 'group_by' in parsed_query and parsed_query['group_by'] is not None:
                result = self.perform_group_by(table_data, parsed_query['group_by'], parsed_query['columns'], parsed_query.get('condition'), schema)
            else:
                # Handle simple SELECT queries
                result = self.perform_select(parsed_query)
            if 'order_by' in parsed_query:
                result = self.sort_results(result, parsed_query['order_by'])
            return result
        else:
            raise ValueError("Only SELECT queries are currently supported.")

    def sort_results(self, data, order_by_clause):
        if not order_by_clause:
            return data

        # Assuming a simple ORDER BY clause with a single column and optional ASC/DESC
        parts = order_by_clause.split()
        column = parts[0]
        ascending = True if len(parts) == 1 or parts[1].lower() == 'asc' else False

        return sorted(data, key=lambda x: x[column], reverse=not ascending)


    def perform_select(self, parsed_query):
        # Handle JOIN
        if parsed_query.get('join'):
            return self.perform_join_select(parsed_query)

        # Handle simple and aggregate select
        table_name = parsed_query['table']
        selected_columns = parsed_query['columns']
        where_condition = parsed_query.get('condition', None)
        group_by_column = parsed_query.get('group_by')

        table_data = self.load_table_data(table_name)
        schema = self.metadata[table_name]['schema']

        if group_by_column:
            # Handle GROUP BY with aggregates
            return self.perform_group_by(table_data, group_by_column, selected_columns, where_condition, schema)
        else:
            # Handle simple select
            return self.perform_simple_select(table_data, selected_columns, where_condition, schema)

    def perform_join_select(self, parsed_query):
        # Simplified JOIN operation
        table1 = parsed_query['table']
        table2 = parsed_query['join']['table']
        join_condition = parsed_query['join']['condition']
        where_condition = parsed_query.get('condition', None)
        selected_columns = parsed_query['columns']

        table1_data = self.load_table_data(table1)
        table2_data = self.load_table_data(table2)
        table1_schema = self.metadata[table1]['schema']
        table2_schema = self.metadata[table2]['schema']

        # Combine schemas for evaluating conditions
        combined_schema = {f"{table1}.{k}": v for k, v in table1_schema.items()}
        combined_schema.update({f"{table2}.{k}": v for k, v in table2_schema.items()})

        joined_data = []
        for row1 in table1_data:
            for row2 in table2_data:
                if self.evaluate_join_condition(row1, row2, join_condition):
                    combined_row = self.combine_rows(row1, row2, table1, table2)
                    if not where_condition or self.evaluate_condition(combined_row, where_condition, combined_schema):
                        selected_row = self.select_columns(combined_row, selected_columns, table1, table2)
                        joined_data.append(selected_row)

        return joined_data

    # def simple_select(self, parsed_query):
    #     """
    #     Perform a simple SELECT operation on a single table.
    #     """
    #     table_name = parsed_query['table']
    #     selected_columns = parsed_query['columns']
    #     where_condition = parsed_query.get('condition', None)
    #
    #     table_data = self.load_table_data(table_name)
    #     schema = self.metadata[table_name]['schema']
    #
    #     # Filter data based on the WHERE condition
    #     if where_condition:
    #         filtered_data = [row for row in table_data if self.evaluate_condition(row, where_condition, schema)]
    #     else:
    #         filtered_data = table_data
    #
    #     # Select specified columns
    #     if selected_columns != ['*']:
    #         return [{col: row[col] for col in selected_columns if col in row} for row in filtered_data]
    #     else:
    #         return filtered_data

    def evaluate_join_condition(self, row1, row2, condition):
        # Assume condition in the format "table1.column = table2.column"
        left, right = condition.replace(" ", "").split('=')
        left_table, left_column = left.split('.')
        right_table, right_column = right.split('.')

        # Compare the values from respective tables
        return row1[left_column] == row2[right_column]

    def combine_rows(self, row1, row2, table1, table2):
        # Prefix column names with table names to avoid conflicts
        combined_row = {f"{table1}.{k}": v for k, v in row1.items()}
        combined_row.update({f"{table2}.{k}": v for k, v in row2.items()})
        return combined_row

    def select_columns(self, row, columns, table1, table2):
        # Handle case when columns are specified as a list of dictionaries
        if len(columns) == 1 and columns[0].get('column') == '*':
            return row

        selected_row = {}
        for col_dict in columns:
            col = col_dict.get('column')

            if '.' in col:
                # Column is prefixed with table name
                selected_row[col] = row[col]
            else:
                # Default to table1 if no table prefix is provided
                selected_row[col] = row.get(f"{table1}.{col}", row.get(f"{table2}.{col}"))

        return selected_row

    def select_columns_simple(self, row, columns):
        """
        Select specified columns from a row.
        """
        if len(columns) == 1 and columns[0].get('column') == '*':
            return row

        selected_row = {}
        for col in columns:
            if col.get('column') in row:
                selected_row[col.get('column')] = row[col.get('column')]
            elif col.get('column') == '*':
                return row
            # You can add more logic here if you want to handle aggregates in a simple select

        return selected_row
    # def load_table_data(self, table_name):
    #     """
    #     Load data from the specified table's CSV file.
    #     Returns a list of dictionaries, where each dictionary represents a row.
    #     """
    #     table_file = os.path.join(self.data_dir, f"{table_name}.csv")
    #     if not os.path.exists(table_file):
    #         raise Exception(f"No data found for table '{table_name}'.")
    #
    #     with open(table_file, 'r') as file:
    #         reader = csv.DictReader(file)
    #         return [row for row in reader]

    def load_table_data(self, table_name):
        """Load data from a table's JSON file."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        if not os.path.exists(table_file):
            raise Exception(f"No data found for table '{table_name}'.")

        with open(table_file, 'r') as file:
            return json.load(file)

    def evaluate_condition(self, row_data, condition, schema):
        """
        Evaluates the condition against a row of data.
        Support conditions in various formats, e.g., 'column = value', 'column=value'.
        """
        # Regular expression to parse the condition
        match = re.match(r"(.*?)\s*([=><])\s*(.*)", condition)
        if not match:
            raise ValueError("Invalid condition format.")

        column, operator, value = match.groups()

        # Strip quotes if the value is a string literal
        if (value.startswith("'") and value.endswith("'")) or \
           (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]


        # Fetch the actual value from the row data
        actual_value = row_data[column]
        if (isinstance(actual_value, str)):
            if (actual_value.startswith("'") and actual_value.endswith("'")) or \
               (actual_value.startswith('"') and actual_value.endswith('"')):
                actual_value = actual_value[1:-1]

        # Type conversion based on schema
        if schema[column] == 'int':
            actual_value = int(actual_value)
            value = int(value)
        elif schema[column] == 'float':
            actual_value = float(actual_value)
            value = float(value)

        # Evaluate the condition
        if operator == '=':
            return actual_value == value
        elif operator == '>':
            return actual_value > value
        elif operator == '<':
            return actual_value < value
        else:
            raise ValueError(f"Unsupported operator: {operator}")


    def perform_group_by(self, table_data, group_by_column, selected_columns, where_condition, schema):
        # Group data and apply aggregate functions
        grouped_data = {}
        for row in table_data:
            if where_condition and not self.evaluate_condition(row, where_condition, schema):
                continue

            key = row[group_by_column]
            if key not in grouped_data:
                grouped_data[key] = []

            grouped_data[key].append(row)

        return self.apply_aggregates(grouped_data, selected_columns, group_by_column)

    def apply_aggregates(self, grouped_data, selected_columns, group_by_column):
        # Apply aggregate functions on grouped data
        aggregated_data = []
        for key, rows in grouped_data.items():
            aggregated_row = {group_by_column: key}
            for col_info in selected_columns:
                if 'function' in col_info:
                    col_name = col_info['column']
                    function = col_info['function'].lower()
                    aggregated_row[f"{function}({col_name})"] = self.apply_aggregate_function(rows, col_info)
                else:
                    aggregated_row[col_info['column']] = rows[0][col_info['column']]
            aggregated_data.append(aggregated_row)

        return aggregated_data

    def apply_aggregate_function(self, rows, col_info):
        # Apply the specified aggregate function
        column = col_info['column']
        values = [row[column] for row in rows]

        if col_info['function'].lower() == 'count':
            return len(values)
        elif col_info['function'].lower() == 'sum':
            return sum(values)
        elif col_info['function'].lower() == 'avg':
            return sum(values) / len(values) if values else 0
        # Add more aggregate functions as needed
        else:
            raise ValueError(f"Unsupported aggregate function: {col_info['function']}")

    def perform_simple_select(self, table_data, selected_columns, where_condition, schema):
        # Perform a simple select operation
        filtered_data = []
        for row in table_data:
            if not where_condition or self.evaluate_condition(row, where_condition, schema):
                filtered_row = self.select_columns_simple(row, selected_columns)
                filtered_data.append(filtered_row)

        return filtered_data

