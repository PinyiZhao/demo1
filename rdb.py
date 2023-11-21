import csv
import os
import re
from collections import defaultdict
import json
import heapq
import os


CHUNK_SIZE = 20


def evaluate_condition(row, condition, headers):
    operators = ['<', '<=', '=', '>', '>=', '!=']
    
    # Check if any of the operators is present in the condition
    operator_used = None
    for operator in operators:
        if operator in condition:
            operator_used = operator
            break

    if operator_used:
        condition_column, condition_value = condition.split(operator_used)
        condition_column_index = headers.index(condition_column)

        # Get the value from the row for the condition column
        row_value = row[condition_column_index]

        # Evaluate the condition based on the operator
        if operator_used == '<':
            return float(row_value) < float(condition_value)
        elif operator_used == '<=':
            return float(row_value) <= float(condition_value)
        elif operator_used == '=':
            if isinstance(row_value, str):
                return row_value.lower() == condition_value.lower()
            else:
                return row_value == condition_value
        elif operator_used == '>':
            return float(row_value) > float(condition_value)
        elif operator_used == '>=':
            return float(row_value) >= float(condition_value)
        elif operator_used == '!=':
            return row_value != condition_value

    return False  # Condition is not valid
    
def read_csv_in_chunks_join(file_path, chunk_size=20):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        chunk = []
        headers = next(reader)  # First row is headers
        for i, row in enumerate(reader, start=1):
            chunk.append(row)
            if i % chunk_size == 0:
                yield headers, chunk
                chunk = []
        if chunk:
            yield headers, chunk
            
def remove_csv_files():    
    try:
        files = os.listdir('.')

        # Remove each file if it is a CSV file
        for file in files:
            try :
                if file.startswith('temp'):
                    os.remove(file)
            except OSError:
                pass    
    except FileNotFoundError:
        pass
             
class Database:
    def __init__(self):
        remove_csv_files()
        self.tables = {}
        self.temp_table_count = 0
        self.previous_temp_file = ""
        self.load_table_mapping()
        print(self.tables)
        
    def load_table_mapping(self):
        for filename in os.listdir(os.getcwd()):
            if filename.endswith('.csv'):
                table_name = filename[:-4]  # Remove '.csv' from filename
                with open(filename, 'r') as file:
                    reader = csv.reader(file)
                    first_two_rows = list(reader)[:2]
                    header_row = first_two_rows[0]
                    data_types_row = first_two_rows[1]
                    self.tables[table_name] = {}
                    self.tables[table_name]['schema'] = dict(zip(header_row, data_types_row))             
                    
    def display_tables(self):
        return '\n'.join(self.tables.keys())
    
    def create_table(self, table_name, schema):
        if table_name in self.tables:
            return f"Table {table_name} already exists."
        # Schema format: [('name', 'str'), ('age', 'int')]
        self.tables[table_name] = {'schema': {col: dtype for col, dtype in schema}, 'rows': []}
        with open(f"{table_name}.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([col for col, _ in schema])
            writer.writerow([col for _, col in schema])
        return f"{table_name}.csv created successfully"

    def insert_into(self, table_name, values):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."

        schema = self.tables[table_name]['schema']
        if len(values) != len(schema):
            return "Column count doesn't match value count."

        # Data type validation
        for val, (col, dtype) in zip(values, schema.items()):
            if dtype == 'int':
                if not str(val).isdigit():
                    return f"Value for {col} should be an integer."
            elif dtype == 'str':
                if not isinstance(val, str):
                    return f"Value for {col} should be a string."

        # Write to CSV
        with open(f"{table_name}.csv", 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(values)
        return f"{table_name}.csv"
    
    def delete_from(self, table_name, conditions):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."


        updated_data = []
        with open(f"{table_name}.csv", 'r', newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read the header row
            data_types = next(reader)  # Read the data types row
            updated_data.append(headers)  # Preserve the header row
            updated_data.append(data_types)  # Preserve the data types row
            for row in reader:
                row_updated = False
                # Evaluate all conditions for each row
                conditions_met = all(evaluate_condition(row, condition, headers) for condition in conditions)
                if not conditions_met:
                    # Update the specified columns with the new values
                    updated_data.append(row)

        with open(f"{table_name}.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(updated_data)

        count = len(updated_data)
        return f"{table_name}.csv"
    
    def update_set(self, table_name, conditions, new_values):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."

        updated_data = []
        with open(f"{table_name}.csv", 'r', newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read the header row
            data_types = next(reader)  # Read the data types row
            updated_data.append(headers)  # Preserve the header row
            updated_data.append(data_types)  # Preserve the data types row
            for row in reader:
                row_updated = False
                # Evaluate all conditions for each row
                conditions_met = all(evaluate_condition(row, condition, headers) for condition in conditions)
                if conditions_met:
                    # Update the specified columns with the new values
                    for new_value in new_values:
                        col, new_val = new_value.split('=')
                        col_index = headers.index(col)
                        row[col_index] = new_val
                    row_updated = True
                updated_data.append(row)
                
        with open(f"{table_name}.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(updated_data)

        return f"{table_name}.csv"
    
    def select_from(self, table_name, conditions=None, columns=None, join=None, groupby=None, order_by=None):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."
        file_path = f"{table_name}.csv"
        columns_with_agg = columns.copy()
        columns_without_agg = [col.split('(')[1].strip(')') if '(' in col else col for col in columns]
        
        if join:
            self.join_tables(table_name, columns_without_agg, join)
            file_path = self.previous_temp_file
    
        selected_data = []
        header_processed = False

        for chunk in read_csv_in_chunks(file_path, CHUNK_SIZE):
            if not header_processed:
                header = {column: index for index, column in enumerate(chunk[0])}
                data_types = chunk[1]
                if columns_without_agg is None or columns_without_agg[0] == "*":
                    selected_columns_indices = list(range(len(chunk[0])))
                else:
                    selected_columns_indices = [header[column] for column in columns_without_agg]
                selected_data.append([chunk[0][index] for index in selected_columns_indices])  # Preserve the selected header
                selected_data.append([data_types[index] for index in selected_columns_indices])  # Preserve the data types
                header_processed = True
                chunk = chunk[2:]  # Remove header and data types rows
            
            headers = list(header.keys())
            for row in chunk:
                if conditions:
                    conditions_met = all(evaluate_condition(row, condition, headers) for condition in conditions)
                    if conditions_met:
                        selected_row = [row[index] for index in selected_columns_indices]
                        selected_data.append(selected_row)
                else:
                    # If no conditions, add row directly
                    selected_row = [row[index] for index in selected_columns_indices]
                    selected_data.append(selected_row)
        out_put_file = f'temp_{self.temp_table_count}.csv'
        self.previous_temp_file = out_put_file
        self.temp_table_count += 1  
        with open(out_put_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(selected_data)                  
        self.perform_groupby(columns_with_agg, groupby)
        self.orderby_csv(order_by)
        # Convert the selected data to a formatted string
        # selected_rows = [", ".join(row) for row in selected_data]

        # return "\n".join(selected_rows)
        return self.previous_temp_file

    def display_table(self, table_name):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."
        # with open(f"{table_name}.csv", 'r') as file:
        #     reader = csv.reader(file)
        #     return "\n".join([", ".join(row) for row in reader])
        return f"{table_name}.csv"

    def read_csv_in_chunks(file_path, chunk_size=CHUNK_SIZE):
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            chunk = []
            for i, row in enumerate(reader):
                if i == 0:
                    headers = row  # Capture headers to yield with each chunk
                    continue
                chunk.append(row)
                if (i % chunk_size == 0) or (i == 0):
                    yield headers, chunk
                    chunk = []
            if chunk:
                yield headers, chunk
                
            
    def perform_groupby(self, columns, groupby, chunk_size=CHUNK_SIZE):
        if not groupby or not columns:
            return
        # Function to write data to CSV
        def write_csv(file_path, data, fieldnames):
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
        columns.sort()
        groupby.sort()
        # Parse the input columns for aggregation functions
        agg_functions = {}
        output_fieldnames = list(groupby)  # Start with groupby fields
        for col in columns:
            if '(' in col and ')' in col:
                func, col_name = col.split('(')
                col_name = col_name.strip(')')
                agg_col_name = f"{func}({col_name})"  # Format as 'func(col_name)'
                if col_name in groupby:
                    raise ValueError(f"Column '{col_name}' cannot be both in groupby and an aggregation function")
                agg_functions[col_name] = func
                output_fieldnames.append(agg_col_name)
            elif col not in groupby:
                output_fieldnames.append(col)  # Non-aggregated fields

        # Initialize grouping and aggregation structures
        grouped_data = {}

        # Process each chunk of data
        for headers, chunk in read_csv_in_chunks_group(self.previous_temp_file, chunk_size=chunk_size):
            # Convert chunk to list of dictionaries for easier processing
            dict_chunk = [dict(zip(headers, row)) for row in chunk]

            # Perform grouping and aggregation for each chunk
            for row in dict_chunk:
                group_key = tuple(row[g] for g in groupby)
                if group_key not in grouped_data:
                    grouped_data[group_key] = {col: [] for col in agg_functions}
                
                for agg_col in agg_functions:
                    try:
                        grouped_data[group_key][agg_col].append(float(row[agg_col]))  # Assuming numeric values
                    except ValueError:
                        # Handle non-numeric values, skip if not convertible
                        pass

        # Aggregate the grouped data
        for key, values in grouped_data.items():
            for col, vals in values.items():
                if agg_functions[col] == 'sum':
                    grouped_data[key][col] = sum(vals)
                elif agg_functions[col] == 'avg':
                    grouped_data[key][col] = sum(vals) / len(vals) if vals else 0
                elif agg_functions[col] == 'max':
                    grouped_data[key][col] = max(vals) if vals else None
                elif agg_functions[col] == 'min':
                    grouped_data[key][col] = min(vals) if vals else None

        # Prepare data for writing to CSV
        output_data = []
        for key, values in grouped_data.items():
            row = dict(zip(groupby, key))
            for col, val in values.items():
                row[f"{agg_functions[col]}({col})"] = val
            output_data.append(row)

        # Write to the output CSV file
        output_csv_path = f'temp_{self.temp_table_count}.csv'
        write_csv(output_csv_path, output_data, output_fieldnames)
        self.previous_temp_file = output_csv_path
        self.temp_table_count += 1            
                  
    def join_tables(self, table_name, columns, join_clause):
        if not join_clause:
            return
        # Parse the join clause
        join_table, on_clauses = join_clause.split(' on ')
        join_conditions = [cond.split('=') for cond in on_clauses.split(',')]

        # Read main table and join table in chunks
        main_table_path = f'{table_name}.csv'
        join_table_path = f'{join_table}.csv'

        headers_written = False

        for main_headers, main_chunk in read_csv_in_chunks_join(main_table_path, CHUNK_SIZE/2):
            for join_headers, join_chunk in read_csv_in_chunks_join(join_table_path, CHUNK_SIZE/2):
                # Perform the join for each chunk pair
                joined_data = []
                for main_row in main_chunk:
                    for join_row in join_chunk:
                        if all(main_row[main_headers.index(main_col.split('.')[1])] == join_row[join_headers.index(join_col.split('.')[1])] for main_col, join_col in join_conditions):
                            # Construct the joined row based on the selected columns
                            joined_row = {}
                            for col in columns:
                                table, col_name = col.split('.')
                                if table == table_name:
                                    col_index = main_headers.index(col_name)
                                    joined_row[col] = main_row[col_index]
                                else:
                                    col_index = join_headers.index(col_name)
                                    joined_row[col] = join_row[col_index]
                            joined_data.append(joined_row)

                # Write output to a CSV file
                out_put_file = f'temp_{self.temp_table_count}.csv'
                with open(out_put_file, 'a', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=columns)
                    if not headers_written:
                        writer.writeheader()
                        headers_written = True
                    writer.writerows(joined_data)
                    
        self.previous_temp_file = out_put_file
        self.temp_table_count += 1

        return out_put_file  # Return the path to the output file for reference
    
    def orderby_csv(self, orderby_columns, chunk_size=CHUNK_SIZE):
        if not orderby_columns:
            return
        # Determine the indices of the orderby columns
        file_path = self.previous_temp_file
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            data_types = next(reader)
            orderby_indices = [headers.index(col) for col in orderby_columns]

        sorted_data = []
        first_chunk = True
        for chunk in read_csv_in_chunks(file_path, chunk_size):
            # Skip headers and data types for subsequent chunks
            if first_chunk:
                first_chunk = False
                chunk = chunk[2:]

            # Sort the chunk
            chunk.sort(key=lambda row: tuple(row[index] for index in orderby_indices))
            sorted_data.extend(chunk)

        # Write the sorted data to a file
        output_file_path = f'temp_{self.temp_table_count}.csv'
        with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)  # Write headers
            writer.writerow(data_types)  # Write data types
            writer.writerows(sorted_data)
        self.previous_temp_file = output_file_path
        self.temp_table_count += 1
        
    def merge_sort_csv(self, orderby_columns, chunk_size=CHUNK_SIZE):
        if not orderby_columns:
            return

        file_path = self.previous_temp_file
        temp_files = []
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            data_types = next(reader)
            orderby_indices = [headers.index(col) for col in orderby_columns]

            # Sort and write each chunk to a temporary file
            for chunk in read_csv_in_chunks(file_path, chunk_size):
                chunk.sort(key=lambda row: tuple(row[index] for index in orderby_indices))
                temp_file_name = f'temp_{len(temp_files)}.csv'
                with open(temp_file_name, 'w', newline='', encoding='utf-8') as temp_file:
                    csv.writer(temp_file).writerows(chunk)
                temp_files.append(temp_file_name)

        # Merge sorted chunks
        output_file_path = f'temp_{self.temp_table_count}.csv'
        with open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
            # Write headers and data types
            writer = csv.writer(outfile)
            writer.writerow(headers)
            writer.writerow(data_types)

            # Merge all files
            files = [open(fname, 'r', newline='', encoding='utf-8') for fname in temp_files]
            readers = [csv.reader(f) for f in files]
            sorted_rows = heapq.merge(*readers, key=lambda row: tuple(row[index] for index in orderby_indices))
            writer.writerows(sorted_rows)

        # Clean up temporary files
        for f in files:
            f.close()
            os.remove(f.name)

        self.previous_temp_file = output_file_path
        self.temp_table_count += 1

class Parser:
    # Usage: 
    def parse(self, command):
        tokens = command.lower().split()
        if tokens[0] == "make" and tokens[1] == "table":
            # Format: make table table_name column_name1:data_type1, column_name2:data_type2
            table_name = tokens[2]
            columns = [(col.split(':')[0], col.split(':')[1]) for col in tokens[3].split(',')]
            return ("create_table", table_name, columns)
        elif tokens[0] == "add" and tokens[1] == "into":
            # Format: add into table_name value1, value2, value3
            table_name = tokens[2]
            values = tokens[3].split(',')
            return ("insert_into", table_name, values)
        elif tokens[0] == "show" and tokens[1] == "table":
            # Format: show table table_name
            table_name = tokens[2]
            return ("display_table", table_name)
        elif tokens[0] == "delete" and tokens[1] == "from" and tokens[3] == "that":
            # Format: delete from table_name where column_name=value
            table_name = tokens[2]
            return ("delete_from", table_name, tokens[4].split(','))
        elif tokens[0] == "update" and tokens[4] == "to":
            # Format: update table_name that column_name1=value1,column_name2=value2 to column_name1=value1,column_name2=value2
            table_name = tokens[1]
            return ("update_set", table_name, tokens[3].split(','), tokens[5].split(','))
        elif tokens[0] == "select" and tokens[1] == "from" and tokens[3] == "that":
            # Format: select from table_name that column_name1=value1,column_name2=value2
            table_name = tokens[2]
            return ("select_from", table_name, tokens[4].split(','))
        elif tokens[0] == "select":
            return self.parse_select_query(tokens)
        else:
            return ("unknown",)
        
    def parse_select_query(self, tokens):
        # Initialize default values for optional parts
        condition_clause, join_clause, groupby_clause, orderby_clause = None, None, None, None

        # Parse 'select' clause
        select_index = tokens.index("from")
        select_clause = tokens[1:select_index]
        select_clause = [item.strip() for item in " ".join(select_clause).split(',')]

        # Parse 'from' clause
        from_index = tokens.index("from")
        table_name = tokens[from_index + 1]

        # Check for 'join' clause and 'that' as part of 'join'
        if "join" in tokens:
            join_index = tokens.index("join")
            join_end_index = tokens.index("groupby") if "groupby" in tokens else tokens.index("orderby") if "orderby" in tokens else len(tokens)
            join_clause = " ".join(tokens[join_index + 1:join_end_index])
            # Extract 'that' clause from join, if present
            if "that" in join_clause:
                join_parts = join_clause.split("that")
                join_clause = join_parts[0].strip()
                condition_clause = join_parts[1].strip()
        
        # Check for standalone 'that' clause (conditions)
        elif "that" in tokens:
            that_index = tokens.index("that")
            that_end_index = tokens.index("groupby") if "groupby" in tokens else tokens.index("orderby") if "orderby" in tokens else len(tokens)
            condition_clause = " ".join(tokens[that_index + 1:that_end_index])

        # Check for 'groupby' clause
        if "groupby" in tokens:
            groupby_index = tokens.index("groupby")
            groupby_end_index = tokens.index("orderby") if "orderby" in tokens else len(tokens)
            groupby_clause = " ".join(tokens[groupby_index + 1:groupby_end_index])

        # Check for 'order by' clause
        if "orderby" in tokens:
            orderby_index = tokens.index("orderby")
            orderby_clause = " ".join(tokens[orderby_index + 1:])

        return {
            "type": "select",
            "from": table_name,
            "conditions": condition_clause.split(',') if condition_clause else None,
            "columns": select_clause,
            "join": join_clause,
            "groupby": groupby_clause.split(',') if groupby_clause else None,
            "orderby": orderby_clause.split(',') if orderby_clause else None    
        }





    
def cli():
    db = Database()
    parser = Parser()

    while True:
        command = input("MyDB > ").strip()
        if command.lower() == "exit":
            break
        elif command.lower().startswith("show tables"):
            print(db.display_tables())
            continue
        result = None
        action = parser.parse(command)
        if isinstance(action, dict):
            result = db.select_from(table_name=action['from'], conditions=action['conditions'], columns=action['columns'], join=action['join'], groupby=action['groupby'], order_by=action['orderby'])
        else:
            if action[0] == "create_table":
                result = db.create_table(action[1], action[2])
            elif action[0] == "insert_into":
                result = db.insert_into(action[1], action[2])
            elif action[0] == "display_table":
                result = db.display_table(action[1])
            elif action[0] == "delete_from":
                result = db.delete_from(action[1], action[2])
            elif action[0] == "update_set":
                result = db.update_set(action[1], action[2], action[3])
            elif action[0] == "select_from":
                result = db.select_from(action[1], action[2])
            else:
                result = "Unknown command or not yet implemented."
        print(result)
        # return results to api
        
class API:
    def __init__(self, *args, **kwargs):
        print("YSH API init")
    
    def csv_to_json(self,file_path):
        print("csv_to_json")
        with open(file_path, 'r', encoding='utf-8') as file:
            # Read the first line as headers
            headers = next(file).strip().split(',')

            # Skip the second line (data types)
            next(file)

            # Read the remaining data
            reader = csv.DictReader(file, fieldnames=headers)
            data_list = [row for row in reader]

            return json.dumps(data_list, indent=4)
    
    def handle_multiple_commands(self,input_command):
        print("handle_multiple_commands")
        input_commands = input_command.split(';')
        count = 0
        for command in input_commands:
            if command:
                command = command.replace('\n', '')
                print(command)
                self.handle_input(command)
                count += 1
                
        return f"{count} commands executed successfully."
            
            
    def handle_input(self,input_command):
        if ';' in input_command:
            return self.handle_multiple_commands(input_command)

        try:  
            print(input_command)
            db = Database()
            parser = Parser()

            command = input_command.strip()

            if command.lower().startswith("show tables"):
                print(db.display_tables())

            result = None
            action = parser.parse(command)
            if isinstance(action, dict):
                result = db.select_from(table_name=action['from'], conditions=action['conditions'], columns=action['columns'], join=action['join'], groupby=action['groupby'], order_by=action['orderby'])
                return self.csv_to_json(result)
            else:
                if action[0] == "create_table":
                    result = db.create_table(action[1], action[2])
                    result = "Create Success!"
                elif action[0] == "insert_into":
                    result = db.insert_into(action[1], action[2])
                    result = "Add Success!"
                elif action[0] == "display_table":
                    result = self.csv_to_json(db.display_table(action[1]))
                elif action[0] == "delete_from":
                    result = db.delete_from(action[1], action[2])
                    result = "Delete Success!"
                elif action[0] == "update_set":
                    result = db.update_set(action[1], action[2], action[3])
                    result = "Update Success!"
                elif action[0] == "select_from":
                    pirnt("select_from")
                    result = db.select_from(action[1], action[2])
                    return self.csv_to_json(result)
                else:
                    result = "Unknown command or not yet implemented."
            return result
            
        except Exception as e:
            print(e)
            return "Invalid Command!"
    

def read_csv_in_chunks(file_path, chunk_size=CHUNK_SIZE):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        chunk = []
        for i, row in enumerate(reader):
            chunk.append(row)
            if (i + 1) % chunk_size == 0:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
            
def read_csv_in_chunks_group(file_path, chunk_size=1024):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        chunk = []
        for i, row in enumerate(reader):
            if i == 0:
                headers = row  # Capture headers to yield with each chunk
                continue
            chunk.append(row)
            if (i % chunk_size == 0) or (i == 0):
                yield headers, chunk
                chunk = []
        if chunk:
            yield headers, chunk

if __name__ == "__main__":
    cli()
    
    # api("make table persons name:str,email:str,country:str,age:int")
    # api("add into persons Shihao,shihaoy@usc.edu,China,24")
    # api("add into persons Ar,ar@usc.edu,China,23")
    # api("add into persons Leo,leo@usc.edu,China,26")
    # api("add into persons Rajan,rajan@usc.edu,India,40")
    # api("add into persons Ram,ram@usc.edu,India,30")
    # api("add into persons Kord,kord@usc.edu,USA,20")
    # api("add into persons Shihao,sh.yu@usc.edu,China,24")
    # api("add into persons Shihao,shihao@usc.edu,China,24")

    # api("make table students name:str,age:int,course:str")
    # api("add into students LEO,15,CSCI570")
    # api("add into students Shihao,23,DSCI551")
    # api("add into students Ar,20,CSCI585")
    # api("add into students Ar,23,DSCI551")
    # api("add into students Ar,12,CSCI572")
    
    # api("make table employees name:str,age:int,hour_rate:int")
    # api("add into employees Shihao,24,40")
    # api("add into employees Ar,23,50")
    
    # api("select employees.name,employees.age,students.name,sum(students.age) from employees join students on employees.name=students.name,employees.age=students.age that employees.name=aar groupby students.name,employees.name,employees.age orderby sum(students.age)")
    # api("make table employees_demo id:int,first_name:str,last_name:str,department_id:int,salary:int")
    # api("add into employees_demo 1,John,Doe,101,60000")
    # api("select name,email from persons")
    # api("select * from persons")
    # api("select name,email,age from persons that name=Shihao")
    # api("update persons that age=25 to age=26")
    # api("update persons that age=20,name=kord to age=21,name=Xiaohu")
    # api("update persons that age=40,name=Rajan to age=21,name=najAR")
    # api("delete from persons that name=Xiaohu")
    # api("select persons.name,persons.email,students.course,students.name from persons join students on persons.name=students.name")
    # api("select persons.name,persons.email,students.course from persons join students on persons.name=students.name that persons.email=shihaoy@usc.edu")
    # api("select persons.name,students.course,max(students.age) from persons join students on persons.name=students.name that students.course=DSCI551 groupby persons.name,students.course orderby max(students.age)")
    # api("select persons.name,employees.name,persons.email,employees.hour_rate from persons join employees on employees.name=students.name that employees.hour_rate>40")