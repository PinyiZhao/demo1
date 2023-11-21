# cli.py
from db import MyDB
from queryParser import SQLParser

class DatabaseCLI:
    def __init__(self):
        self.db = MyDB()

    def run(self):
        while True:
            command = input("MyDB > ").strip()
            if command.lower() == 'exit':
                break
            self.process_command(command)

    def process_command(self, command):
        try:
            if command.startswith('create'):
                return self.create_table(command)
            elif command.startswith('insert'):
                return self.insert_data(command)
            elif command.startswith('delete'):
                return self.delete_data(command)
            elif command.startswith('update'):
                return self.update_data(command)
            elif command.startswith('select'):
                return self.query_data(command)
            # Add more commands as needed
            else:
                print("Unknown command")
        except Exception as e:
            print(f"Error: {e}")

    def create_table(self, command):
        """
        Parses a create table command and creates a table.
        Command format: create table table_name (column1 type1, column2 type2, ...)
        """
        try:
            parts = command.split(' ', 2)  # Split the command into parts
            if len(parts) < 3 or parts[0].lower() != 'create' or parts[1].lower() != 'table':
                raise ValueError("Invalid command format for create table.")

            table_name, columns_str = parts[2].split(' ', 1)

            if not columns_str.startswith('(') or not columns_str.endswith(')'):
                raise ValueError("Invalid command format. Columns must be enclosed in parentheses.")

            # Removing parentheses and splitting by comma to get individual column definitions
            columns_str = columns_str[1:-1].strip()  # Remove the parentheses and strip spaces
            column_definitions = [col.strip() for col in columns_str.split(',')]

            # Parsing each column definition to extract name and type
            schema = {}
            for col_def in column_definitions:
                col_name, col_type = col_def.split()
                schema[col_name] = col_type

            # Calling the create_table method of MyDB
            return self.db.create_table(table_name, schema)


        except ValueError as e:
            print(f"Error: {e}")
            return f"Error: {e}"
        except Exception as e:
            print(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

    def insert_data(self, command):
        """
        Parses an insert command and inserts data into a table.
        Command format: insert into table_name (column1, column2, ...) values (value1, value2, ...)
        """
        try:
            parts = command.split(' ', 3)
            if len(parts) < 4 or parts[0].lower() != 'insert' or parts[1].lower() != 'into':
                raise ValueError("Invalid command format for insert.")

            table_name, rest = parts[2], parts[3]
            columns_part, values_part = rest.split(' values ')

            if not columns_part.startswith('(') or not columns_part.endswith(')') or not values_part.startswith('(') or not values_part.endswith(')'):
                raise ValueError("Invalid command format. Columns and values must be enclosed in parentheses.")

            # Extracting column names and values
            columns = [col.strip() for col in columns_part[1:-1].split(',')]
            values = [value.strip() for value in values_part[1:-1].split(',')]

            if len(columns) != len(values):

                raise ValueError("The number of columns and values must be the same.")

            # Creating a dictionary of column-value pairs
            data = dict(zip(columns, values))

            # Calling the insert method of MyDB
            return self.db.insert(table_name, data)

        except ValueError as e:
            print(f"Error: {e}")
            return f"Error: {e}"
        except Exception as e:
            print(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

    def delete_data(self, command):
        """
        Parses a delete command and deletes data from a table.
        Command format: delete from table_name where condition
        """
        try:
            parts = command.split(' ', 3)
            if len(parts) < 4 or parts[0].lower() != 'delete' or parts[1].lower() != 'from':
                raise ValueError("Invalid command format for delete.")

            table_name, where_clause = parts[2], parts[3]
            if not where_clause.lower().startswith('where'):
                raise ValueError("Invalid command format. Expected 'where' clause.")

            condition = where_clause[6:].strip()  # Extract the condition part after 'where'

            # Calling the delete method of MyDB
            return self.db.delete(table_name, condition)

        except ValueError as e:
            print(f"Error: {e}")
            return f"Error: {e}"
        except Exception as e:
            print(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

    def update_data(self, command):
        try:
            parts = command.split(' ', 2)
            if len(parts) < 3 or parts[0].lower() != 'update':
                raise ValueError("Invalid command format for update.")

            table_name, rest = parts[1], parts[2]
            set_part, where_part = rest.split(' where ')
            set_items = set_part[4:].split(', ')  # Remove 'set ' and split

            updates = {}
            for item in set_items:
                col, val = item.split('=')
                updates[col.strip()] = val.strip()

            condition = where_part.strip()

            # Calling the update method of MyDB
            return self.db.update(table_name, updates, condition)

        except ValueError as e:
            print(f"Error: {e}")
            return f"Error: {e}"
        except Exception as e:
            print(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

    def query_data(self, command):
        """
        Parses a query command and retrieves data from a table.
        Command format: query table_name where condition
        """
        try:
            # Use the SQLParser to parse the query
            # parsed_query = SQLParser().parse(command)

            # Execute the query using MyDB
            result = self.db.query(command)
            for row in result:
                print(row)
            return result
        except Exception as e:
            print(f"Error in query execution: {e}")
            return f"Error in query execution: {e}"



# Other methods for handling different commands can be added here
