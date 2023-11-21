# from db import MyDB
from cli import DatabaseCLI

def main():
    cli = DatabaseCLI()
    cli.run()

if __name__ == "__main__":
    main()



# if __name__ == '__main__':
#     db = MyDB()
#     while True:
#         command = input("MyDB > ")
#         if command.lower() == 'exit':
#             break
#         elif command.startswith('create'):
#             try:
#                 db.create_table('employees', {'id': 'int', 'name': 'string', 'age': 'int', 'department': 'string'})
#             except Exception as e:
#                 print(f"Create Error: {e}")
#         elif command.startswith('insert'):
#             try:
#                 db.insert('employees', {'id': '1', 'name': 'John', 'age': '30', 'department': 'HR'})
#             except ValueError as e:
#                 print(f"Insert Error: {e}")
#         elif command.startswith('delete'):
#             db.delete('employees', "name = John")
#
#
