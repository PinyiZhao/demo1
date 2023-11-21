import re

class SQLParser:
    def parse(self, query):
        query = query.strip()
        if query.lower().startswith("select"):
            return self.parse_select(query)
        # Add more cases for other types of queries (INSERT, UPDATE, etc.)
        else:
            raise ValueError("Unsupported SQL query type.")

    def parse_select(self, query):
        # Extended regular expression to include aggregate functions
        pattern = r"""
            select\s+(.*?)\s+from\s+(\w+)           # Select and From clauses
            (?:\s+join\s+(\w+)\s+on\s+(.*?))?       # Optional JOIN clause
            (?:\s+where\s+(.*?))?                   # Optional WHERE clause
            (?:\s+group\s+by\s+(.*?))?              # Optional GROUP BY clause
            (?:\s+order\s+by\s+(.*?))?              # Optional ORDER BY clause
            $                                       # End of string
        """
        match = re.match(pattern, query, re.IGNORECASE | re.VERBOSE)
        if not match:
            raise ValueError("Invalid SQL SELECT query format.")

        select_clause, from_table, join_table, join_condition, where_clause, group_by_clause, order_by_clause = match.groups()

        # Handle aggregate functions in the select clause
        columns = self.parse_select_clause(select_clause)

        query_data = {
            "type": "select",
            "columns": columns,
            "table": from_table,
            "join": {
                "table": join_table,
                "condition": join_condition
            } if join_table else None,
            "condition": where_clause.strip() if where_clause else None,
            "group_by": group_by_clause.strip() if group_by_clause else None
        }

        order_by_clause = match.group(7).strip() if match.group(7) else None

        query_data['order_by'] = order_by_clause

        return query_data

    def parse_select_clause(self, select_clause):
        """
        Parse the select clause to handle normal and aggregated columns.
        """
        columns = []
        for part in select_clause.split(','):
            part = part.strip()
            if part.lower().startswith(("count(", "sum(", "avg(")):  # Add more aggregate functions as needed
                # Extracting aggregate function and column
                function, column = re.match(r"(\w+)\((\w+)\)", part).groups()
                columns.append({'function': function, 'column': column})
            else:
                columns.append({'column': part})
        return columns

# Example usage
# parser = SQLParser()
# parsed_query = parser.parse("SELECT * FROM table1 JOIN table2 ON table1.id = table2.id")
# print(parsed_query)
