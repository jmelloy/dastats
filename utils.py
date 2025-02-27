import sqlite3
import argparse
import tempfile
import os
import re


def get_table_info(db_path, table_name):
    """
    Get table information using PRAGMA table_info()
    Returns column information and table constraints
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get column information
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    # Get foreign key constraints
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    foreign_keys = cursor.fetchall()

    # Get index information (for UNIQUE constraints)
    cursor.execute(f"PRAGMA index_list({table_name})")
    indexes = cursor.fetchall()

    index_details = {}
    for idx in indexes:
        index_name = idx[1]
        is_unique = idx[2]

        cursor.execute(f"PRAGMA index_info({index_name})")
        index_columns = cursor.fetchall()
        index_details[index_name] = {
            "unique": is_unique,
            "columns": [col[2] for col in index_columns],
        }

    # Get the CREATE TABLE statement to extract any CHECK constraints
    cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    r = cursor.fetchone()
    if r:
        create_stmt = r[0]
    else:
        create_stmt = None

    conn.close()

    return {
        "columns": columns,
        "foreign_keys": foreign_keys,
        "indexes": index_details,
        "create_stmt": create_stmt,
    }


def create_temp_db_from_sql(sql_statement):
    """
    Create a temporary SQLite database from a CREATE TABLE statement
    and return its path and the table name
    """
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    # Extract the table name from the CREATE TABLE statement
    table_match = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["\'`]?([a-zA-Z0-9_]+)["\'`]?',
        sql_statement,
        re.IGNORECASE,
    )

    if not table_match:
        raise ValueError("Invalid CREATE TABLE statement")

    table_name = table_match.group(1)

    # Create a connection and execute the CREATE TABLE statement
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute(sql_statement)
    conn.commit()
    conn.close()

    return get_table_info(path, table_name)


def extract_check_constraints(create_stmt):
    """Extract CHECK constraints from a CREATE TABLE statement"""
    check_constraints = []
    # Pattern to match CHECK constraints
    pattern = r"CHECK\s*\((.*?)\)"

    for match in re.finditer(pattern, create_stmt, re.IGNORECASE | re.DOTALL):
        check_constraints.append(match.group(0))

    return check_constraints


def generate_alter_statements(table1_info, table2_info, table1_name, table2_name=None):
    """
    Generate ALTER TABLE statements to transform table1 to table2 for SQLite 3.35.0+
    """
    alter_statements = []

    # First, add a comment about SQLite version requirements
    alter_statements.append(
        "-- These ALTER TABLE statements require SQLite 3.35.0 or higher"
    )

    # Handle table rename if needed
    if table2_name and table1_name != table2_name:
        alter_statements.append(
            f'ALTER TABLE "{table1_name}" RENAME TO "{table2_name}";'
        )
        current_table_name = table2_name
    else:
        current_table_name = table1_name

    # Map column info for easier comparison
    # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
    columns1 = {
        col[1]: {
            "cid": col[0],
            "type": col[2],
            "notnull": col[3],
            "default": col[4],
            "pk": col[5],
        }
        for col in table1_info["columns"]
    }
    columns2 = {
        col[1]: {
            "cid": col[0],
            "type": col[2],
            "notnull": col[3],
            "default": col[4],
            "pk": col[5],
        }
        for col in table2_info["columns"]
    }

    # Column names in each table
    cols1 = set(columns1.keys())
    cols2 = set(columns2.keys())

    # Extract CHECK constraints to be recreated if needed
    check_constraints1 = extract_check_constraints(table1_info["create_stmt"])
    check_constraints2 = extract_check_constraints(table2_info["create_stmt"])

    # New columns to add
    for col_name in cols2 - cols1:
        col_info = columns2[col_name]

        # Build column definition
        col_def = f"{col_info['type']}"

        if col_info["notnull"]:
            col_def += " NOT NULL"

        if col_info["default"] is not None:
            col_def += f" DEFAULT {col_info['default']}"

        if col_info["pk"]:
            col_def += " PRIMARY KEY"

        alter_statements.append(
            f'ALTER TABLE "{current_table_name}" ADD COLUMN "{col_name}" {col_def};'
        )

    # Drop columns (SQLite 3.35.0+)
    for col_name in cols1 - cols2:
        alter_statements.append(
            f'ALTER TABLE "{current_table_name}" DROP COLUMN "{col_name}";'
        )

    # Rename or modify columns (SQLite 3.25.0+)
    for col_name in cols1.intersection(cols2):
        col1 = columns1[col_name]
        col2 = columns2[col_name]

        # Check if column needs modification
        if (
            col1["type"] != col2["type"]
            or col1["notnull"] != col2["notnull"]
            or col1["default"] != col2["default"]
            or col1["pk"] != col2["pk"]
        ):

            # SQLite 3.35.0+ supports some column modifications
            if col1["type"] != col2["type"]:
                alter_statements.append(
                    f"-- Cannot directly change column type in SQLite"
                )
                alter_statements.append(f"-- From: {col1['type']} To: {col2['type']}")
                alter_statements.append(f"-- Requires table recreation")

            # Update NOT NULL constraint
            if col1["notnull"] != col2["notnull"]:
                if col2["notnull"]:
                    alter_statements.append(
                        f"-- Setting column to NOT NULL requires table recreation"
                    )
                else:
                    alter_statements.append(
                        f"-- Removing NOT NULL constraint requires table recreation"
                    )

            # Update DEFAULT value
            if col1["default"] != col2["default"]:
                default_val = "NULL" if col2["default"] is None else col2["default"]
                alter_statements.append(
                    f'ALTER TABLE "{current_table_name}" ALTER COLUMN "{col_name}" SET DEFAULT {default_val};'
                )

    # Handle constraints that need to be dropped and recreated
    # Check if we need complete table recreation
    needs_recreation = False

    # Need recreation if primary key changes
    pk_cols1 = [col[1] for col in table1_info["columns"] if col[5]]
    pk_cols2 = [col[1] for col in table2_info["columns"] if col[5]]
    if pk_cols1 != pk_cols2:
        needs_recreation = True

    # Need recreation for column type changes
    for col_name in cols1.intersection(cols2):
        if columns1[col_name]["type"] != columns2[col_name]["type"]:
            needs_recreation = True
            break

    # Need recreation for NOT NULL changes
    for col_name in cols1.intersection(cols2):
        if columns1[col_name]["notnull"] != columns2[col_name]["notnull"]:
            needs_recreation = True
            break

    # Need recreation if CHECK constraints change
    if set(check_constraints1) != set(check_constraints2):
        needs_recreation = True

    # If we need to recreate the table, add those statements
    if needs_recreation:
        alter_statements.append(
            "\n-- Some changes require table recreation. Alternative approach:"
        )

        # Generate new table creation
        create_cols = []
        for col_name, col_info in columns2.items():
            col_def = f"\"{col_name}\" {col_info['type']}"

            if col_info["notnull"]:
                col_def += " NOT NULL"

            if col_info["default"] is not None:
                col_def += f" DEFAULT {col_info['default']}"

            if col_info["pk"]:
                col_def += " PRIMARY KEY"

            create_cols.append(col_def)

        # Add foreign key constraints
        fk_constraints = []
        for fk in table2_info["foreign_keys"]:
            # PRAGMA foreign_key_list returns: (id, seq, table, from, to, on_update, on_delete, match)
            fk_table = fk[2]
            from_col = fk[3]
            to_col = fk[4]
            on_update = fk[5]
            on_delete = fk[6]

            fk_def = f'FOREIGN KEY ("{from_col}") REFERENCES "{fk_table}"("{to_col}")'

            if on_update != "NO ACTION":
                fk_def += f" ON UPDATE {on_update}"

            if on_delete != "NO ACTION":
                fk_def += f" ON DELETE {on_delete}"

            fk_constraints.append(fk_def)

        # Add unique constraints
        unique_constraints = []
        for idx_name, idx_info in table2_info["indexes"].items():
            if idx_info["unique"]:
                cols = ", ".join([f'"{col}"' for col in idx_info["columns"]])
                unique_constraints.append(f"UNIQUE ({cols})")

        # Add CHECK constraints
        all_constraints = fk_constraints + unique_constraints + check_constraints2
        constraints_sql = ""
        if all_constraints:
            constraints_sql = ", " + ", ".join(all_constraints)

        create_stmt = f"CREATE TABLE \"new_{current_table_name}\" ({', '.join(create_cols)}{constraints_sql});"
        alter_statements.append(create_stmt)

        # Copy data from old table to new table
        common_columns = [f'"{col}"' for col in cols1.intersection(cols2)]
        insert_stmt = f"INSERT INTO \"new_{current_table_name}\" ({', '.join(common_columns)}) SELECT {', '.join(common_columns)} FROM \"{current_table_name}\";"
        alter_statements.append(insert_stmt)

        # Drop old table
        alter_statements.append(f'DROP TABLE "{current_table_name}";')

        # Rename new table to original name
        alter_statements.append(
            f'ALTER TABLE "new_{current_table_name}" RENAME TO "{current_table_name}";'
        )

        # Recreate any non-unique indexes
        for idx_name, idx_info in table2_info["indexes"].items():
            if not idx_info["unique"]:  # Unique indexes already handled in CREATE TABLE
                cols = ", ".join([f'"{col}"' for col in idx_info["columns"]])
                alter_statements.append(
                    f'CREATE INDEX "{idx_name}" ON "{current_table_name}" ({cols});'
                )
    else:
        # Handle index changes without full table recreation
        indexes1 = {name: details for name, details in table1_info["indexes"].items()}
        indexes2 = {name: details for name, details in table2_info["indexes"].items()}

        # Drop indexes that no longer exist
        for idx_name in indexes1:
            if idx_name not in indexes2:
                alter_statements.append(f'DROP INDEX IF EXISTS "{idx_name}";')

        # Add new indexes
        for idx_name, idx_info in indexes2.items():
            if idx_name not in indexes1:
                cols = ", ".join([f'"{col}"' for col in idx_info["columns"]])
                if idx_info["unique"]:
                    alter_statements.append(
                        f'CREATE UNIQUE INDEX "{idx_name}" ON "{current_table_name}" ({cols});'
                    )
                else:
                    alter_statements.append(
                        f'CREATE INDEX "{idx_name}" ON "{current_table_name}" ({cols});'
                    )

    comments = [a for a in alter_statements if a.startswith("--")]
    if len(comments) == len(alter_statements):
        return []
    return alter_statements


def main():
    parser = argparse.ArgumentParser(
        description="Generate ALTER TABLE statements for SQLite 3 to transform one table schema to another"
    )
    parser.add_argument("--file1", help="First SQL file path (source schema)")
    parser.add_argument("--file2", help="Second SQL file path (target schema)")
    parser.add_argument("--sql1", help="First SQL statement directly (source schema)")
    parser.add_argument("--sql2", help="Second SQL statement directly (target schema)")
    parser.add_argument(
        "--output", help="Output file for ALTER statements (default: print to console)"
    )

    args = parser.parse_args()

    # Get SQL statements from either files or direct input
    sql1 = ""
    sql2 = ""

    if args.file1:
        with open(args.file1, "r") as f:
            sql1 = f.read()
    elif args.sql1:
        sql1 = args.sql1
    else:
        print("Error: First SQL statement required (use --file1 or --sql1)")
        return

    if args.file2:
        with open(args.file2, "r") as f:
            sql2 = f.read()
    elif args.sql2:
        sql2 = args.sql2
    else:
        print("Error: Second SQL statement required (use --file2 or --sql2)")
        return

    try:
        # Create temporary databases with the provided schemas
        db1_path, table1_name = create_temp_db_from_sql(sql1)
        db2_path, table2_name = create_temp_db_from_sql(sql2)

        # Get table information using PRAGMA
        table1_info = get_table_info(db1_path, table1_name)
        table2_info = get_table_info(db2_path, table2_name)

        # Generate ALTER statements
        alter_statements = generate_alter_statements(
            table1_info, table2_info, table1_name, table2_name
        )

        # Clean up temporary databases
        os.unlink(db1_path)
        os.unlink(db2_path)

        # Output the statements
        if args.output:
            with open(args.output, "w") as f:
                for stmt in alter_statements:
                    f.write(stmt + "\n")
            print(f"ALTER statements saved to {args.output}")
        else:
            print("-- SQLite ALTER TABLE statements to transform schema:")
            for stmt in alter_statements:
                print(stmt)

    except Exception as e:
        print(f"Error: {e}")
        # Clean up temporary files in case of error
        if "db1_path" in locals():
            os.unlink(db1_path)
        if "db2_path" in locals():
            os.unlink(db2_path)


if __name__ == "__main__":
    main()
