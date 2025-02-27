from flask import Flask, jsonify
import duckdb
import os

app = Flask(__name__)

# Initialize DuckDB connection


@app.route("/")
def home():
    conn = duckdb.connect("test.db", read_only=True)
    rs = conn.execute("select * from users")
    return jsonify(rs.fetchall())


if __name__ == "__main__":
    print("Connected to test.db", os.getpid())
    conn = duckdb.connect("test.db")

    # Create a sample table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR
        )
    """
    )

    # Insert sample data
    conn.execute(
        """
        INSERT INTO users (id, name, email) VALUES 
        (1, 'John Doe', 'john@example.com'),
        (2, 'Jane Smith', 'jane@example.com') on conflict do nothing
    """
    )

    app.run(debug=True)
