from flask import Flask, request, redirect, jsonify, render_template
from da import DeviantArt

import os
from datetime import datetime
from sql import top_by_activity, get_deviation_activity

# Initialize Flask app
app = Flask(__name__)
da = DeviantArt()


@app.route("/")
def index():
    if not os.path.exists(".credentials.json"):
        return render_template("credentials.html")
    else:
        return redirect("/stats/")


# OAuth configuration
@app.route("/login", methods=["POST"])
def login():
    """Redirect to the OAuth provider's authorization URL."""

    client_id = request.form.get("client_id")
    client_secret = request.form.get("client_secret")

    if not client_id or not client_secret:
        return "Error: Missing client_id or client_secret."
    da.set_credentials(client_id, client_secret)

    return redirect(da.authorization_url())


@app.route("/callback")
def callback():
    """Handle the OAuth callback and exchange the code for a token."""
    code = request.args.get("code")
    if not code:
        return "Error: No code received."

    data = da.update_access_token(code)
    return jsonify(data)


@app.route("/stats/")
def stats():
    return render_template("dashboard.html")


@app.route("/update-table", methods=["POST"])
def update_table():
    date_str = request.form.get("date")
    limit = request.form.get("limit", 10)
    if date_str:
        date_object = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        table_data = top_by_activity(date_object, limit, da.access_token)

        return render_template("partials/table.html", table_data=table_data)
    return "Invalid date", 400


@app.route("/get-sparkline-data", methods=["POST"])
def get_sparkline_data():
    event_id = request.json.get("id")
    date_str = request.json.get("date")
    if event_id:
        # Simulate event-specific data points
        sparkline_data = get_deviation_activity(event_id, date_str)
        return jsonify({"status": "success", "data": sparkline_data})
    return jsonify({"status": "error", "message": "Invalid event ID"}), 400


if __name__ == "__main__":
    app.run(port=8080, debug=True)
