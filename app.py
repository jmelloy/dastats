from flask import Flask, request, redirect, jsonify
import requests
import time
import json
from da import get_token, DeviantArt

app = Flask(__name__)

# OAuth configuration

da = DeviantArt()


@app.route("/")
def index():
    return "Hello, World!"


@app.route("/login")
def login():
    """Redirect to the OAuth provider's authorization URL."""
    return redirect(da.authorization_url())


@app.route("/callback")
def callback():
    """Handle the OAuth callback and exchange the code for a token."""
    code = request.args.get("code")
    if not code:
        return "Error: No code received."

    data = da.update_access_token(code)
    return jsonify(data)


if __name__ == "__main__":
    print("Login using: http://localhost:8080/login")
    app.run(port=8080, debug=True)
