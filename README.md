# dastats

First, set up an App in Deviant Art (User -> Settings -> Apps & Uploads). Set the referral URL to localhost:8080/login.

![Authorized Application](screenshots/AuthorizedApps.png?raw=true)
![Application Setup](screenshots/ApplicationSetup.png?raw=true)

Field values:

```
Oauth2 Grant Type: Authorization Code
Redirect Whitelist: http://localhost:8080/callback
Original URI Whitelist: http://localhost:8080/login
```

Note: a lot of things try to run on port 8080, so you could try this on a different port. It doesn't really matter, they just all have to be consistent.

Second, get the secret key from (User -> Settings -> My Applications) [https://www.deviantart.com/developers/apps]

![Application Setup](screenshots/ClientSecret.png?raw=true)

Clone this repo `git clone git@github.com:jmelloy/dastats.git`

Install the requirements and run the app

```
pip install -r requirements.txt
python app.py
```

This will spit out a URL (http://127.0.0.1:8080). Click that, and it will ask you for the secret key and client ID from step 2. This will log you into deviant art and ask for permissions.

On my machine, DuckDB would not do the concurrency properly, so I had to run the stats download manually. The token file has been created in .token.json in your dastats directory.  Hit control-C to stop the app, and then run:
```
python da.py
```

Then, run the app again:

```
python app.py
```

Then it should take you to the /stats/ page [http://127.0.0.1:8080/stats/]

After running through the login process, you can also run `python da.py` to populate the database. This will take a while, but it will be faster next time.  It may get rate limited by deviant art, so if that happen just stop and wait a bit.
