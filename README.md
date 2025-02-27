# dastats

First, set up an App in Deviant Art (User -> Settings -> Apps & Uploads). Set the referral URL to localhost:8080/login.

![Authorized Application](screenshots/AuthorizedApps.png?raw=true)
![Application Setup](screenshots/ApplicationSetup.png?raw=true)
![Client Secret](screenshots/ApplicationSetup2.png?raw=true)

Field values:

```
Oauth2 Grant Type: Authorization Code
Redirect Whitelist: http://localhost:4444/callback http://localhost:8080/callback
Original URI Whitelist: http://localhost:4444/login http://localhost:8080/login
```

Note: since a lot of things try to run on port 8080, I randomly chose 4444. It doesn't really matter, they just all have to be consistent.

Second, get the secret key from (User -> Settings -> My Applications) [https://www.deviantart.com/developers/apps]

![Client Secret](screenshots/ClientSecret.png?raw=true)

Clone this repo `git clone git@github.com:jmelloy/dastats.git`

Install the requirements and run the app

```
pip install -r requirements.txt
python app.py -p 4444
```

This will spit out a URL (http://localhost:4444). Click that, and it will ask you for the secret key and client ID from step 2. This will log you into deviant art and ask for permissions.

This should start automatically populating the database in the background. The stats page will be blank until the database is populated.

Then it should take you to the /stats/ page [http://localhost:4444/stats/]

After running through the login process, you can also run `python da.py` to populate the database.   It may get rate limited by deviant art, so if that happen just stop and wait a bit. It should only download what's missing on the next run.
