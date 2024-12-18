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

Second, get the secret key from (User -> Settings -> My Applications) [https://www.deviantart.com/developers/apps]

![Application Setup](screenshots/ClientSecret.png?raw=true)

Clone this repo `git clone git@github.com:jmelloy/dastats.git`

Install the requirements and run the app

```
pip install -r requirements.txt
python app.py
```

This will spit out a URL (http://127.0.0.1:8080). Click that, and it will ask you for the secret key and client ID from step 2. This will log you into deviant art and ask for permissions.

Stats will start downloading. Depending on how many deviations you have, this could take a few minutes.

Then it should take you to the /stats/ page [http://127.0.0.1:8080/stats/]

```

```
