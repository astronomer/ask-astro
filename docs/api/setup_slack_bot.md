# Create an ask-astro Slack bot

1. Go to <https://api.slack.com/apps>
2. Click "Create New Apps" ![1-create-new-app](static/1-create-new-app.png)
3. Click "From scratch" in the "Create an app" page
4. Name the app and pick a Slack workspace ![3-name-app](static/3-name-app.png)
5. After creating the Slack app, we'll be redirected to the home page of this app ![4-app-home-page](static/4-app-home-page.png)
6. Scroll down, and we'll find the credentials for the ask-astro API backend ![5-credentials](static/5-credentials.png)
    * `SLACK_CLIENT_ID`: Client ID
    * `SLACK_CLIENT_SECRET`: Client Secret
    * `SLACK_SIGNING_SECRET`: Singing Secret
7. Start the ask-astro API backend with a reachable host (e.g., with ngrok)
8. Go to "OAuth & Permissions" page (`https://api.slack.com/apps/<API_ID>/oauth?)`
9. Add  `https://<ask-astro api backend host>/slack/oauth_redirect` to "Redirect URLs" ![6-redirect-url](static/6-redirect-url.png)

10. Scroll down and add the following scopes ![7-scope](static/7-scope.png)
    * commands
    * app_mentions:read
    * channels:read
    * channels:history
    * groups:read
    * groups:history
    * chat:write
    * reactions:read
    * reactions:write
    * users:read
    * users:read.email
    * team:read
    * im:history
    * mpim:history
    * files:read
11. Go to "Event Subscriptions" page (`https://api.slack.com/apps/<API_ID>/event-subscriptions?`)
12. Set "Request URL" as `https://<ask-astro api backend host>/slack/events` ![8-event-subscription](static/8-event-subscription.png)
13. 12. Go to "Interactivity & Shortcuts" page.
14. Set "Request URL" as `https://<ask-astro api backend host>/slack/events`.
15. Go to `https://<ask-astro api backend host>/slack/install` and you'll see ![9-slack-install](static/9-slack-install.png)
16. The ask-astro bot should be ready to go in you Slack work space
