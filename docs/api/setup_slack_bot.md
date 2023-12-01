# Create an ask-astro Slack bot

1. Go to `https://api.slack.com/apps`.
2. Click **Create New Apps**.
    ![1-create-new-app](static/1-create-new-app.png)
3. On the **Create an app** page, click **From scratch**.
4. Name the app and pick a Slack workspace
    ![3-name-app](static/3-name-app.png)
    After creating the Slack app, Slack redirects you to the home page of this app.
    ![4-app-home-page](static/4-app-home-page.png)
5. Scroll down, to find the credentials for the ask-astro API backend
    ![5-credentials](static/5-credentials.png)
    * `SLACK_CLIENT_ID`: Client ID
    * `SLACK_CLIENT_SECRET`: Client Secret
    * `SLACK_SIGNING_SECRET`: Signing Secret
6. Start the ask-astro API backend by using a reachable host, such as [ngrok](https://ngrok.com/).
7. Go to the [OAuth & Permissions](https://api.slack.com/apps/<API_ID>/oauth?) page.
8. Add `https://<ask-astro api backend host>/slack/oauth_redirect` to **Redirect URLs**.
    ![6-redirect-url](static/6-redirect-url.png)
9. Add the following scopes.
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
    ![7-scope](static/7-scope.png)
10. Go to **Event Subscriptions** page for your App. (`https://api.slack.com/apps/<API_ID>/event-subscriptions?`)
11. Set **Request URL** to `https://<ask-astro api backend host>/slack/events`.
    ![8-event-subscription](static/8-event-subscription.png)
12. Go to the **Interactivity & Shortcuts** page.
13. Set **Request URL** to `https://<ask-astro api backend host>/slack/events`.
14. Go to `https://<ask-astro api backend host>/slack/install` and click **Add to Slack**.
    ![9-slack-install](static/9-slack-install.png)

The ask-astro bot is now available to use in your Slack work space!
