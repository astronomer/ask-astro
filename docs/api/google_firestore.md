# Setting Up Firestore for the Ask Astro Application on Google Cloud Run

Firestore is a scalable, serverless NoSQL database offered by Google Cloud. This guide explains how to set up Firestore for the Ask Astro application running on Google Cloud Run within a Google Cloud Project. We will also discuss how Firestore connects to Cloud Build and Cloud Run using environment variables.

## What is Firestore?

Firestore is a NoSQL, document-oriented database that is designed to store and manage data in a flexible, scalable, and serverless manner. It is well-suited for real-time applications and is a core component of the Firebase platform. Firestore offers features such as real-time data synchronization, offline support, and robust security rules.

## Prerequisites

Before you begin, ensure you have the following:

- A Google Cloud Platform (GCP) account.
- A project created on GCP for the Ask Astro application, where your Cloud Run service will run.

## Steps

### 1. Enable Firestore API

Firestore is provided through the Firestore API. To enable it:

1. Navigate to the [Google Cloud Console](https://console.cloud.google.com/).
2. Select your Ask Astro project.
3. In the left sidebar, click on "APIs & Services" > "Library."
4. Search for "Firestore" and select the "Cloud Firestore API."
5. Click the "Enable" button.

### 2. Create a Firestore Database

1. In the Google Cloud Console, go to "Firestore" from the sidebar.
2. Click on "Create Database."
3. Choose the "Production mode" for a more secure setup (recommended for production apps).
4. Select a location for your Firestore database. Choose the one closest to your Ask Astro users for reduced latency.
5. Set up your security rules based on the Ask Astro app's requirements.

### 3. Configure Firebase SDK

Firestore is often used with the Firebase SDK for easier client-side integration. To configure it:

1. Go to the [Firebase Console](https://console.firebase.google.com/).
2. Create a new Firebase project for Ask Astro.
3. In your Firebase project, go to "Project settings" > "General."
4. Scroll down to the "Your apps" section and click on the `</>` icon to add a web app.
5. Follow the setup instructions, and Firebase will provide you with a configuration object containing your Firebase configuration (apiKey, authDomain, projectId, etc.).
6. Include this configuration in your Ask Astro Cloud Run app to authenticate with Firestore.

### 4. Deploy Ask Astro on Google Cloud Run

1. Build the Ask Astro application's Docker image and push it to a container registry (e.g., Google Container Registry or Docker Hub).
2. Deploy Ask Astro on Google Cloud Run by following the [official documentation](https://cloud.google.com/run/docs/deploying).

### 5. Connect Firestore with Cloud Build and Cloud Run

To connect Firestore with Ask Astro, Cloud Build, and Cloud Run, you can use environment variables. The following environment variables can be set in your Ask Astro Cloud Run service configuration to specify Firestore collections:

- `FIRESTORE_INSTALLATION_STORE_COLLECTION=ask-astro-dev-install-store`
- `FIRESTORE_STATE_STORE_COLLECTION=ask-astro-dev-state-store`
- `FIRESTORE_MESSAGES_COLLECTION=ask-astro-dev-messages`
- `FIRESTORE_MENTIONS_COLLECTION=ask-astro-dev-mentions`
- `FIRESTORE_ACTIONS_COLLECTION=ask-astro-dev-actions`
- `FIRESTORE_RESPONSES_COLLECTION=ask-astro-dev-responses`
- `FIRESTORE_REACTIONS_COLLECTION=ask-astro-dev-reactions`
- `FIRESTORE_SHORTCUTS_COLLECTION=ask-astro-dev-shortcuts`
- `FIRESTORE_TEAMS_COLLECTION=ask-astro-dev-teams`
- `FIRESTORE_REQUESTS_COLLECTION=ask-astro-dev-requests`

These environment variables allow the Ask Astro Cloud Run application to interact with specific Firestore collections. You can access these variables in your Ask Astro application code to read from and write to Firestore.

The above steps successfully sets up Firestore for the Ask Astro application running on Google Cloud Run. Firestore provides a scalable and reliable NoSQL database for the serverless applications, and with the Firebase SDK and environment variables, you can easily integrate it into Ask Astro and connect it to Cloud Build and Cloud Run.
