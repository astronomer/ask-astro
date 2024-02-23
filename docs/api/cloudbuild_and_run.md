# Backend API CI/CD

Currently, Ask Astro uses [Google cloud run](https://cloud.google.com/run/docs/overview/what-is-cloud-run) to run the API server,
and deploys the backend API with [cloud build](https://cloud.google.com/build/docs/overview). You can find the cloud
build configuration in [`/api/cloudbuild.yaml`](../../api/cloudbuild.yaml).

## Create cloud run service

These steps create a cloud service and template for your cloud build on the Google Cloud Platform.

1. Go to `https://console.cloud.google.com/`, and search for the **Cloud Run** service.
2. On the **Cloud Run** page, click on **CREATE SERVICE**.

![create cloud run service](../images/cloud_run_home.png)

3. Select **Continuously deploy new revision from source repository** and **Set up with cloud build**
4. Select Repository provider as **GitHub** and click **Next**.

![create cloud build config](../images/cloud_run_ci_deploy.png)

5. Set up the build configuration:
   - Branch name: `main`
   - Build type: Python via buildpacks
   - Build context directory: `/api`

![create cloud build config](../images/cloud_run_build_config.png)

6. Add **Service name**, **Region**, **CPU allocation and pricing**, **Autoscaling**, and **Ingress**.

![create cloud build config](../images/cloud_run_config1.png)

7. Allow **Unauthenticated invocation** and click on **Create**.

![create cloud build config](../images/cloud_run_create.png)

8. Add the required Environment variables to run the backend API.
   1. After you create the service, visit the **Created service** page
   2. Click on **EDIT AND DEPLOY NEW REVISION**
   3. Add you Environment variable and click **Deploy**

![cloud run edit](../images/cloud_run_edit.png)

![create cloud build config](../images/cloud_run_add_env.png)

## Update cloud build configuration

1. Go to `https://console.cloud.google.com/` and search for the **cloud build** service.

![cloud build trigger](../images/cloud_build_trigger.png)

2. Click on **Triggers**. When you created a **Cloud Run Service**, it also made a **Cloud Trigger Template** that you can edit.
4. Click on the cloud build trigger template and update the following information:
   1. **Name**, **Description**, **Tag**, and **Event**
   ![cloud build config1](../images/cloud_build_config1.png)
   2. Select your **Source** and **Branch**.
   ![cloud build config2](../images/cloud_build_config2.png)
   3. In Configuration, select **Cloud Build configuration file** and define the Cloud Build configuration file location as `api/cloudbuild.yaml`.
   4. Don't make any changes to the advance settings and click **Save**.
   ![cloud build config3](../images/cloud_build_config3.png)

5. In settings, enable **Cloud build** and **Cloud run**.

![cloud build setting](../images/cloud_build_setting.png)
