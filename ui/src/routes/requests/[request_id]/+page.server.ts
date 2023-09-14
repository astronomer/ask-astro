import { ASK_ASTRO_API_URL } from "$env/static/private";
import type { PageServerLoad, Actions } from "./$types";

export const load: PageServerLoad = async ({ params, depends }) => {
  depends(params.request_id);

  try {
    const request = await fetch(
      `${ASK_ASTRO_API_URL}/requests/${params.request_id}`
    );

    return await request.json();
  } catch (err) {
    console.log(err);
    return {};
  }
};

export const actions: Actions = {
  feedback: async ({ request }) => {
    const formData = await request.formData();
    const requestUuid = formData.get("request_uuid");
    const positive = formData.get("is_correct") === "true";

    const body = JSON.stringify({ positive });
    await fetch(`${ASK_ASTRO_API_URL}/requests/${requestUuid}/feedback`, {
      method: "POST",
      body,
      headers: {
        "Content-Type": "application/json",
      },
    });
  },
};
