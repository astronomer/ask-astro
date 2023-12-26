import { ASK_ASTRO_API_URL } from "$env/static/private";
import { error } from "@sveltejs/kit";
import type { PageServerLoad, Actions } from "./$types";

export const load: PageServerLoad = async ({ params, depends }) => {
  depends(params.request_id);

  const request = await fetch(
    `${ASK_ASTRO_API_URL}/requests/${params.request_id}`
  );

  // if we have a 404, throw it
  if (request.status === 404) {
    throw error(404, "Request not found");
  }

  if (request.status === 429) {
    throw error(429, "Too many requests");
  }

  return await request.json();
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
