import { ASK_ASTRO_API_URL } from "$env/static/private";
import { redirect } from "@sveltejs/kit";
import type { PageServerLoad } from "./$types";

export const load: PageServerLoad = async () => {
  try {
    const requests = await fetch(`${ASK_ASTRO_API_URL}/requests`);

    return requests.json();
  } catch (err) {
    console.error(err);

    return { requests: [] };
  }
};

export const actions = {
  submitPrompt: async ({ request }) => {
    const formData = await request.formData();

    const prompt = formData.get("prompt")?.toString();
    const from_request_uuid = formData.get("from_request_uuid");

    if (!prompt) throw new Error("Prompt is required");

    const body: { prompt: string; from_request_uuid?: string } = {
      prompt,
    };

    if (
      from_request_uuid &&
      from_request_uuid !== "null" &&
      from_request_uuid !== "undefined"
    ) {
      body.from_request_uuid = from_request_uuid?.toString();
    }

    const response = await fetch(`${ASK_ASTRO_API_URL}/requests`, {
      method: "POST",
      body: JSON.stringify(body),
      headers: {
        "Content-Type": "application/json",
      },
    });

    const json = await response.json();
    throw redirect(302, `/requests/${json.request_uuid}`);
  },
};
