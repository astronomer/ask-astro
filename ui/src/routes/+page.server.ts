import {
  ASK_ASTRO_API_URL,
  RATE_LIMITER_COOKIE_SECRET,
  RATE_LIMIT_FOR_IP,
  RATE_LIMIT_FOR_IPUA,
  RATE_LIMIT_FOR_COOKIE
} from "$env/static/private";
import { error, redirect } from "@sveltejs/kit";
import { RateLimiter } from 'sveltekit-rate-limiter/server';
import type { PageServerLoad } from "./$types";
import type { RequestEvent } from '@sveltejs/kit';

const limiter = new RateLimiter({
  IP: [parseInt(RATE_LIMIT_FOR_IP), 'd'],
  IPUA: [parseInt(RATE_LIMIT_FOR_IPUA), 'd'],
  cookie: {
    name: 'ask_astro_limiter_id',
    secret: RATE_LIMITER_COOKIE_SECRET,
    rate: [parseInt(RATE_LIMIT_FOR_COOKIE), 'm'],
    preflight: true // Require preflight call (see load function)
  }
});

const publicServiceAnnouncement = "Public Service Announcement: Ask Astro is currently undergoing maintenance and will be back shortly. We apologize for any inconvenience this may cause!";

export const load: PageServerLoad = async (event) => {
  await limiter.cookieLimiter?.preflight(event);

  let health_status;
  try {
    health_status = await fetch(`${ASK_ASTRO_API_URL}/health_status`);
    health_status = await health_status.json();
  } catch (err) {
  }

  try {
    const requests = await fetch(`${ASK_ASTRO_API_URL}/requests`);
    return {
      requests: await requests.json(),
      publicServiceAnnouncement: (health_status?.status === "maintenance" || !health_status) ? publicServiceAnnouncement : null,
    };
  } catch (err) {
    console.error(err);
    return {
      requests: [],
      publicServiceAnnouncement: (health_status?.status === "maintenance" || !health_status) ? publicServiceAnnouncement : null,
    };
  }
};


export const actions = {
  submitPrompt: async (event: RequestEvent) => {

    if (await limiter.isLimited(event))
      throw error(429, "You have made too many requests and exceeded the rate limit. Please wait for some time before trying again.");

    const formData = await event.request.formData();

    const prompt = formData.get("prompt")?.toString() ?? "";
    const from_request_uuid = formData.get("from_request_uuid");

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
