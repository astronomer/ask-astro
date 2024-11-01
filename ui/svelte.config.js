import adapter from "@sveltejs/adapter-cloudflare";
import { vitePreprocess } from "@sveltejs/kit/vite";

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: [vitePreprocess({})],
  kit: {
    adapter: adapter(),
    csp: {
      directives: {
        'script-src': ['self', 'https://www.astronomer.io'],
      },
    }
  },
};

export default config;
