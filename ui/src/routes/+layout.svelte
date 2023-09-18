<script>
  import "../app.postcss";
  import "./styles.css";

  import { Input } from "$lib/components/ui/input";
  import { Button } from "$lib/components/ui/button";
  import ShootingStars from "$lib/components/custom/ShootingStars.svelte";
  import { applyAction, enhance } from "$app/forms";
  import { page } from "$app/stores";

  let isSubmittingPrompt = false;
  let includeCurrentRequest = true;
  let prompt = "";
</script>

<svelte:head>
  <script
    defer
    src="https://static.cloudflareinsights.com/beacon.min.js"
    data-cf-beacon={'{"token": "7a1f1bf468574e8a8f4b6e21cd94db42"}'}
  ></script>
</svelte:head>

<ShootingStars />

<div class="app">
  <main>
    <section class="pb-8 pt-4">
      <h1 class="mb-4">
        <a href="/" class="text-white">Welcome to Ask Astro</a>
      </h1>
      <form
        method="post"
        action="/?/submitPrompt"
        use:enhance={() => {
          if (isSubmittingPrompt) return;

          isSubmittingPrompt = true;
          return async ({ result }) => {
            isSubmittingPrompt = false;

            prompt = "";
            await applyAction(result);
          };
        }}
      >
        <div class="flex w-full gap-2">
          <Input
            placeholder="Ask an Airflow or Astronomer question..."
            name="prompt"
            bind:value={prompt}
          />
          {#if includeCurrentRequest}
            <input
              type="hidden"
              name="from_request_uuid"
              value={$page.params.request_id}
            />
          {/if}
          <Button type="submit" disabled={isSubmittingPrompt}>Ask</Button>
        </div>

        {#if $page.params.request_id}
          <div class="flex w-full mt-2">
            <div class="flex-auto" />
            <label class="flex items-center">
              <input
                type="checkbox"
                name="include_current_request"
                bind:checked={includeCurrentRequest}
              />
              <span class="ml-2 text-white">Continue current conversation</span>
            </label>
          </div>
        {/if}
      </form>
    </section>

    <slot />
  </main>

  <footer>
    Made with ❤️ by <a href="https://astronomer.io" target="_blank"
      >Astronomer</a
    >
  </footer>
</div>

<style>
  .app {
    display: flex;
    flex-direction: column;
    min-height: 100vh;
    overflow-x: hidden;
  }

  main {
    flex: 1;
    display: flex;
    flex-direction: column;
    padding: 1rem;
    width: 100%;
    max-width: 64rem;
    margin: 0 auto;
    box-sizing: border-box;
  }

  footer {
    padding: 1rem;
    text-align: center;
    color: #fff;
  }
  footer a {
    color: #fff;
    text-decoration: underline;
  }
</style>
