<script>
  import "../app.postcss";
  import "./styles.css";

  import { applyAction, enhance } from "$app/forms";
  import { page } from "$app/stores";
  import StarsIcon from "$lib/components/custom/StarsIcon.svelte";

  let isSubmittingPrompt = false;
  let includeCurrentRequest = true;
  let prompt = "";
</script>

<svelte:head>
  <link
    rel="preload"
    href="/fonts/Inter-VariableFont_slnt,wght.ttf"
    as="font"
    crossorigin="anonymous"
  />
</svelte:head>

<div class="app">
  <main>
    <section class="pb-8 pt-12">
      <div class="flex gap-2 items-center mb-4">
        <div class="flex-auto" />
        <StarsIcon />
        <h1 class="text-white">
          <a href="/" class="no-underline">Ask Astro</a>
        </h1>
        <div class="flex-auto" />
      </div>

      <div class="text-white text-center mb-8">
        Ask Astro is an open-source, reference implementation of
        <a
          href="https://a16z.com/emerging-architectures-for-llm-applications/"
          target="_blank">Andreessen Horowitz's LLM Application Architecture</a
        >. It's meant to showcase how to build an LLM application on top of
        Apache Airflow. You can read more about the project and find the source
        code
        <a href="https://github.com/astronomer/ask-astro"> on GitHub</a>. Note
        that anything you ask may be publicly visible and should not include any
        sensitive information.
      </div>

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
          <input
            placeholder="Ask an Airflow or Astronomer question..."
            name="prompt"
            class="search-input flex-auto"
            bind:value={prompt}
          />
          {#if includeCurrentRequest}
            <input
              type="hidden"
              name="from_request_uuid"
              value={$page.params.request_id}
            />
          {/if}
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
    >. See the
    <a href="http://github.com/astronomer/ask-astro" target="_blank"
      >source code</a
    >.
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

  .search-input {
    box-sizing: border-box;
    padding-left: 2rem;

    border-radius: 30px;
    border: 2px solid #604785;
    background: linear-gradient(
      116deg,
      rgba(135, 101, 161, 0.2) 34.6%,
      rgba(108, 70, 138, 0.2) 71.11%
    );
    backdrop-filter: blur(4px);

    height: 4rem;

    color: #c8b6d7;
    font-family: Inter;
    font-size: 1.5rem;
    font-style: normal;
    font-weight: 400;

    @media (max-width: 640px) {
      font-size: 1rem;
      padding-left: 1rem;
      height: 3rem;
    }
  }
  ::placeholder {
    filter: opacity(0.5);
  }
</style>
