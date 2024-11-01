<script>
  import "../app.postcss";
  import "./styles.css";

  import { applyAction, enhance } from "$app/forms";
  import { page } from "$app/stores";
  import StarsIcon from "$lib/components/custom/StarsIcon.svelte";
  import ConsentManager from "$lib/components/custom/ConsentManager.svelte";
  import InfoIcon from "$lib/components/custom/InfoIcon.svelte";
  import XIcon from "$lib/components/custom/XIcon.svelte";
  import { fly } from "svelte/transition";

  let isSubmittingPrompt = false;
  let includeCurrentRequest = true;
  let prompt = "";
  let showDescription = false;

  let placeholderText = "Ask an Apache Airflow® or Astronomer question...";
  $: {
    placeholderText = !!$page.params?.request_id
      ? "Continue this conversation with another question..."
      : "Ask an Apache Airflow® or Astronomer question...";
  }
</script>

<div class="app">
  <main>
    <section class="pb-8 pt-12">
      <div class="flex gap-2 items-center mb-4">
        <div class="flex-auto" />
        <StarsIcon />
        <h1 class="text-white flex gap-2">
          <a href="/" class="no-underline ask-astro-header">ASK ASTRO</a>
          <div class="mt-auto">
            <button
              on:click={() => (showDescription = !showDescription)}
              class="cursor-pointer align-bottom bottom-0"
            >
              {#if showDescription}
                <XIcon />
              {:else}
                <InfoIcon />
              {/if}
            </button>
          </div>
        </h1>
        <div class="flex-auto" />
      </div>

      {#if showDescription}
        <div class="text-white text-center mb-8 description" transition:fly>
          Ask Astro is an open-source, reference implementation of
          <a
            href="https://a16z.com/emerging-architectures-for-llm-applications/"
            target="_blank"
            >Andreessen Horowitz's LLM Application Architecture</a
          >. It's meant to showcase how to build an LLM application on top of
          Apache Airflow®. You can read more about the project and find the
          source code
          <a href="https://github.com/astronomer/ask-astro"> on GitHub</a>. Note
          that anything you ask may be publicly visible and should not include
          any sensitive information.
        </div>
      {/if}

      {#if $page.params.request_id}
        <div class="py-2">
          <a class="mt-2 pb-2 go-back-text" href="/">&lt; Start a new thread</a>
        </div>
      {/if}

      {#if !$page.data.publicServiceAnnouncement}
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
              placeholder={placeholderText}
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
        </form>
      {/if}
    </section>
    <slot />

    <div class="footer">
      Made with ❤️ by <a href="https://astronomer.io" target="_blank"
        >Astronomer</a
      >.
      <br />
      See the
      <a href="http://github.com/astronomer/ask-astro" target="_blank"
        >source code</a
      >.
    </div>
  </main>

  <ConsentManager />
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

  .footer {
    width: 90%;
    margin-left: auto;
    margin-right: auto;
    border-top: 1px solid #59498a;
    padding: 1rem;
    padding-top: 2rem;
    text-align: center;
    color: #d6b2f3;
  }
  .footer a {
    text-decoration: underline;
  }

  .search-input {
    box-sizing: border-box;
    padding-left: 2rem;

    border-radius: 30px;
    border: 2px solid #0f3e85;
    background: linear-gradient(
      116deg,
      rgba(95, 155, 243, 0.2) 34.6%,
      rgba(67, 118, 194, 0.2) 71.11%
    );
    backdrop-filter: blur(4px);

    height: 4rem;

    color: #5bacf6;
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
    color: #5bacf6;
    filter: opacity(0.9);
  }

  .ask-astro-header {
    background: linear-gradient(to right, #faf4ff, #cb9def);
    -webkit-background-clip: text;
    background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 800;
    font-size: 3rem;

    @media (max-width: 640px) {
      font-size: 2rem;
    }
  }

  .description {
    font-family: IntelOneMono;
    color: #d6b2f3;
    margin-left: auto;
    margin-right: auto;
    width: 80%;

    @media (max-width: 640px) {
      width: 100%;
    }
  }

  .go-back-text {
    color: #3a99f2;
    font-weight: 800;
    text-transform: uppercase;
    text-decoration: none;
  }
</style>
