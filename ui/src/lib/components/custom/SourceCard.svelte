<script lang="ts">
  import { Skeleton } from "$lib/components/ui/skeleton";
  import SvelteMarkdown from "svelte-markdown";
  import { ChevronDown, ChevronUp } from "radix-icons-svelte";

  export let name: string | undefined = undefined;
  export let snippet: string | undefined = undefined;
  export let loading: boolean = false;

  // TODO: move to backend

  const codesamplesPrefix = "https://github.com/astronomer/docs/blob/main/code-samples/dags";

  const translatedUrl = name.startsWith(codesamplesPrefix)
    ? name
    : name
      ?.replace(
        "https://github.com/astronomer/docs/blob/main",
        "https://docs.astronomer.io"
      )
      ?.replace(
        "github.com/apache/airflow/blob/main/docs/apache-airflow",
        "airflow.apache.org/docs/apache-airflow/stable"
      )
      ?.replace(".md", ".html")
      ?.replace(".rst", ".html");

  let expanded = false;
</script>

<div class="w-full flex gap-2 pb-1">
  <p class="truncate font-normal w-full source-link">
    {#if loading || !translatedUrl}
      <Skeleton class="w-full h-4" />
    {:else}
      <a href={translatedUrl} target="_blank">{translatedUrl}</a>
    {/if}
  </p>

  <div class="flex-auto" />

  <button
    class="cursor-pointer"
    on:click={() => {
      expanded = !expanded;
    }}
  >
    {#if expanded}
      <ChevronUp class="w-4 h-4" color="#edd7ff" />
    {:else}
      <ChevronDown class="w-4 h-4" color="#edd7ff" />
    {/if}
  </button>
</div>

<div class="source-content">
  {#if expanded}
    {#if loading}
      <div class="flex flex-col gap-2 pt-4">
        <Skeleton class="w-full h-4" />
        <Skeleton class="w-full h-4" />
        <Skeleton class="w-full h-4" />
      </div>
    {:else}
      <div class="rendered-md">
        <SvelteMarkdown source={snippet} />
      </div>
    {/if}
  {:else if loading}
    <Skeleton class="w-full h-4 mt-4" />
  {:else}
    <div class="line-clamp-1 collapsed rendered-md">
      <SvelteMarkdown source={snippet} />
    </div>
  {/if}
</div>

<style>
  .collapsed {
    max-height: 2rem;
    line-height: 1rem;
  }

  .source-link {
    color: #edd7ff;
    font-size: 17px;
  }

  .source-content {
    color: #8c80b0;
    font-size: 15px;
  }
</style>
