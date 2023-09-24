<script lang="ts">
  import type { PageData } from "./$types";
  import RequestCard from "$lib/components/custom/RequestCard.svelte";

  export let data: PageData;
</script>

<svelte:head>
  <title>Ask Astro</title>
</svelte:head>

<p class="previously-asked pt-4">Previously asked questions</p>

<div class="grid gap-2 pt-2 pb-12 home-grid-cols">
  {#if data.requests && data.requests.length > 0}
    {#each data.requests as req}
      <RequestCard
        uuid={req.uuid}
        prompt={req.prompt}
        sources={req.sources}
        messages={req.messages}
      />
    {/each}
  {/if}
</div>

<style>
  .previously-asked {
    color: #8c80b0;
    font-weight: 300;
  }

  .home-grid-cols {
    grid-template-rows: 1fr;
    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
    width: 100%;
  }
</style>
