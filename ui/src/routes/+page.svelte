<script lang="ts">
  import type { PageData } from "./$types";
  import RequestCard from "$lib/components/custom/RequestCard.svelte";

  export let data: PageData;
</script>

<svelte:head>
  <title>Ask Astro</title>
</svelte:head>

<div class="grid gap-2 pt-6 home-grid-cols">
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
  .home-grid-cols {
    grid-template-rows: 1fr;
    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
    width: 100%;
  }
</style>
