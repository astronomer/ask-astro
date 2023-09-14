<script lang="ts">
  import type { PageData } from "./$types";
  import RequestCard from "$lib/components/custom/RequestCard.svelte";

  export let data: PageData;
</script>

<svelte:head>
  <title>Ask Astro</title>
</svelte:head>

<section>
  <div class="grid gap-2 pt-6 home-grid-cols">
    {#each data.requests as req}
      <RequestCard {...req} />
    {/each}
  </div>
</section>

<style>
  section {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    height: fit-content;
    margin: auto;
  }

  .home-grid-cols {
    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
    width: 100%;
  }
</style>
