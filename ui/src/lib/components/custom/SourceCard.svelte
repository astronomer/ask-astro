<script lang="ts">
  import * as Card from "$lib/components/ui/card";
  import { Skeleton } from "$lib/components/ui/skeleton";
  import SvelteMarkdown from "svelte-markdown";
  import { ChevronDown, ChevronUp, Link2 } from "radix-icons-svelte";

  export let name: string | undefined = undefined;
  export let snippet: string | undefined = undefined;
  export let loading: boolean = false;

  let expanded = false;
</script>

<Card.Root class="p-2 astro-card-root">
  <Card.Content class="p-2">
    <Card.Title class="flex items-center">
      <Link2 class="w-4 h-4 mr-1 flex-shrink-0" />

      <p class="truncate font-normal w-full">
        {#if loading || !name}
          <Skeleton class="w-full h-4" />
        {:else}
          From <a href={name} target="_blank">{name}</a>:
        {/if}
      </p>

      <div class="pr-20 flex-auto" />

      <button
        class="cursor-pointer"
        on:click={() => {
          expanded = !expanded;
        }}
      >
        {#if expanded}
          <ChevronUp class="w-4 h-4" />
        {:else}
          <ChevronDown class="w-4 h-4" />
        {/if}
      </button>
    </Card.Title>

    <Card.Description>
      {#if expanded}
        {#if loading}
          <div class="flex flex-col gap-2 pt-4">
            <Skeleton class="w-full h-4" />
            <Skeleton class="w-full h-4" />
            <Skeleton class="w-full h-4" />
          </div>
        {:else}
          <SvelteMarkdown source={snippet} />
        {/if}
      {:else if loading}
        <Skeleton class="w-full h-4 mt-4" />
      {:else}
        <div class="line-clamp-1 collapsed">
          <SvelteMarkdown source={snippet} />
        </div>
      {/if}
    </Card.Description>
  </Card.Content>
</Card.Root>

<style>
  .collapsed {
    max-height: 2rem;
    line-height: 1rem;
  }
</style>
