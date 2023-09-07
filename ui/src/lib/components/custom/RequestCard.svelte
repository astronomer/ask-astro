<script lang="ts">
  import * as Card from "$lib/components/ui/card";
  import { goto } from "$app/navigation";

  export let uuid: string;
  export let prompt: string;
  export let sources: unknown[];
  export let messages: unknown[];

  let strippedPrompt = prompt;

  if (strippedPrompt.match(/^<@U[A-Z0-9]{10}> /)) {
    strippedPrompt = strippedPrompt.replace(/^<@U[A-Z0-9]{10}> /, "");
  }

  // make sure the first letter is capitalized
  strippedPrompt =
    strippedPrompt.charAt(0).toUpperCase() + strippedPrompt.slice(1);
</script>

<Card.Root
  class="p-2 card-root astro-card-root home-card"
  on:click={() => goto(`/requests/${uuid}`)}
>
  <Card.Content class="p-2 h-full flex flex-col">
    <Card.Title class="pb-2 line-clamp-2 text-xl font-normal">
      {strippedPrompt}
    </Card.Title>

    <div class="flex-auto" />

    <Card.Footer class="flex gap-1 pt-4 text-sm font-light">
      <div>
        <p>{sources.length} sources</p>
      </div>
      {#if messages.length > 0}
        <div>•</div>
        <div>
          <p>{messages.length} messages</p>
        </div>
      {/if}
      <div class="flex-auto" />
      <div>
        <a href="/requests/{uuid}" class="no-underline">See more →</a>
      </div>
    </Card.Footer>
  </Card.Content>
</Card.Root>
