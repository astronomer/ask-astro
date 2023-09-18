<script lang="ts">
  import * as Card from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import { goto } from "$app/navigation";

  export let uuid: string;
  export let prompt: string;
  export let response: string;
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
  <Card.Content class="p-2">
    <Card.Title class="truncate pb-2">
      {strippedPrompt}
    </Card.Title>
    <Card.Description>
      <p class="line-clamp-3">
        {response}
      </p>
    </Card.Description>

    <Card.Footer class="flex gap-1 pt-4">
      <div>
        <p class="text-sm">{sources.length} sources</p>
      </div>
      {#if messages.length > 0}
        <div>•</div>
        <div>
          <p class="text-sm">{messages.length} messages</p>
        </div>
      {/if}
      <div class="flex-auto" />
      <div>
        <Button href="/requests/{uuid}">See more →</Button>
      </div>
    </Card.Footer>
  </Card.Content>
</Card.Root>
