<script lang="ts">
  import { browser } from "$app/environment";
  import { invalidate } from "$app/navigation";
  import { page, navigating } from "$app/stores";
  import MessageCard from "$lib/components/custom/MessageCard.svelte";
  import SourceCard from "$lib/components/custom/SourceCard.svelte";

  const updateDataIfLoading = async () => {
    if ($page.data.status === "in_progress") {
      await invalidate($page.params.request_id);
      setTimeout(updateDataIfLoading, 3000);
    }
  };

  $: {
    // as soon as we're done navigating, start updating the data
    if (!$navigating && browser) {
      updateDataIfLoading();
    }
  }

  let loading = true;
  // TODO: stronger typing
  let sortedMessages: any[] = [];

  $: {
    loading = $page.data.status === "in_progress";
    sortedMessages =
      $page.data.messages?.sort((a: any, b: any) => {
        if (a.sent_at && b.sent_at) {
          return b.sent_at - a.sent_at;
        }

        if (a.additional_kwargs?.ts && b.additional_kwargs?.ts) {
          return b.additional_kwargs.ts - a.additional_kwargs.ts;
        }

        return 0;
      }) || [];
  }
</script>

<div class="flex gap-4 flex-col">
  <div class="text-white text-xl font-bold">Thread</div>

  {#if loading}
    <MessageCard type="ai" content={$page.data.prompt} isLoading />
    <MessageCard
      type="human"
      content={$page.data.prompt}
      additional_kwargs={{
        ts: $page.data.sent_at,
      }}
      highlight
    />
  {:else}
    <MessageCard
      type="ai"
      content={$page.data.response}
      highlight
      requestUuid={$page.params.request_id}
      additional_kwargs={{
        ts: $page.data.response_received_at,
      }}
      showFeedback
    />
    <MessageCard
      type="human"
      content={$page.data.prompt}
      additional_kwargs={{
        ts: $page.data.sent_at,
      }}
      highlight
    />
  {/if}

  {#each sortedMessages as message}
    <MessageCard {...message} />
  {/each}

  <div class="text-white text-xl font-bold">Sources</div>
  {#if loading}
    <SourceCard loading />
    <SourceCard loading />
    <SourceCard loading />
  {:else}
    {#each $page.data.sources as source}
      <SourceCard {...source} />
    {/each}
  {/if}
</div>
