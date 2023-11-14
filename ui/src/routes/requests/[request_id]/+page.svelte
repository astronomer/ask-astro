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
  let title = "Ask Astro";

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

  $: {
    if ($page.data.prompt) {
      title = `Ask Astro - ${$page.data.prompt.slice(0, 40)}${
        $page.data.prompt.length > 40 ? "..." : ""
      }`;
    }
  }
</script>

<svelte:head>
  <title>
    {title}
  </title>
</svelte:head>

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
    />
  {:else}
    <MessageCard
      type="ai"
      content={$page.data.response}
      requestUuid={$page.params.request_id}
      additional_kwargs={{
        ts: $page.data.response_received_at,
        status: $page.data.status,
      }}
      showFeedback
    />
    <MessageCard
      type="human"
      content={$page.data.prompt}
      additional_kwargs={{
        ts: $page.data.sent_at,
      }}
    />
  {/if}

  {#each sortedMessages as message}
    <MessageCard {...message} />
  {/each}

  <!--
  <div class="sources mt-8">
    <h2 class="sources-heading">Sources</h2>
    {#if loading}
      <SourceCard loading />
      <SourceCard loading />
      <SourceCard loading />
    {:else}
      {#each $page.data.sources as source}
        <div class="mb-4">
          <SourceCard {...source} />
        </div>
      {/each}
    {/if}
  </div>
  -->
</div>

<style>
  .sources {
    border-top: 1px solid #59498a;
    width: 90%;
    margin-left: auto;
    margin-right: auto;
  }

  .sources-heading {
    color: #8c80b0;
    padding-top: 1rem;
    padding-bottom: 1rem;
    font-size: 22px;
  }
</style>
