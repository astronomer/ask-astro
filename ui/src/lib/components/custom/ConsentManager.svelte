<script>
  import inEU from "@segment/in-eu";
  import { onMount } from "svelte";

  onMount(() => {
    window.localStorage.setItem("inEU", inEU());
  });
</script>

<div id="consent-manager" class="absolute w-full bottom-0" />
