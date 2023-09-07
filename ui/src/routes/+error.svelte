<script lang="ts">
  import { page } from "$app/stores";
</script>

<div class="text-center text-white">
  <h1>{$page.status}</h1>
  <h3>
    {$page.error?.message}
  </h3>
</div>
