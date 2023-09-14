<script>
  import ShootingStar from "./ShootingStar.svelte";
  import { onMount } from "svelte";

  const duration = 5;

  // create a shooting star every `duration` seconds
  onMount(() => {
    const interval = setInterval(() => {
      const shootingStar = new ShootingStar({
        target: document.body,
        props: {
          animationDuration: duration,
        },
      });

      setTimeout(() => {
        shootingStar.$destroy();
      }, duration * 1000);
    }, duration * 1000);

    return () => {
      clearInterval(interval);
    };
  });
</script>
