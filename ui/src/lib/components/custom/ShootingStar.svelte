<script lang="ts">
  export let animationDuration: number;

  // pick a random enter and exit point
  let enterY = Math.random() * 100;
  let exitY = Math.random() * 100;

  // enterX and exitX are either 100,0 or 0,100 - decide with 50/50 chance
  let enterX = Math.random() > 0.5 ? 98 : 2;
  let exitX = enterX === 98 ? 2 : 98;
</script>

<div
  class="shooting-star"
  style="
      --enter-x: {enterX}vw;
      --enter-y: {enterY}vh;
      --exit-x: {exitX}vw;
      --exit-y: {exitY}vh;
      --animation-duration: {animationDuration}s;
    "
/>

<style>
  .shooting-star {
    position: absolute;
    width: 4px;
    height: 4px;
    background: #4c5ad4;
    border-radius: 50%;
    animation: shooting-star var(--animation-duration)
      cubic-bezier(0.4, 0, 1, 0.25);
    z-index: -1;
    top: 0;
    left: 0;
    opacity: 0;
  }

  @keyframes shooting-star {
    0% {
      opacity: 0.6;
      top: var(--enter-y);
      left: var(--enter-x);
    }
    40% {
      opacity: 1;
      top: var(--exit-y);
      left: var(--exit-x);
    }
    41% {
      opacity: 0;
      top: var(--exit-y);
      left: var(--exit-x);
    }
    100% {
      opacity: 0;
      top: var(--exit-y);
      left: var(--exit-x);
    }
  }
</style>
