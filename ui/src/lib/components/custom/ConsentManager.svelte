<script>
  import inEU from "@segment/in-eu";
  import { onMount } from "svelte";

  onMount(() => {
    window.localStorage.setItem("inEU", inEU());
  });
</script>

<svelte:head>
  <script>
    const inEU = window.localStorage.getItem("inEU");
    window.consentManagerConfig = function (exports) {
      let React = exports.React;
      let bannerContent = React.createElement(
        "span",
        null,
        "We use cookies (and other similar technologies) to collect data to improve your experience on our site. By using our website, you’re agreeing to the collection of data as described in our",
        " ",
        React.createElement(
          "a",
          {
            href: "/privacy/",
            target: "_blank",
          },
          "Privacy Policy"
        ),
        "."
      );

      return {
        container: "#consent-manager",
        writeKey: "YtGAJiHNKB84NxpN0PxmgP0LmHeoMKGl",
        bannerActionsBlock: inEU,
        bannerContent: bannerContent,
        bannerSubContent: "You can change your preferences.",
        preferencesDialogTitle: "Website Data Collection Preferences",
        preferencesDialogContent:
          "We use data collected by cookies and JavaScript libraries to improve your browsing experience, analyze site traffic, deliver personalized advertisements, and increase the overall performance of our site.",
        cancelDialogTitle: "Are you sure you want to cancel?",
        cancelDialogContent:
          "Your preferences have not been saved. By continuing to use our website, you՚re agreeing to our Website Data Collection Policy.",
        closeBehavior: inEU ? "dismiss" : "accept",
        bannerHideCloseButton: inEU,
        defaultDestinationBehavior: "imply",
        implyConsentOnInteraction: !inEU,
      };
    };
  </script>
  <script>
    !(function () {
      var analytics = window && (window.analytics = window.analytics || []);
      if (!analytics.initialize)
        if (analytics.invoked)
          window.console &&
            console.error &&
            console.error("Segment snippet included twice.");
        else {
          analytics.invoked = !0;
          analytics.methods = [
            "trackSubmit",
            "trackClick",
            "trackLink",
            "trackForm",
            "pageview",
            "identify",
            "reset",
            "group",
            "track",
            "ready",
            "alias",
            "debug",
            "page",
            "once",
            "off",
            "on",
            "addSourceMiddleware",
          ];
          analytics.factory = function (t) {
            return function () {
              var e = Array.prototype.slice.call(arguments);
              e.unshift(t);
              analytics.push(e);
              return analytics;
            };
          };
          for (var t = 0; t < analytics.methods.length; t++) {
            var e = analytics.methods[t];
            analytics[e] = analytics.factory(e);
          }
          analytics.load = function (t, e) {
            var n = document.createElement("script");
            n.type = "text/javascript";
            n.async = !0;
            n.src =
              ("https:" === document.location.protocol
                ? "https://"
                : "http://") +
              "cdn.segment.com/analytics.js/v1/" +
              t +
              "/analytics.min.js";
            var o = document.getElementsByTagName("script")[0];
            o.parentNode.insertBefore(n, o);
            analytics._loadOptions = e;
          };
          analytics.SNIPPET_VERSION = "4.1.0";
          analytics.page();
        }
    })();
  </script>
  <script
    src="https://www.astronomer.io/scripts/consent-manager.js"
    defer
  ></script>
</svelte:head>

<div id="consent-manager" class="absolute w-full bottom-0" />
