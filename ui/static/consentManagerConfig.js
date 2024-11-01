const inEU = true;
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
      "Privacy Policy",
    ),
    ".",
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
