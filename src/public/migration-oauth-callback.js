(function () {
  var payloadNode = document.getElementById("migration-oauth-payload");
  if (!payloadNode) {
    return;
  }

  var payload = {
    type: "migration-salesforce-oauth",
    ok: payloadNode.dataset.ok === "true",
    migrationId: payloadNode.dataset.migrationId || "",
    message: payloadNode.dataset.message || ""
  };

  try {
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage(payload, window.location.origin);
    } else {
      setTimeout(function () {
        window.location.href = "/";
      }, 1200);
    }
  } catch (error) {
    console.error(error);
  }

  setTimeout(function () {
    try {
      window.close();
    } catch (error) {
      console.error(error);
    }
  }, 250);
})();
