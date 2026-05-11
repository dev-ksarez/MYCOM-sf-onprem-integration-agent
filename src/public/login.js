(function () {
  var loginForm = document.getElementById("login-form");
  if (!loginForm) {
    return;
  }

  loginForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    var username = String(document.getElementById("login-username").value || "").trim();
    var password = String(document.getElementById("login-password").value || "");
    var errorBox = document.getElementById("login-error");
    errorBox.classList.add("d-none");

    try {
      var csrfToken = String(document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute("content") || "");
      var response = await fetch("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
        body: JSON.stringify({ username: username, password: password })
      });
      var payload = await response.json().catch(function () {
        return { error: "Anmeldung fehlgeschlagen" };
      });
      if (!response.ok) {
        throw new Error(payload.error || "Anmeldung fehlgeschlagen");
      }
      window.location.href = "/";
    } catch (error) {
      errorBox.textContent = error.message || "Anmeldung fehlgeschlagen";
      errorBox.classList.remove("d-none");
    }
  });
})();
