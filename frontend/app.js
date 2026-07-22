const apiBase = new URLSearchParams(window.location.search).get("api") || "";
const fallbackProviders = ["chatgpt-codex", "mock-llm", "openrouter", "openai-codex"];

const state = {
  sessions: [],
  providers: [],
  selectedSessionId: null,
  modelSelection: null,
};

const els = {
  sessionsList: document.querySelector("#sessionsList"),
  refreshSessions: document.querySelector("#refreshSessions"),
  createSessionForm: document.querySelector("#createSessionForm"),
  sessionTitle: document.querySelector("#sessionTitle"),
  sessionTags: document.querySelector("#sessionTags"),
  chatTitle: document.querySelector("#chatTitle"),
  chatMeta: document.querySelector("#chatMeta"),
  modelForm: document.querySelector("#modelForm"),
  providerSelect: document.querySelector("#providerSelect"),
  modelName: document.querySelector("#modelName"),
  saveModel: document.querySelector("#saveModel"),
  modelStatus: document.querySelector("#modelStatus"),
  authStatus: document.querySelector("#authStatus"),
  loginChatGPT: document.querySelector("#loginChatGPT"),
  deviceLogin: document.querySelector("#deviceLogin"),
  messages: document.querySelector("#messages"),
  messageForm: document.querySelector("#messageForm"),
  messageContent: document.querySelector("#messageContent"),
  sendMessage: document.querySelector("#sendMessage"),
};

els.refreshSessions.addEventListener("click", () => loadSessions());
els.createSessionForm.addEventListener("submit", createSession);
els.modelForm.addEventListener("submit", saveModelSelection);
els.loginChatGPT.addEventListener("click", startChatGPTLogin);
els.messageForm.addEventListener("submit", sendMessage);

init().catch((error) => {
  setSessionsStatus(error.message);
  setModelStatus(error.message);
});

async function init() {
  renderProviderOptions();
  setModelStatus("Loading providers...");
  await loadProviders();
  await loadChatGPTTokens();
  await loadSessions();
}

async function loadProviders() {
  try {
    const payload = await request("/providers");
    state.providers = payload.providers || [];
    setModelStatus("Providers loaded. Select a session to use a session model.");
  } catch (error) {
    state.providers = [];
    setModelStatus(`Using fallback providers. ${error.message}`);
  }
  renderProviderOptions();
}

async function loadChatGPTTokens() {
  try {
    const tokens = await request("/auth/openai-codex/tokens");
    const latest = tokens[0];
    if (!latest) {
      setAuthStatus("Not logged in");
      return;
    }
    const expiry = latest.expires_at ? new Date(latest.expires_at).toLocaleString() : "no expiry";
    setAuthStatus(`Logged in as ${latest.subject}; expires ${expiry}`);
  } catch (error) {
    setAuthStatus(`Login status unavailable. ${error.message}`);
  }
}

async function startChatGPTLogin() {
  els.loginChatGPT.disabled = true;
  els.deviceLogin.hidden = true;
  els.deviceLogin.innerHTML = "";
  setAuthStatus("Starting login...");
  try {
    const device = await request("/auth/openai-codex/device/start", { method: "POST" });
    renderDeviceLogin(device);
    await pollChatGPTLogin(device);
  } catch (error) {
    setAuthStatus(error.message);
  } finally {
    els.loginChatGPT.disabled = false;
  }
}

async function pollChatGPTLogin(device) {
  let current = device;
  while (true) {
    await sleep((current.interval_seconds || 5) * 1000);
    const payload = await request(`/auth/openai-codex/device/${encodeURIComponent(device.id)}/poll`, {
      method: "POST",
    });
    if (payload.status === "complete") {
      els.deviceLogin.hidden = true;
      setAuthStatus(`Logged in as ${payload.token.subject}`);
      ensureProviderOption("chatgpt-codex");
      await loadProviders();
      return;
    }
    current = payload;
    renderDeviceLogin({ ...device, ...payload });
  }
}

async function loadSessions() {
  setSessionsStatus("Loading sessions...");
  try {
    state.sessions = await request("/sessions");
    renderSessions();
    if (!state.selectedSessionId && state.sessions.length > 0) {
      await selectSession(state.sessions[0].id);
    } else if (state.selectedSessionId) {
      await loadMessages();
    } else {
      renderEmptyChat();
    }
  } catch (error) {
    setSessionsStatus(error.message);
  }
}

async function createSession(event) {
  event.preventDefault();
  const title = els.sessionTitle.value.trim() || null;
  const tags = els.sessionTags.value
    .split(",")
    .map((tag) => tag.trim())
    .filter(Boolean);

  const session = await request("/sessions", {
    method: "POST",
    body: JSON.stringify({ title, tags }),
  });

  els.sessionTitle.value = "";
  els.sessionTags.value = "";
  state.sessions = [session, ...state.sessions.filter((item) => item.id !== session.id)];
  renderSessions();
  await selectSession(session.id);
}

async function selectSession(sessionId) {
  state.selectedSessionId = sessionId;
  renderSessions();
  const session = state.sessions.find((item) => item.id === sessionId);
  els.chatTitle.textContent = session?.title || sessionId;
  els.chatMeta.textContent = "Loading model...";
  els.messageContent.disabled = false;
  els.sendMessage.disabled = false;
  await loadModelSelection();
  await loadMessages();
}

async function loadModelSelection() {
  if (!state.selectedSessionId) {
    state.modelSelection = null;
    return;
  }
  state.modelSelection = await request(`/sessions/${encodeURIComponent(state.selectedSessionId)}/model-selection`);
  renderModelSelection();
}

async function saveModelSelection(event) {
  event.preventDefault();
  const provider = els.providerSelect.value.trim();
  const model = els.modelName.value.trim();
  if (!provider || !model) {
    setModelStatus("Provider and model are required.");
    return;
  }
  els.saveModel.disabled = true;
  try {
    const body = { provider, model };
    if (state.selectedSessionId) {
      body.session_id = state.selectedSessionId;
    }
    await request("/model-selection", {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (state.selectedSessionId) {
      await loadModelSelection();
    } else {
      setModelStatus(`Saved global default: ${provider} / ${model}`);
    }
  } finally {
    els.saveModel.disabled = false;
  }
}

async function loadMessages() {
  if (!state.selectedSessionId) {
    renderEmptyChat();
    return;
  }
  els.messages.innerHTML = `<div class="empty">Loading messages...</div>`;
  try {
    const messages = await request(`/sessions/${encodeURIComponent(state.selectedSessionId)}/messages`);
    renderMessages(messages);
  } catch (error) {
    els.messages.innerHTML = `<div class="status">${escapeHtml(error.message)}</div>`;
  }
}

async function sendMessage(event) {
  event.preventDefault();
  const content = els.messageContent.value.trim();
  if (!content || !state.selectedSessionId) {
    return;
  }

  els.messageContent.value = "";
  els.sendMessage.disabled = true;
  try {
    await request(`/sessions/${encodeURIComponent(state.selectedSessionId)}/messages`, {
      method: "POST",
      body: JSON.stringify({ content }),
    });
    await loadMessages();
  } finally {
    els.sendMessage.disabled = false;
    els.messageContent.focus();
  }
}

function renderSessions() {
  if (state.sessions.length === 0) {
    setSessionsStatus("No sessions yet.");
    return;
  }

  els.sessionsList.innerHTML = state.sessions
    .map((session) => {
      const active = session.id === state.selectedSessionId ? " active" : "";
      const title = escapeHtml(session.title || session.id);
      const tags = escapeHtml((session.tags || []).join(", ") || session.id);
      return `
        <button class="session-item${active}" data-session-id="${escapeHtml(session.id)}" type="button">
          <span class="session-title">${title}</span>
          <span class="session-tags">${tags}</span>
        </button>
      `;
    })
    .join("");

  for (const button of els.sessionsList.querySelectorAll(".session-item")) {
    button.addEventListener("click", () => selectSession(button.dataset.sessionId));
  }
}

function renderProviderOptions() {
  const selected = els.providerSelect.value;
  const values = uniqueProviders([...fallbackProviders, ...state.providers, selected].filter(Boolean));
  els.providerSelect.innerHTML = values
    .map((provider) => `<option value="${escapeHtml(provider)}">${escapeHtml(provider)}</option>`)
    .join("");
  if (selected && values.includes(selected)) {
    els.providerSelect.value = selected;
  }
}

function renderDeviceLogin(device) {
  els.deviceLogin.hidden = false;
  els.deviceLogin.innerHTML = `
    <a href="${escapeHtml(device.verification_url)}" target="_blank" rel="noreferrer">Open verification</a>
    <code>${escapeHtml(device.user_code)}</code>
    <span>Waiting for approval...</span>
  `;
  setAuthStatus("Approve the device code in ChatGPT.");
}

function renderModelSelection() {
  const session = state.sessions.find((item) => item.id === state.selectedSessionId);
  const tags = session?.tags?.length ? session.tags.join(", ") : state.selectedSessionId;
  const selection = state.modelSelection;
  if (!selection) {
    els.chatMeta.textContent = tags || "No model selected";
    return;
  }
  ensureProviderOption(selection.provider);
  els.providerSelect.value = selection.provider;
  els.modelName.value = selection.model;
  els.chatMeta.textContent = `${selection.provider} / ${selection.model} (${selection.scope})`;
  setModelStatus(`Current session uses ${selection.provider} / ${selection.model} from ${selection.scope}.`);
  if (tags && tags !== state.selectedSessionId) {
    els.chatMeta.textContent += ` · ${tags}`;
  }
}

function ensureProviderOption(provider) {
  if ([...els.providerSelect.options].some((option) => option.value === provider)) {
    return;
  }
  const option = document.createElement("option");
  option.value = provider;
  option.textContent = provider;
  els.providerSelect.append(option);
}

function uniqueProviders(providers) {
  return [...new Set(providers.map((provider) => provider.trim()).filter(Boolean))].sort();
}

function renderMessages(messages) {
  if (messages.length === 0) {
    els.messages.innerHTML = `<div class="empty">No messages yet.</div>`;
    return;
  }

  els.messages.innerHTML = messages
    .map((message) => {
      const role = escapeHtml(message.role || "event");
      const content = escapeHtml(message.content || "");
      return `
        <article class="message ${role}">
          <div class="message-role">${role}</div>
          <div class="message-content">${content}</div>
        </article>
      `;
    })
    .join("");
  els.messages.scrollTop = els.messages.scrollHeight;
}

function renderEmptyChat() {
  els.chatTitle.textContent = "Select a session";
  els.chatMeta.textContent = "No session selected";
  els.messageContent.disabled = true;
  els.sendMessage.disabled = true;
  setModelStatus("No session selected; saving sets the global default.");
  els.messages.innerHTML = `<div class="empty">Create or select a session.</div>`;
}

function setSessionsStatus(text) {
  els.sessionsList.innerHTML = `<div class="status">${escapeHtml(text)}</div>`;
}

function setModelStatus(text) {
  els.modelStatus.textContent = text;
}

function setAuthStatus(text) {
  els.authStatus.textContent = text;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function request(path, options = {}) {
  const response = await fetch(`${apiBase}${path}`, {
    headers: { "content-type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
