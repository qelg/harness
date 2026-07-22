const apiBase = new URLSearchParams(window.location.search).get("api") || "";

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
  messages: document.querySelector("#messages"),
  messageForm: document.querySelector("#messageForm"),
  messageContent: document.querySelector("#messageContent"),
  sendMessage: document.querySelector("#sendMessage"),
};

els.refreshSessions.addEventListener("click", () => loadSessions());
els.createSessionForm.addEventListener("submit", createSession);
els.modelForm.addEventListener("submit", saveModelSelection);
els.messageForm.addEventListener("submit", sendMessage);

await loadProviders();
await loadSessions();

async function loadProviders() {
  try {
    const payload = await request("/providers");
    state.providers = payload.providers || [];
    renderProviderOptions();
  } catch (error) {
    state.providers = [];
    renderProviderOptions();
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
  els.providerSelect.disabled = false;
  els.modelName.disabled = false;
  els.saveModel.disabled = false;
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
  if (!state.selectedSessionId) {
    return;
  }
  const provider = els.providerSelect.value.trim();
  const model = els.modelName.value.trim();
  if (!provider || !model) {
    return;
  }
  els.saveModel.disabled = true;
  try {
    await request("/model-selection", {
      method: "POST",
      body: JSON.stringify({ provider, model, session_id: state.selectedSessionId }),
    });
    await loadModelSelection();
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
  const values = state.providers.length > 0 ? state.providers : ["mock-llm"];
  els.providerSelect.innerHTML = values
    .map((provider) => `<option value="${escapeHtml(provider)}">${escapeHtml(provider)}</option>`)
    .join("");
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
  els.providerSelect.disabled = true;
  els.modelName.disabled = true;
  els.saveModel.disabled = true;
  els.modelName.value = "";
  els.messages.innerHTML = `<div class="empty">Create or select a session.</div>`;
}

function setSessionsStatus(text) {
  els.sessionsList.innerHTML = `<div class="status">${escapeHtml(text)}</div>`;
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
