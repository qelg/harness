const apiBase = new URLSearchParams(window.location.search).get("api") || "";
const fallbackProviders = ["chatgpt-codex", "mock-llm", "openrouter", "openai-codex"];

const state = {
  sessions: [],
  providers: [],
  toolsets: [],
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
  toolsetsList: document.querySelector("#toolsetsList"),
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
  renderToolsetOptions(["default"]);
  setModelStatus("Loading providers...");
  await loadProviders();
  await loadToolsets();
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

async function loadToolsets() {
  try {
    const payload = await request("/toolsets");
    state.toolsets = payload.toolsets || [];
  } catch (error) {
    state.toolsets = ["default"];
    setModelStatus(`Using fallback toolsets. ${error.message}`);
  }
  renderToolsetOptions(state.modelSelection?.toolsets || ["default"]);
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
  const toolsets = selectedToolsets();
  if (!provider || !model) {
    setModelStatus("Provider and model are required.");
    return;
  }
  els.saveModel.disabled = true;
  try {
    const body = { provider, model, toolsets };
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
  const streamingSessionId = state.selectedSessionId;
  let streamingMessageId = null;
  try {
    appendTransientMessage({
      id: `pending-user-${Date.now()}`,
      role: "user",
      content,
    });
    streamingMessageId = appendTransientMessage({
      id: `pending-assistant-${Date.now()}`,
      role: "assistant",
      content: "",
    });
    await streamUserMessage(streamingSessionId, content, {
      onDelta(delta) {
        appendToMessage(streamingMessageId, delta);
      },
      onFailed(message) {
        replaceMessage(streamingMessageId, message);
      },
    });
    if (state.selectedSessionId === streamingSessionId) {
      await loadMessages();
    }
  } catch (error) {
    if (streamingMessageId !== null) {
      replaceMessage(streamingMessageId, {
        id: streamingMessageId,
        role: "assistant",
        content: `Streaming failed: ${error.message}`,
      });
    }
  } finally {
    els.sendMessage.disabled = false;
    els.messageContent.focus();
  }
}

async function streamUserMessage(sessionId, content, handlers) {
  const response = await fetch(`${apiBase}/sessions/${encodeURIComponent(sessionId)}/messages/stream`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ content }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Streaming response is not readable.");
  }

  for await (const event of readSse(response.body)) {
    if (event.type === "llm.delta") {
      handlers.onDelta(event.data.payload?.delta || "");
    } else if (event.type === "llm.run.failed") {
      handlers.onFailed(failedRunMessage(event.data));
    }
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

function renderToolsetOptions(selectedToolsets = []) {
  const selected = new Set(selectedToolsets.length ? selectedToolsets : ["default"]);
  const values = uniqueProviders([...(state.toolsets.length ? state.toolsets : ["default"]), ...selected]);
  els.toolsetsList.innerHTML = values
    .map((toolset) => {
      const checked = selected.has(toolset) ? " checked" : "";
      return `
        <label class="toolset-option">
          <input type="checkbox" value="${escapeHtml(toolset)}"${checked} />
          <span>${escapeHtml(toolset)}</span>
        </label>
      `;
    })
    .join("");
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
  renderToolsetOptions(selection.toolsets || ["default"]);
  els.chatMeta.textContent = `${selection.provider} / ${selection.model} (${selection.scope})`;
  setModelStatus(
    `Current session uses ${selection.provider} / ${selection.model} with ${(selection.toolsets || []).join(", ") || "no"} toolsets from ${selection.scope}.`
  );
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

function selectedToolsets() {
  return [...els.toolsetsList.querySelectorAll("input[type='checkbox']:checked")]
    .map((input) => input.value)
    .filter(Boolean);
}

function renderMessages(messages) {
  if (messages.length === 0) {
    els.messages.innerHTML = `<div class="empty">No messages yet.</div>`;
    return;
  }

  els.messages.innerHTML = messages
    .map((message) => messageHtml(message))
    .join("");
  els.messages.scrollTop = els.messages.scrollHeight;
}

function appendTransientMessage(message) {
  if (els.messages.querySelector(".empty")) {
    els.messages.innerHTML = "";
  }
  const id = String(message.id);
  els.messages.insertAdjacentHTML("beforeend", messageHtml({ ...message, id }));
  els.messages.scrollTop = els.messages.scrollHeight;
  return id;
}

function appendToMessage(messageId, delta) {
  if (!delta) {
    return;
  }
  const node = els.messages.querySelector(`[data-message-id="${cssEscape(messageId)}"] .message-content`);
  if (!node) {
    return;
  }
  node.textContent += delta;
  els.messages.scrollTop = els.messages.scrollHeight;
}

function replaceMessage(messageId, message) {
  const node = els.messages.querySelector(`[data-message-id="${cssEscape(messageId)}"]`);
  if (!node) {
    return;
  }
  node.outerHTML = messageHtml({ ...message, id: messageId });
  els.messages.scrollTop = els.messages.scrollHeight;
}

function failedRunMessage(event) {
  const error = event.payload?.error || "LLM run failed";
  return {
    id: event.id,
    role: "assistant",
    content: `LLM run failed: ${error}`,
  };
}

function messageHtml(message) {
  const role = escapeHtml(message.role || "event");
  const content = escapeHtml(formatMessageContent(message.content));
  return `
    <article class="message ${role}" data-message-id="${escapeHtml(message.id)}">
      <div class="message-role">${role}</div>
      <div class="message-content">${content}</div>
    </article>
  `;
}

function formatMessageContent(content) {
  if (content === null || content === undefined) {
    return "";
  }
  if (typeof content === "string") {
    return content;
  }
  return JSON.stringify(content, null, 2);
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

async function* readSse(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary !== -1) {
        const rawEvent = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const event = parseSseEvent(rawEvent);
        if (event) {
          yield event;
        }
        boundary = buffer.indexOf("\n\n");
      }
    }
    buffer += decoder.decode();
    const event = parseSseEvent(buffer);
    if (event) {
      yield event;
    }
  } finally {
    reader.releaseLock();
  }
}

function parseSseEvent(rawEvent) {
  let type = "message";
  const data = [];
  for (const line of rawEvent.split("\n")) {
    if (line.startsWith("event:")) {
      type = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice(5).trimStart());
    }
  }
  if (data.length === 0) {
    return null;
  }
  return { type, data: JSON.parse(data.join("\n")) };
}

function cssEscape(value) {
  if (window.CSS && typeof window.CSS.escape === "function") {
    return window.CSS.escape(String(value));
  }
  return String(value).replaceAll('"', '\\"');
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
