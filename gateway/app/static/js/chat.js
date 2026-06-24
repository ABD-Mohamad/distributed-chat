let ws = null;
let currentChatId = null;
let currentChatName = "";

async function loadChats() {
  const data = await api("GET", "/chats");
  if (!data) return;
  const list = document.getElementById("chatList");
  const empty = document.getElementById("chatListEmpty");
  list.innerHTML = "";
  if (data.length === 0) {
    empty.style.display = "block";
    return;
  }
  empty.style.display = "none";
  data.forEach(c => {
    const div = document.createElement("div");
    div.className = "chat-list-item" + (c.id === currentChatId ? " active" : "");
    div.dataset.id = c.id;
    div.innerHTML = `<div class="chat-name">${escapeHtml(c.name)}</div>`;
    div.addEventListener("click", () => selectChat(c.id, c.name));
    list.appendChild(div);
  });
}

async function selectChat(chatId, chatName) {
  currentChatId = chatId;
  currentChatName = chatName;
  document.querySelectorAll(".chat-list-item").forEach(el => el.classList.remove("active"));
  const item = document.querySelector(`.chat-list-item[data-id="${chatId}"]`);
  if (item) item.classList.add("active");
  document.querySelector(".no-chat").style.display = "none";
  document.getElementById("chatHeaderName").textContent = escapeHtml(chatName);
  document.getElementById("chatHeaderMeta").textContent = "";
  document.getElementById("msgInput").disabled = false;
  document.getElementById("sendBtn").disabled = false;
  clearMessages();

  const data = await api("GET", `/chats/${chatId}/messages`);
  if (data) {
    data.forEach(m => appendMessage(m, false));
  }

  connectWs(chatId);
}

function clearMessages() {
  const area = document.getElementById("messagesArea");
  area.innerHTML = "";
  document.getElementById("messagesEmpty").style.display = "none";
}

function appendMessage(m, animate) {
  const area = document.getElementById("messagesArea");
  const empty = document.getElementById("messagesEmpty");
  empty.style.display = "none";

  const div = document.createElement("div");
  const user = getUser();
  const isOwn = user && m.sender_id === user.id;
  div.className = "message" + (isOwn ? " own" : " other");

  const sentAt = m.sent_at ? new Date(m.sent_at + "Z") : new Date();
  const time = sentAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

  div.innerHTML = `
    <div class="sender">${escapeHtml(m.sender_username || "unknown")}</div>
    ${escapeHtml(m.body)}
    <div class="time">${time}</div>
  `;

  area.appendChild(div);
  area.scrollTop = area.scrollHeight;
}

function connectWs(chatId) {
  if (ws) {
    ws.close();
    ws = null;
  }
  const url = apiWs(`/ws/${chatId}`);
  ws = new WebSocket(url);
  ws.onmessage = (e) => {
    try {
      const m = JSON.parse(e.data);
      appendMessage(m, true);
    } catch {}
  };
  ws.onclose = () => {
    if (ws) {
      setTimeout(() => connectWs(chatId), 3000);
    }
  };
}

async function sendMessage() {
  const input = document.getElementById("msgInput");
  const text = input.value.trim();
  if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({ body: text }));
  input.value = "";
}

async function createChat() {
  const input = document.getElementById("newChatInput");
  const name = input.value.trim();
  if (!name) return;
  const data = await api("POST", "/chats", { name });
  if (data && data.id) {
    input.value = "";
    await loadChats();
    selectChat(data.id, data.name);
  }
}

function escapeHtml(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

async function logout() {
  const token = getToken();
  if (token) {
    try {
      await fetch("/logout", {
        method: "POST",
        headers: { "Authorization": "Bearer " + token }
      });
    } catch {}
  }
  if (ws) { ws.close(); ws = null; }
  clearToken();
  window.location.href = "/";
}

function toggleSidebar() {
  document.querySelector(".chat-sidebar").classList.toggle("open");
}
