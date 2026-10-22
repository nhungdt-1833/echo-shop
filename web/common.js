// Shared helpers for the Task 2 test pages. Plain JS, no build step.
const state = { token: null, ws: null };
const $ = (id) => document.getElementById(id);

// textContent (never innerHTML): server/chat data is shown as text, so a
// message like "<script>alert(1)</script>" cannot run.
function log(text, kind = "") {
  const li = document.createElement("li");
  li.className = kind;
  li.textContent = `${new Date().toLocaleTimeString()}  ${text}`;
  $("log").prepend(li);
}

async function api(method, path, body) {
  const headers = { "Content-Type": "application/json" };
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${res.status} ${data.message || ""}`);
  return data;
}

async function login() {
  try {
    const data = await api("POST", "/login", { email: $("email").value, password: $("password").value });
    state.token = data.token;
    log("login ok", "ok");
  } catch (e) {
    log(`login failed: ${e.message}`, "err");
  }
}

function connect() {
  if (!state.token) return log("login first", "err");
  if (state.ws) state.ws.close();
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws?token=${encodeURIComponent(state.token)}`);
  ws.onopen = () => log("ws connected", "ok");
  ws.onclose = (e) => log(`ws closed (code ${e.code})`, "err");
  ws.onmessage = (e) => log(`⬇ ${e.data}`, "in");
  state.ws = ws;
}

function wsSend(msg) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return log("not connected", "err");
  state.ws.send(JSON.stringify(msg));
  log(`⬆ ${JSON.stringify(msg)}`, "out");
}

// --- chat room "order:<id>" ---
const room = () => `order:${$("room-order").value}`;
const joinRoom = () => wsSend({ type: "join", room: room() });
const leaveRoom = () => wsSend({ type: "leave", room: room() });

function sendChat() {
  const input = $("chat-text");
  if (!input.value.trim()) return;
  wsSend({ type: "chat", room: room(), text: input.value });
  input.value = "";
}

let lastTyping = 0;
function onTyping() {
  const now = Date.now();
  if (now - lastTyping < 1000) return; // at most one "typing" per second
  lastTyping = now;
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify({ type: "typing", room: room() }));
  }
}
