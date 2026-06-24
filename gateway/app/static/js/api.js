const TOKEN_KEY = "nexuschat_token";
const USER_KEY = "nexuschat_user";

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setToken(token) {
  localStorage.setItem(TOKEN_KEY, token);
}

function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
}

function getUser() {
  const raw = localStorage.getItem(USER_KEY);
  if (raw) {
    try { return JSON.parse(raw); } catch {}
  }
  return null;
}

function setUser(user) {
  localStorage.setItem(USER_KEY, JSON.stringify(user));
}

function parseJwt(token) {
  try {
    const payload = token.split(".")[1];
    return JSON.parse(atob(payload));
  } catch {
    return null;
  }
}

async function api(method, path, body) {
  const opts = {
    method,
    headers: { "Content-Type": "application/json" },
  };
  const token = getToken();
  if (token) opts.headers["Authorization"] = "Bearer " + token;
  if (body !== undefined) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  if (r.status === 401) {
    clearToken();
    if (!window.location.pathname.startsWith("/dashboard") && !window.location.pathname.startsWith("/admin")) {
      window.location.href = "/";
    }
    return null;
  }
  return r.json();
}

function apiWs(path) {
  const token = getToken();
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}${path}?token=${token}`;
}
