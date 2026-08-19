#!/usr/bin/env node
// Telegram DM relay for the ExCode secretary.
//
// Polls new Telegram DMs through the existing @overpod/mcp-telegram stdio
// server (which shares the single authorized session via the daemon socket),
// enqueues one Ouroboros headless task per message, and never sends Telegram
// traffic itself. State on disk makes processing idempotent across restarts.
//
// Usage:
//   relay.mjs            — run the poll loop
//   relay.mjs list-chats — one-shot: print dialogs (for allowlist setup)
//   relay.mjs state      — one-shot: print current persisted state

import { spawn } from "node:child_process";
import { readFileSync, writeFileSync, renameSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";

const RUNNER = "/home/roomhacker/.local/share/ouroboros-secretary/mcp-telegram/mcp-telegram-runner.mjs";
const SHARE = "/home/roomhacker/.local/share/ouroboros-secretary/relay";
const CONFIG_PATH = join(SHARE, "relay-config.json");
const STATE_PATH = join(SHARE, "relay-state.json");
const LOG_PATH = join(SHARE, "relay.log");

const DEFAULTS = {
  pollSec: 5,
  typingSec: 10, // "печатает…" refresh interval while an Ouroboros task runs
  typingMaxSec: 1800, // safety cap: stop the indicator if a task never ends
  ouroborosUrl: "http://127.0.0.1:8765",
  allowChatIds: [], // numeric Telegram user ids allowed to reach the secretary; empty = paused
  settingsPath: "/home/roomhacker/Ouroboros/data/settings.json",
  maxProcessedIds: 1000,
};

function loadJson(path, fallback) {
  try { return JSON.parse(readFileSync(path, "utf8")); } catch { return fallback; }
}

const config = { ...DEFAULTS, ...loadJson(CONFIG_PATH, {}) };

function log(level, msg) {
  const line = `${new Date().toISOString()} [${level}] ${msg}`;
  mkdirSync(dirname(LOG_PATH), { recursive: true });
  writeFileSync(LOG_PATH, line + "\n", { flag: "a" });
  if (level === "ERROR") console.error(line); else console.log(line);
}

function saveState(state) {
  mkdirSync(dirname(STATE_PATH), { recursive: true });
  const tmp = STATE_PATH + ".tmp";
  writeFileSync(tmp, JSON.stringify(state, null, 2), { mode: 0o600 });
  renameSync(tmp, STATE_PATH);
}

function trimProcessed(state) {
  if (state.processed.length > config.maxProcessedIds) {
    state.processed = state.processed.slice(-Math.floor(config.maxProcessedIds / 2));
  }
}

// ── Minimal MCP stdio client (newline-delimited JSON-RPC) ───────────────────

function startMcp() {
  const child = spawn("/usr/bin/node", [RUNNER], { stdio: ["pipe", "pipe", "inherit"] });
  let buf = "";
  const pending = new Map();
  let nextId = 1;
  child.stdout.on("data", (chunk) => {
    buf += chunk.toString("utf8");
    let idx;
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      try {
        const msg = JSON.parse(line);
        if (Number.isInteger(msg.id) && pending.has(msg.id)) {
          const { resolve, reject } = pending.get(msg.id);
          pending.delete(msg.id);
          if (msg.error) reject(new Error(msg.error.message || JSON.stringify(msg.error)));
          else resolve(msg.result);
        }
      } catch (e) {
        log("ERROR", `unparseable MCP line: ${e.message}`);
      }
    }
  });
  child.on("exit", (code, signal) => {
    for (const { reject } of pending.values()) reject(new Error(`telegram MCP exited (code=${code} signal=${signal})`));
    pending.clear();
  });
  const request = (method, params) => new Promise((resolve, reject) => {
    const id = nextId++;
    pending.set(id, { resolve, reject });
    child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
    const timer = setTimeout(() => {
      if (pending.has(id)) { pending.delete(id); reject(new Error(`MCP ${method} timed out`)); }
    }, 120000);
    timer.unref?.();
  });
  const notify = (method) => child.stdin.write(JSON.stringify({ jsonrpc: "2.0", method }) + "\n");
  return {
    async init() {
      await request("initialize", {
        protocolVersion: "2025-03-26",
        capabilities: {},
        clientInfo: { name: "excode-telegram-relay", version: "1.2.0" },
      });
      notify("notifications/initialized");
    },
    callTool(name, args) {
      return request("tools/call", { name, arguments: args });
    },
    stop() { try { child.kill(); } catch {} },
  };
}

function toolText(result) {
  const text = (result?.content || []).filter((c) => c.type === "text").map((c) => c.text).join("\n");
  if (result?.isError) throw new Error(`telegram tool failed: ${text.slice(0, 300)}`);
  return text;
}

// ── Ouroboros task enqueue ──────────────────────────────────────────────────

function ouroborosPassword() {
  const settings = loadJson(config.settingsPath, {});
  const pw = String(settings.OUROBOROS_NETWORK_PASSWORD || "").trim();
  if (!pw) throw new Error("OUROBOROS_NETWORK_PASSWORD missing in settings");
  return pw;
}

async function enqueueSecretaryTask(m, selfId) {
  const taskId = `tg-${m.peer.id}-${m.id}`;
  const dateIso = new Date(m.date * 1000).toISOString();
  const text = (m.text || "").trim() || "(сообщение без текста: медиа или сервисное)";
  const description = [
    "Входящее личное сообщение Telegram для секретаря ExCode. Тебе доступны MCP mcp_affine_* и mcp_telegram_*.",
    "",
    `Отправитель: user id ${m.fromId}; диалог (chatId для ответа): ${m.peer.id}; message id: ${m.id}; время: ${dateIso}.`,
    `Текст сообщения: «${text}»`,
    "",
    "Порядок работы:",
    "1. Прочитай в AFFiNE (workspace 36c0a6f7-b562-47f1-b60a-5a2b14d1043f) документ «Секретарь · Правила ответов в Telegram» (find_doc_by_title).",
    "2. При необходимости прочитай «Секретарь · Люди», «Секретарь · Проекты», «Секретарь · Операционный журнал» и документы родителя «ExCode · Operating System».",
    `3. Ответь отправителю в тот же диалог: mcp_telegram__telegram-send-message, chatId "${m.peer.id}", replyTo ${m.id}. Один ответ, по-русски, живой тон, без канцелярита, без выдуманных фактов.`,
    "4. Если сообщение содержит поручение, решение или новый факт — добавь короткую запись в «Секретарь · Операционный журнал» (append_markdown). Не создавай дубликаты документов.",
    "5. Если что-то требует решения человека — прямо пометь это в ответе («требует твоего решения»).",
    "6. Писать другим людям (не отправителю) можно только согласно действующим мандатам, записанным в «Секретарь · Операционный журнал». Не спамить: один адресат — одно осмысленное сообщение. Ночью (22:00–09:00 МСК) людям не писать — готовь черновики в AFFiNE и отложи отправку.",
  ].join("\n");
  const resp = await fetch(`${config.ouroborosUrl}/api/tasks`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-ouroboros-password": ouroborosPassword(),
    },
    body: JSON.stringify({ task_id: taskId, description }),
  });
  const body = await resp.json().catch(() => ({}));
  if (resp.status === 409) return { skipped: true, reason: "task already exists" };
  if (!resp.ok) throw new Error(`ouroboros ${resp.status}: ${JSON.stringify(body).slice(0, 200)}`);
  return { skipped: false, taskId: body.task_id };
}

async function taskStatus(taskId) {
  const resp = await fetch(`${config.ouroborosUrl}/api/tasks/${taskId}`, {
    headers: { "x-ouroboros-password": ouroborosPassword() },
  });
  if (!resp.ok) throw new Error(`ouroboros ${resp.status}`);
  const body = await resp.json();
  const t = body.task || body;
  return String(t.status || "");
}

// Keep the "typing…" indicator alive in one chat while its Ouroboros task is
// running, so the person sees the secretary is working, not hung.
async function watchTaskWithTyping(mcp, taskId, chatId) {
  const deadline = Date.now() + config.typingMaxSec * 1000;
  let status = "";
  let ticks = 0;
  while (Date.now() < deadline) {
    try {
      await mcp.callTool("telegram-send-typing", { chatId: String(chatId) });
      ticks++;
    } catch (e) {
      log("WARN", `typing to ${chatId} failed: ${e.message}`);
    }
    await sleep(config.typingSec * 1000);
    try {
      status = await taskStatus(taskId);
    } catch (e) {
      log("WARN", `status of ${taskId} failed: ${e.message}`);
      continue;
    }
    if (["completed", "failed", "cancelled"].includes(status)) break;
  }
  try { await mcp.callTool("telegram-send-typing", { chatId: String(chatId), action: "cancel" }); } catch {}
  log("INFO", `typing watcher done for ${taskId} in ${chatId}: status=${status || "timeout"}, ticks=${ticks}`);
}

// ── One-shot helpers ────────────────────────────────────────────────────────

async function listChats(mcp) {
  const text = toolText(await mcp.callTool("telegram-list-chats", { limit: 30 }));
  log("INFO", "list-chats requested via one-shot mode");
  console.log(text);
}

// ── Main loop ───────────────────────────────────────────────────────────────

async function main() {
  const mode = process.argv[2] || "run";
  const mcp = startMcp();
  try {
    await mcp.init();
    const statusText = toolText(await mcp.callTool("telegram-status", {}));
    const selfId = (statusText.match(/id:\s*(\d+)/) || [])[1] || "";
    if (!selfId) throw new Error(`cannot determine own user id from status: ${statusText}`);
    log("INFO", `connected as secretary account id=${selfId} (${statusText})`);

    if (mode === "list-chats") { await listChats(mcp); return; }
    if (mode === "state") { console.log(JSON.stringify(loadJson(STATE_PATH, null), null, 2)); return; }

    let state = loadJson(STATE_PATH, null);
    if (!state) {
      const st = JSON.parse(toolText(await mcp.callTool("telegram-get-state", {})));
      state = { cursor: { pts: st.pts, qts: st.qts, date: st.date }, processed: [] };
      saveState(state);
      log("INFO", `baseline cursor set (pts=${st.pts}); backlog will not be processed`);
    }

    log("INFO", `relay running; allowChatIds=${JSON.stringify(config.allowChatIds)} pollSec=${config.pollSec}`);

    // If the shared telegram daemon hangs, a fresh stdio client (via systemd
    // restart) is the observed recovery path — exit and let systemd respawn us.
    let consecutiveFailures = 0;
    const MAX_CONSECUTIVE_FAILURES = 10;

    for (;;) {
      let diff;
      try {
        diff = JSON.parse(toolText(await mcp.callTool("telegram-get-updates", {
          pts: state.cursor.pts, qts: state.cursor.qts, date: state.cursor.date,
        })));
        consecutiveFailures = 0;
      } catch (e) {
        consecutiveFailures++;
        log("ERROR", `get-updates failed (${consecutiveFailures}/${MAX_CONSECUTIVE_FAILURES}): ${e.message}`);
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          log("ERROR", "telegram path unresponsive for too long, exiting for systemd restart");
          process.exit(1);
        }
        await sleep(config.pollSec * 1000);
        continue;
      }
      state.cursor = { pts: diff.state.pts, qts: diff.state.qts, date: diff.state.date };
      saveState(state);

      if (diff.fallback?.kind === "tooLong") {
        const st = JSON.parse(toolText(await mcp.callTool("telegram-get-state", {})));
        state.cursor = { pts: st.pts, qts: st.qts, date: st.date };
        saveState(state);
        log("WARN", "update gap too long, resynced cursor; skipping backlog");
      }

      for (const m of diff.newMessages || []) {
        const key = `${m.peer.kind}:${m.peer.id}:${m.id}`;
        if (state.processed.includes(key)) continue;

        // Policy skips are final: mark processed so they are never re-examined.
        const skip = (reason) => {
          state.processed.push(key);
          trimProcessed(state);
          saveState(state);
          log("INFO", `skip message ${key}: ${reason}`);
        };
        if (m.isService) { skip("service message"); continue; }
        if (m.peer.kind !== "user") { skip("not a DM"); continue; }
        if (String(m.fromId?.id ?? m.fromId) === selfId) { skip("own outgoing message"); continue; }
        if (String(m.peer.id) === selfId) { skip("Saved Messages"); continue; }
        if (!(m.text || "").trim()) { skip("no text (media)"); continue; }
        if (!config.allowChatIds.map(String).includes(String(m.peer.id))) {
          skip("sender not in allowChatIds");
          continue;
        }
        try {
          // Read receipt right away: the message reached the secretary.
          try {
            await mcp.callTool("telegram-mark-as-read", { chatId: String(m.peer.id) });
          } catch (e) {
            log("WARN", `mark-as-read for ${m.peer.id} failed: ${e.message}`);
          }
          const res = await enqueueSecretaryTask(m, selfId);
          state.processed.push(key);
          trimProcessed(state);
          saveState(state);
          if (res.skipped) {
            log("INFO", `message ${key}: ${res.reason}`);
          } else {
            log("INFO", `message ${key}: enqueued ouroboros task ${res.taskId}`);
            watchTaskWithTyping(mcp, res.taskId, m.peer.id).catch((e) =>
              log("ERROR", `typing watcher for ${res.taskId} crashed: ${e.message}`));
          }
        } catch (e) {
          // Not marked processed: retried next poll; the deterministic task_id
          // makes the retry idempotent on the Ouroboros side (409 = exists).
          log("ERROR", `message ${key}: enqueue failed (will retry): ${e.message}`);
        }
      }

      if (diff.isFinal === false) continue; // drain queued updates immediately
      await sleep(config.pollSec * 1000);
    }
  } finally {
    mcp.stop();
  }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

main().catch((e) => { log("ERROR", `fatal: ${e.message}`); process.exit(1); });
