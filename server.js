require("dotenv").config();
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");
const nodemailer = require("nodemailer");
const { initializeApp, cert } = require("firebase-admin/app");
const { getMessaging } = require("firebase-admin/messaging");

const DB_FILE = path.join(__dirname, "database.json");

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

let mailTransporter = null;
let mailEnabled = false;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  mailTransporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: process.env.SMTP_PORT ? Number(process.env.SMTP_PORT) : 587,
    secure: process.env.SMTP_SECURE === "true",
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });
  mailEnabled = true;
  console.log("Nodemailer configured — welcome emails enabled.");
} else {
  console.log(
    "SMTP_HOST/SMTP_USER/SMTP_PASS not set — emails will be required and stored, but not actually sent.",
  );
}

function isValidEmail(email) {
  return typeof email === "string" && EMAIL_RE.test(email.trim());
}

async function sendWelcomeEmail(toEmail, username) {
  if (!mailEnabled) return;
  try {
    await mailTransporter.sendMail({
      from: process.env.MAIL_FROM || process.env.SMTP_USER,
      to: toEmail,
      subject: "خوش آمدید به بهاران",
      text: `سلام ${username}،\n\nحساب کاربری شما با موفقیت ثبت شد و این ایمیل به آن متصل شده است.\n\nبهاران`,
    });
  } catch (err) {
    console.error(
      "Failed to send welcome email to",
      toEmail,
      err.message || err,
    );
  }
}

const resetCodes = new Map();
const RESET_CODE_TTL_MS = 10 * 60 * 1000;
const RESET_CODE_RESEND_COOLDOWN_MS = 60 * 1000;

const groupDeleteCodes = new Map();

function findUserByEmail(email) {
  const lower = email.trim().toLowerCase();
  const entry = Object.entries(db.users).find(
    ([, u]) => u.email && u.email.toLowerCase() === lower,
  );
  return entry ? { username: entry[0], user: entry[1] } : null;
}

function generateResetCode() {
  return String(crypto.randomInt(0, 1000000)).padStart(6, "0");
}

async function sendResetCodeEmail(toEmail, username, code) {
  if (!mailEnabled) {
    console.log(
      `[dev] Password reset code for ${username} <${toEmail}>: ${code}`,
    );
    return true;
  }
  try {
    await mailTransporter.sendMail({
      from: process.env.MAIL_FROM || process.env.SMTP_USER,
      to: toEmail,
      subject: "کد بازیابی رمز عبور بهاران",
      text: `سلام ${username}،\n\nکد بازیابی رمز عبور شما: ${code}\n\nاین کد تا ۱۰ دقیقه دیگر معتبر است. اگر این درخواست را شما نداده‌اید، این ایمیل را نادیده بگیرید.\n\nبهاران`,
    });
    return true;
  } catch (err) {
    console.error(
      "Failed to send reset code email to",
      toEmail,
      err.message || err,
    );
    return false;
  }
}

async function sendGroupDeleteCodeEmail(toEmail, username, chatName, code) {
  if (!mailEnabled) {
    console.log(
      `[dev] Group deletion code for "${chatName}" requested by ${username} <${toEmail}>: ${code}`,
    );
    return true;
  }
  try {
    await mailTransporter.sendMail({
      from: process.env.MAIL_FROM || process.env.SMTP_USER,
      to: toEmail,
      subject: "کد تایید حذف گروه در بهاران",
      text: `سلام ${username}،\n\nکد تایید حذف گروه/سرور «${chatName}»: ${code}\n\nاین کد تا ۱۰ دقیقه دیگر معتبر است. اگر این درخواست را شما نداده‌اید، این ایمیل را نادیده بگیرید و گروه شما حذف نخواهد شد.\n\nبهاران`,
    });
    return true;
  } catch (err) {
    console.error(
      "Failed to send group-delete code email to",
      toEmail,
      err.message || err,
    );
    return false;
  }
}

const SERVICE_ACCOUNT_PATH = path.join(
  __dirname,
  "firebase-service-account.json",
);
let firebaseEnabled = false;
let messaging = null;
if (fs.existsSync(SERVICE_ACCOUNT_PATH)) {
  const app = initializeApp({
    credential: cert(require(SERVICE_ACCOUNT_PATH)),
  });
  messaging = getMessaging(app);
  firebaseEnabled = true;
  console.log("Firebase Admin initialized — call push notifications enabled.");
} else {
  console.log(
    "firebase-service-account.json not found — calls will only ring while the recipient's WebSocket is connected.",
  );
}

const pendingCalls = new Map();
const PENDING_CALL_TIMEOUT_MS = 30000;

const UPLOADS_DIR = path.join(__dirname, "public", "uploads");
fs.mkdirSync(UPLOADS_DIR, { recursive: true });

function saveBase64File(dataUrl, suggestedName) {
  const match = /^data:(.+?);base64,(.+)$/.exec(dataUrl || "");
  if (!match) return null;
  const mime = match[1];
  const base64Payload = match[2];
  let ext = path.extname(suggestedName || "");
  if (!ext) {
    const guess = mime.split("/")[1];
    ext = guess ? "." + guess.split("+")[0] : "";
  }
  const filename = `${Date.now()}_${crypto.randomBytes(4).toString("hex")}${ext}`;
  fs.writeFileSync(
    path.join(UPLOADS_DIR, filename),
    Buffer.from(base64Payload, "base64"),
  );
  return `/uploads/${filename}`;
}

function findMessageById(id) {
  let found = db.messages.find((m) => m.id === id);
  if (found) return found;
  for (const chatId in db.chats) {
    found = db.chats[chatId].messages.find((m) => m.id === id);
    if (found) return found;
  }
  for (const serverId in db.servers) {
    for (const ch of db.servers[serverId].channels) {
      found = ch.messages.find((m) => m.id === id);
      if (found) return found;
    }
  }
  return null;
}

function buildReplyPreview(replyToId) {
  if (!replyToId) return null;
  const original = findMessageById(replyToId);
  if (!original) return null;
  return {
    sender: original.sender,
    text: original.isDeletedForEveryone
      ? "پیام حذف شده"
      : original.text || (original.fileType ? "پیوست" : ""),
  };
}

function getLastDirectMessage(currentUsername, otherUsername) {
  const relevant = db.messages.filter(
    (m) =>
      ((m.sender === currentUsername && m.recipient === otherUsername) ||
        (m.sender === otherUsername && m.recipient === currentUsername)) &&
      (!m.deletedFor || !m.deletedFor.includes(currentUsername)),
  );
  const last = relevant[relevant.length - 1] || null;
  return last
    ? { text: last.text, timestamp: last.timestamp, sender: last.sender }
    : null;
}

function getUserChats(username) {
  return Object.values(db.chats)
    .filter((chat) => chat.members.includes(username))
    .map((chat) => {
      const visibleMessages = chat.messages.filter(
        (m) => !m.deletedFor || !m.deletedFor.includes(username),
      );
      const last = visibleMessages[visibleMessages.length - 1] || null;
      return {
        id: chat.id,
        name: chat.name,
        type: chat.type,
        lastMessage: last
          ? { text: last.text, timestamp: last.timestamp, sender: last.sender }
          : null,
        unreadCount: 0,
      };
    });
}

/* ---------------- Discord-like server (guild) helpers ---------------- */

const ALL_PERMISSIONS = [
  "ADMINISTRATOR",
  "MANAGE_SERVER",
  "MANAGE_CHANNELS",
  "MANAGE_ROLES",
  "KICK_MEMBERS",
  "MANAGE_MESSAGES",
  "SEND_MESSAGES",
  "VIEW_CHANNELS",
];

function makeEveryoneRole() {
  return {
    id: "everyone",
    name: "@everyone",
    color: null,
    permissions: ["SEND_MESSAGES", "VIEW_CHANNELS"],
    position: 0,
    isEveryone: true,
  };
}

function getServerMemberRoles(server, username) {
  if (!server.memberRoles) server.memberRoles = {};
  const ids = server.memberRoles[username] || [];
  const roles = [server.roles.find((r) => r.id === "everyone")].filter(Boolean);
  ids.forEach((rid) => {
    const r = server.roles.find((x) => x.id === rid);
    if (r) roles.push(r);
  });
  return roles;
}

function getMemberPermissions(server, username) {
  if (server.owner === username) return new Set(ALL_PERMISSIONS);
  const roles = getServerMemberRoles(server, username);
  const perms = new Set();
  roles.forEach((r) => (r.permissions || []).forEach((p) => perms.add(p)));
  if (perms.has("ADMINISTRATOR")) return new Set(ALL_PERMISSIONS);
  return perms;
}

function memberHasPermission(server, username, permission) {
  if (!server || !server.members.includes(username)) return false;
  const perms = getMemberPermissions(server, username);
  return perms.has(permission);
}

function highestRoleColor(server, username) {
  if (server.owner === username) return "#e0b23c";
  const roles = getServerMemberRoles(server, username)
    .filter((r) => !r.isEveryone && r.color)
    .sort((a, b) => b.position - a.position);
  return roles.length ? roles[0].color : null;
}

function buildServerMembersPayload(server) {
  return server.members.map((m) => ({
    username: m,
    isOwner: server.owner === m,
    roleIds: (server.memberRoles && server.memberRoles[m]) || [],
    color: highestRoleColor(server, m),
  }));
}

function buildServerSnapshot(server, username) {
  const perms = Array.from(getMemberPermissions(server, username));
  return {
    id: server.id,
    name: server.name,
    icon: server.icon || null,
    owner: server.owner,
    roles: server.roles,
    categories: server.categories.sort((a, b) => a.position - b.position),
    channels: server.channels
      .sort((a, b) => a.position - b.position)
      .map((c) => ({
        id: c.id,
        name: c.name,
        type: c.type,
        categoryId: c.categoryId,
        position: c.position,
      })),
    members: buildServerMembersPayload(server),
    myPermissions: perms,
  };
}

function findChannel(server, channelId) {
  return server.channels.find((c) => c.id === channelId);
}

/* ---------------------------------------------------------------------- */

async function sendCallPush(token, fromUsername, callType, targetUsername) {
  if (!firebaseEnabled) return;
  await messaging.send({
    token,
    android: {
      priority: "high",
      ttl: PENDING_CALL_TIMEOUT_MS,
    },
    data: {
      type: "call-invite",
      caller: fromUsername,
      callType: callType === "video" ? "video" : "audio",
      target: targetUsername,
    },
  });
}
function deliverPendingCallIfAny(ws, username) {
  const pending = pendingCalls.get(username);
  if (!pending) return;
  pendingCalls.delete(username);
  clearTimeout(pending.timeoutHandle);
  if (Date.now() > pending.expiresAt) return;
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(
      JSON.stringify({
        type: "call-invite",
        from: pending.from,
        callType: pending.callType,
      }),
    );
  }
}

function upgradeIncomingMessagesToDelivered(username) {
  const messageIdsBySender = {};
  db.messages.forEach((m) => {
    if (m.recipient === username && m.status === "sent") {
      m.status = "delivered";
      if (!messageIdsBySender[m.sender]) messageIdsBySender[m.sender] = [];
      messageIdsBySender[m.sender].push(m.id);
    }
  });

  const senders = Object.keys(messageIdsBySender);
  if (senders.length === 0) return;
  saveDb(db);

  senders.forEach((sender) => {
    const senderWs = getWsByUsername(sender);
    if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      senderWs.send(
        JSON.stringify({
          type: "message-status",
          status: "delivered",
          messageIds: messageIdsBySender[sender],
        }),
      );
    }
  });
}

function loadDb() {
  if (!fs.existsSync(DB_FILE)) {
    const initialData = { users: {}, messages: [], chats: {}, servers: {} };
    fs.writeFileSync(DB_FILE, JSON.stringify(initialData, null, 2));
    return initialData;
  }
  try {
    const data = fs.readFileSync(DB_FILE, "utf8");
    const parsed = JSON.parse(data);
    if (!parsed.chats) parsed.chats = {};
    if (!parsed.servers) parsed.servers = {};
    return parsed;
  } catch (err) {
    console.error("Error reading database file, resetting:", err);
    return { users: {}, messages: [], chats: {}, servers: {} };
  }
}

function saveDb(db) {
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
}

let db = loadDb();

const requestHandler = (req, res) => {
  let targetFile = req.url === "/" ? "/index.html" : req.url;
  let filePath = path.join(__dirname, "public", targetFile);

  let extname = String(path.extname(filePath)).toLowerCase();
  let mimeTypes = {
    ".html": "text/html",
    ".js": "text/javascript",
    ".css": "text/css",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpg",
    ".jpeg": "image/jpg",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mp3": "audio/mpeg",
    ".ogg": "audio/ogg",
    ".pdf": "application/pdf",
    ".zip": "application/zip",
    ".txt": "text/plain",
  };

  let contentType = mimeTypes[extname] || "application/octet-stream";

  fs.readFile(filePath, (error, content) => {
    if (error) {
      if (error.code === "ENOENT") {
        fs.readFile(
          path.join(__dirname, "public", "index.html"),
          (err, htmlContent) => {
            if (err) {
              res.writeHead(404, { "Content-Type": "text/plain" });
              res.end("404: index.html not found in public folder!");
            } else {
              res.writeHead(200, { "Content-Type": "text/html" });
              res.end(htmlContent, "utf-8");
            }
          },
        );
      } else {
        res.writeHead(500);
        res.end("Server error: " + error.code);
      }
    } else {
      res.writeHead(200, { "Content-Type": contentType });
      res.end(content, "utf-8");
    }
  });
};

const server = http.createServer(requestHandler);

const wss = new WebSocket.Server({ server });
const clients = new Map();
const rooms = new Map();
const MESSAGES_PAGE_SIZE = 10;
function paginateMessages(fullList, beforeMessageId) {
  let endIndex = fullList.length;
  if (beforeMessageId) {
    const idx = fullList.findIndex((m) => m.id === beforeMessageId);
    if (idx >= 0) endIndex = idx;
  }
  const startIndex = Math.max(0, endIndex - MESSAGES_PAGE_SIZE);
  const page = fullList.slice(startIndex, endIndex);
  const hasMore = startIndex > 0;
  return { page, hasMore };
}

function getFullMessageListFor(currentUsername, target) {
  if (target.startsWith("group_") || target.startsWith("announcement_")) {
    const chat = db.chats[target];
    return chat
      ? chat.messages.filter(
          (m) => !m.deletedFor || !m.deletedFor.includes(currentUsername),
        )
      : [];
  }
  return db.messages.filter(
    (m) =>
      ((m.sender === currentUsername && m.recipient === target) ||
        (m.sender === target && m.recipient === currentUsername)) &&
      (!m.deletedFor || !m.deletedFor.includes(currentUsername)),
  );
}

function isOnline(username) {
  for (const uname of clients.values()) {
    if (uname === username) return true;
  }
  return false;
}

function getWsByUsername(username) {
  for (const [wsClient, uname] of clients.entries()) {
    if (uname === username) return wsClient;
  }
  return null;
}

function enrichContacts(currentUsername, usernames) {
  return usernames.map((cUsername) => {
    const u = db.users[cUsername];
    return {
      username: cUsername,
      profilePic: u ? u.profilePic : null,
      online: isOnline(cUsername),
      lastSeen: u ? u.lastSeen || null : null,
      lastMessage: getLastDirectMessage(currentUsername, cUsername),
    };
  });
}

function broadcastPresence(username, online, lastSeen) {
  for (const [ownerName, ownerData] of Object.entries(db.users)) {
    if (ownerData.contacts.includes(username)) {
      const ownerWs = getWsByUsername(ownerName);
      if (ownerWs && ownerWs.readyState === WebSocket.OPEN) {
        ownerWs.send(
          JSON.stringify({
            type: "presence-update",
            username,
            online,
            lastSeen,
          }),
        );
      }
    }
  }
}

function buildChatMembersPayload(chat) {
  return chat.members.map((m) => ({
    username: m,
    isOwner: chat.owner === m,
    isAdmin: chat.admins.includes(m),
  }));
}

function broadcastToServerMembers(server, payload) {
  server.members.forEach((member) => {
    const clientWs = getWsByUsername(member);
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify(payload));
    }
  });
}

function heartbeat() {
  this.isAlive = true;
}

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

wss.on("close", () => clearInterval(heartbeatInterval));

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", heartbeat);

  let currentUsername = null;

  ws.on("message", (message) => {
    let msg;
    try {
      msg = JSON.parse(message);
    } catch (e) {
      return;
    }

    switch (msg.type) {
      case "signup": {
        if (db.users[msg.username]) {
          ws.send(
            JSON.stringify({
              type: "auth-error",
              message: "این نام کاربری قبلاً ثبت‌نام شده است.",
            }),
          );
          return;
        }

        const signupEmail = (msg.email || "").trim();
        if (!isValidEmail(signupEmail)) {
          ws.send(
            JSON.stringify({
              type: "auth-error",
              message: "لطفاً یک ایمیل معتبر وارد کنید.",
            }),
          );
          return;
        }
        const emailTaken = Object.values(db.users).some(
          (u) => u.email && u.email.toLowerCase() === signupEmail.toLowerCase(),
        );
        if (emailTaken) {
          ws.send(
            JSON.stringify({
              type: "auth-error",
              message: "این ایمیل قبلاً برای حساب دیگری استفاده شده است.",
            }),
          );
          return;
        }

        db.users[msg.username] = {
          password: msg.password,
          email: signupEmail,
          profilePic: null,
          contacts: [],
          lastSeen: null,
          theme: "dark",
        };
        saveDb(db);

        currentUsername = msg.username;
        clients.set(ws, currentUsername);
        ws.send(
          JSON.stringify({
            type: "auth-success",
            username: currentUsername,
            email: signupEmail,
            profilePic: null,
            contacts: [],
            chats: getUserChats(currentUsername),
            servers: getUserServers(currentUsername),
            theme: "dark",
          }),
        );
        broadcastPresence(currentUsername, true, null);
        sendWelcomeEmail(signupEmail, currentUsername);
        break;
      }

      case "login": {
        const identifier = (msg.username || "").trim();
        let user = db.users[identifier];
        let resolvedUsername = identifier;
        if (!user && identifier.includes("@")) {
          const found = findUserByEmail(identifier);
          if (found) {
            user = found.user;
            resolvedUsername = found.username;
          }
        }
        if (!user || user.password !== msg.password) {
          ws.send(
            JSON.stringify({
              type: "auth-error",
              message: "نام کاربری/ایمیل یا رمز عبور اشتباه است.",
            }),
          );
          return;
        }
        currentUsername = resolvedUsername;
        clients.set(ws, currentUsername);

        ws.send(
          JSON.stringify({
            type: "auth-success",
            username: currentUsername,
            email: user.email || null,
            profilePic: user.profilePic,
            contacts: enrichContacts(currentUsername, user.contacts),
            chats: getUserChats(currentUsername),
            servers: getUserServers(currentUsername),
            theme: user.theme || "dark",
          }),
        );
        broadcastPresence(currentUsername, true, null);
        deliverPendingCallIfAny(ws, currentUsername);
        upgradeIncomingMessagesToDelivered(currentUsername);
        break;
      }

      case "sync-check": {
        const user = db.users[msg.username];
        if (user) {
          currentUsername = msg.username;
          clients.set(ws, currentUsername);

          ws.send(
            JSON.stringify({
              type: "sync-data",
              username: currentUsername,
              email: user.email || null,
              profilePic: user.profilePic,
              contacts: enrichContacts(currentUsername, user.contacts),
              chats: getUserChats(currentUsername),
              servers: getUserServers(currentUsername),
              theme: user.theme || "dark",
            }),
          );
          broadcastPresence(currentUsername, true, null);
          deliverPendingCallIfAny(ws, currentUsername);
          upgradeIncomingMessagesToDelivered(currentUsername);
        }
        break;
      }

      case "register-push-token": {
        if (!currentUsername || !db.users[currentUsername]) return;
        if (typeof msg.token === "string" && msg.token.length > 0) {
          db.users[currentUsername].pushToken = msg.token;
          saveDb(db);
        }
        break;
      }

      case "update-profile": {
        if (!currentUsername || !db.users[currentUsername]) return;
        if (msg.profilePic !== undefined) {
          if (
            typeof msg.profilePic === "string" &&
            msg.profilePic.startsWith("data:")
          ) {
            const url = saveBase64File(msg.profilePic, "avatar.png");
            db.users[currentUsername].profilePic =
              url || db.users[currentUsername].profilePic;
          } else {
            db.users[currentUsername].profilePic = msg.profilePic;
          }
        }
        if (msg.theme !== undefined)
          db.users[currentUsername].theme = msg.theme;
        saveDb(db);

        ws.send(
          JSON.stringify({
            type: "profile-updated",
            profilePic: db.users[currentUsername].profilePic,
            theme: db.users[currentUsername].theme,
          }),
        );
        break;
      }

      case "set-email": {
        if (!currentUsername || !db.users[currentUsername]) return;
        const newEmail = (msg.email || "").trim();
        if (!isValidEmail(newEmail)) {
          ws.send(
            JSON.stringify({
              type: "email-error",
              message: "لطفاً یک ایمیل معتبر وارد کنید.",
            }),
          );
          return;
        }
        const emailTaken = Object.entries(db.users).some(
          ([uname, u]) =>
            uname !== currentUsername &&
            u.email &&
            u.email.toLowerCase() === newEmail.toLowerCase(),
        );
        if (emailTaken) {
          ws.send(
            JSON.stringify({
              type: "email-error",
              message: "این ایمیل قبلاً برای حساب دیگری استفاده شده است.",
            }),
          );
          return;
        }

        const isFirstTime = !db.users[currentUsername].email;
        db.users[currentUsername].email = newEmail;
        saveDb(db);

        ws.send(
          JSON.stringify({
            type: "email-updated",
            email: newEmail,
          }),
        );
        if (isFirstTime) sendWelcomeEmail(newEmail, currentUsername);
        break;
      }
      case "forgot-password": {
        const email = (msg.email || "").trim();
        if (!isValidEmail(email)) {
          ws.send(
            JSON.stringify({
              type: "reset-error",
              message: "لطفاً یک ایمیل معتبر وارد کنید.",
            }),
          );
          return;
        }

        const found = findUserByEmail(email);
        if (found) {
          const existing = resetCodes.get(found.username);
          if (
            existing &&
            Date.now() < existing.sentAt + RESET_CODE_RESEND_COOLDOWN_MS
          ) {
            ws.send(
              JSON.stringify({
                type: "reset-error",
                message:
                  "کد قبلاً ارسال شده. لطفاً کمی صبر کنید و دوباره تلاش کنید.",
              }),
            );
            return;
          }
          const code = generateResetCode();
          resetCodes.set(found.username, {
            code,
            expiresAt: Date.now() + RESET_CODE_TTL_MS,
            sentAt: Date.now(),
          });
          sendResetCodeEmail(email, found.username, code);
        }

        ws.send(
          JSON.stringify({
            type: "reset-code-sent",
            message: "اگر این ایمیل ثبت شده باشد، کد بازیابی برای آن ارسال شد.",
          }),
        );
        break;
      }

      case "reset-password": {
        const email = (msg.email || "").trim();
        const code = (msg.code || "").trim();
        const newPassword = msg.newPassword || "";

        if (!isValidEmail(email)) {
          ws.send(
            JSON.stringify({
              type: "reset-error",
              message: "لطفاً یک ایمیل معتبر وارد کنید.",
            }),
          );
          return;
        }
        if (!newPassword || newPassword.length < 4) {
          ws.send(
            JSON.stringify({
              type: "reset-error",
              message: "رمز عبور جدید باید حداقل ۴ کاراکتر باشد.",
            }),
          );
          return;
        }

        const found = findUserByEmail(email);
        const pending = found ? resetCodes.get(found.username) : null;

        if (
          !found ||
          !pending ||
          pending.code !== code ||
          Date.now() > pending.expiresAt
        ) {
          ws.send(
            JSON.stringify({
              type: "reset-error",
              message: "کد نامعتبر یا منقضی شده است.",
            }),
          );
          return;
        }

        db.users[found.username].password = newPassword;
        saveDb(db);
        resetCodes.delete(found.username);

        ws.send(
          JSON.stringify({
            type: "password-reset-success",
            message:
              "رمز عبور شما با موفقیت تغییر کرد. اکنون می‌توانید وارد شوید.",
          }),
        );
        break;
      }

      /* ------------------- Discord-like server events ------------------- */

      case "create-server": {
        if (!currentUsername) return;
        const serverId = "srv_" + Date.now();
        const members = Array.from(
          new Set([...(msg.members || []), currentUsername]),
        );
        const generalCategory = {
          id: "cat_" + Date.now(),
          name: "دسته‌بندی عمومی",
          position: 0,
        };
        const generalChannel = {
          id: "chn_" + (Date.now() + 1),
          name: "عمومی",
          type: "text",
          categoryId: generalCategory.id,
          position: 0,
          messages: [],
        };
        db.servers[serverId] = {
          id: serverId,
          name: msg.serverName || msg.groupName || "سرور جدید",
          icon: null,
          owner: currentUsername,
          members,
          memberRoles: {},
          roles: [makeEveryoneRole()],
          categories: [generalCategory],
          channels: [generalChannel],
        };
        saveDb(db);

        members.forEach((member) => {
          const clientWs = getWsByUsername(member);
          if (clientWs && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(
              JSON.stringify({
                type: "server-created",
                server: {
                  id: serverId,
                  name: db.servers[serverId].name,
                  icon: null,
                },
              }),
            );
          }
        });
        break;
      }

      case "fetch-server": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server || !server.members.includes(currentUsername)) return;
        ws.send(
          JSON.stringify({
            type: "server-data",
            server: buildServerSnapshot(server, currentUsername),
          }),
        );
        break;
      }

      case "create-category": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_CHANNELS")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        const name = (msg.name || "دسته‌بندی جدید").trim();
        const category = {
          id: "cat_" + Date.now() + Math.random().toString(36).slice(2, 6),
          name,
          position: server.categories.length,
        };
        server.categories.push(category);
        saveDb(db);
        broadcastToServerMembers(server, {
          type: "server-data",
          server: buildServerSnapshot(server, currentUsername),
        });
        server.members.forEach((m) => {
          if (m === currentUsername) return;
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "create-channel": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_CHANNELS")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        const name = (msg.name || "کانال-جدید").trim();
        const channel = {
          id: "chn_" + Date.now() + Math.random().toString(36).slice(2, 6),
          name,
          type: msg.channelType === "voice" ? "voice" : "text",
          categoryId: msg.categoryId || null,
          position: server.channels.length,
          messages: [],
        };
        server.channels.push(channel);
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "delete-channel": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_CHANNELS")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        server.channels = server.channels.filter((c) => c.id !== msg.channelId);
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "delete-category": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_CHANNELS")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        server.categories = server.categories.filter(
          (c) => c.id !== msg.categoryId,
        );
        server.channels.forEach((c) => {
          if (c.categoryId === msg.categoryId) c.categoryId = null;
        });
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "create-role": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_ROLES")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        const role = {
          id: "role_" + Date.now() + Math.random().toString(36).slice(2, 6),
          name: (msg.name || "نقش جدید").trim(),
          color: msg.color || "#7289da",
          permissions: Array.isArray(msg.permissions)
            ? msg.permissions.filter((p) => ALL_PERMISSIONS.includes(p))
            : [],
          position: server.roles.length,
        };
        server.roles.push(role);
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "edit-role": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_ROLES")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        const role = server.roles.find((r) => r.id === msg.roleId);
        if (!role || (role.isEveryone === true && msg.name)) {
          // allow permission edits on everyone role but not renaming/coloring
        }
        if (!role) return;
        if (!role.isEveryone) {
          if (msg.name) role.name = msg.name.trim();
          if (msg.color) role.color = msg.color;
        }
        if (Array.isArray(msg.permissions)) {
          role.permissions = msg.permissions.filter((p) =>
            ALL_PERMISSIONS.includes(p),
          );
        }
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "delete-role": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_ROLES")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        if (msg.roleId === "everyone") return;
        server.roles = server.roles.filter((r) => r.id !== msg.roleId);
        Object.keys(server.memberRoles || {}).forEach((m) => {
          server.memberRoles[m] = server.memberRoles[m].filter(
            (rid) => rid !== msg.roleId,
          );
        });
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "assign-role":
      case "unassign-role": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "MANAGE_ROLES")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        if (!server.members.includes(msg.member)) return;
        if (!server.memberRoles) server.memberRoles = {};
        if (!server.memberRoles[msg.member])
          server.memberRoles[msg.member] = [];
        const set = new Set(server.memberRoles[msg.member]);
        if (msg.type === "assign-role") set.add(msg.roleId);
        else set.delete(msg.roleId);
        server.memberRoles[msg.member] = Array.from(set);
        saveDb(db);
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "add-server-member": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        const canManage =
          server.owner === currentUsername ||
          memberHasPermission(server, currentUsername, "MANAGE_SERVER");
        if (!canManage) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب سرور یا مدیران می‌توانند عضو اضافه کنند.",
            }),
          );
          return;
        }
        const newMember = (msg.newMember || "").trim();
        if (!newMember || !db.users[newMember]) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "کاربری با این نام یافت نشد.",
            }),
          );
          return;
        }
        if (server.members.includes(newMember)) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "این کاربر قبلاً عضو است.",
            }),
          );
          return;
        }
        server.members.push(newMember);
        saveDb(db);

        const newMemberWs = getWsByUsername(newMember);
        if (newMemberWs && newMemberWs.readyState === WebSocket.OPEN) {
          newMemberWs.send(
            JSON.stringify({
              type: "server-created",
              server: { id: server.id, name: server.name, icon: server.icon },
            }),
          );
        }
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "kick-server-member": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (!memberHasPermission(server, currentUsername, "KICK_MEMBERS")) {
          ws.send(
            JSON.stringify({ type: "error", message: "دسترسی غیرمجاز." }),
          );
          return;
        }
        if (msg.member === server.owner) return;
        server.members = server.members.filter((m) => m !== msg.member);
        if (server.memberRoles) delete server.memberRoles[msg.member];
        saveDb(db);

        const kickedWs = getWsByUsername(msg.member);
        if (kickedWs && kickedWs.readyState === WebSocket.OPEN) {
          kickedWs.send(
            JSON.stringify({ type: "server-kicked", serverId: server.id }),
          );
        }
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      case "fetch-channel-history": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server || !server.members.includes(currentUsername)) return;
        const channel = findChannel(server, msg.channelId);
        if (!channel) return;
        const { page, hasMore } = paginateMessages(channel.messages, null);
        ws.send(
          JSON.stringify({
            type: "channel-history",
            serverId: server.id,
            channelId: channel.id,
            history: page,
            hasMore,
          }),
        );
        break;
      }

      case "fetch-more-channel-history": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server || !server.members.includes(currentUsername)) return;
        const channel = findChannel(server, msg.channelId);
        if (!channel) return;
        const { page, hasMore } = paginateMessages(
          channel.messages,
          msg.beforeMessageId,
        );
        ws.send(
          JSON.stringify({
            type: "more-channel-history",
            serverId: server.id,
            channelId: channel.id,
            history: page,
            hasMore,
          }),
        );
        break;
      }

      case "send-channel-message": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server || !server.members.includes(currentUsername)) return;
        if (!memberHasPermission(server, currentUsername, "SEND_MESSAGES")) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "شما اجازه ارسال پیام در این سرور را ندارید.",
            }),
          );
          return;
        }
        const channel = findChannel(server, msg.channelId);
        if (!channel || channel.type !== "text") return;

        let storedFileUrl = null;
        if (msg.fileData) {
          try {
            storedFileUrl = saveBase64File(msg.fileData, msg.fileName);
          } catch (err) {
            console.error("Failed to save uploaded file:", err);
          }
        }

        const messageObj = {
          id: "msg_" + Date.now() + Math.random(),
          clientId: msg.clientId || null,
          sender: currentUsername,
          channelId: channel.id,
          text: msg.text || "",
          fileData: storedFileUrl,
          fileName: msg.fileName || null,
          fileType: msg.fileType || null,
          isVoice: msg.isVoice || false,
          latitude: msg.latitude || null,
          longitude: msg.longitude || null,
          replyTo: msg.replyTo || null,
          replyToPreview: buildReplyPreview(msg.replyTo),
          timestamp: Date.now(),
        };
        channel.messages.push(messageObj);
        saveDb(db);

        broadcastToServerMembers(server, {
          type: "new-channel-message",
          serverId: server.id,
          channelId: channel.id,
          message: messageObj,
        });
        break;
      }

      case "edit-channel-message": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        const channel = findChannel(server, msg.channelId);
        if (!channel) return;
        const message = channel.messages.find((m) => m.id === msg.messageId);
        if (!message || message.sender !== currentUsername) return;
        message.text = msg.newText;
        message.edited = true;
        saveDb(db);
        broadcastToServerMembers(server, {
          type: "channel-message-edited",
          serverId: server.id,
          channelId: channel.id,
          messageId: message.id,
          newText: message.text,
        });
        break;
      }

      case "delete-channel-message": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        const channel = findChannel(server, msg.channelId);
        if (!channel) return;
        const message = channel.messages.find((m) => m.id === msg.messageId);
        if (!message) return;
        const canDelete =
          message.sender === currentUsername ||
          memberHasPermission(server, currentUsername, "MANAGE_MESSAGES");
        if (!canDelete) return;
        message.text = "این پیام حذف شده است";
        message.isDeletedForEveryone = true;
        message.fileData = null;
        message.fileName = null;
        saveDb(db);
        broadcastToServerMembers(server, {
          type: "channel-message-deleted",
          serverId: server.id,
          channelId: channel.id,
          messageId: message.id,
        });
        break;
      }

      case "leave-server": {
        if (!currentUsername) return;
        const server = db.servers[msg.serverId];
        if (!server) return;
        if (server.owner === currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "صاحب سرور نمی‌تواند سرور را ترک کند؛ آن را حذف کنید.",
            }),
          );
          return;
        }
        server.members = server.members.filter((m) => m !== currentUsername);
        if (server.memberRoles) delete server.memberRoles[currentUsername];
        saveDb(db);
        ws.send(JSON.stringify({ type: "server-kicked", serverId: server.id }));
        server.members.forEach((m) => {
          const cws = getWsByUsername(m);
          if (cws && cws.readyState === WebSocket.OPEN) {
            cws.send(
              JSON.stringify({
                type: "server-data",
                server: buildServerSnapshot(server, m),
              }),
            );
          }
        });
        break;
      }

      /* ---------------------------------------------------------------- */

      case "request-delete-group": {
        if (!currentUsername) return;
        const server = db.servers[msg.chatId];
        if (!server) return;
        if (server.owner !== currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب سرور می‌تواند آن را حذف کند.",
            }),
          );
          return;
        }
        const ownerUser = db.users[currentUsername];
        if (!ownerUser || !isValidEmail(ownerUser.email || "")) {
          ws.send(
            JSON.stringify({
              type: "group-delete-error",
              chatId: msg.chatId,
              message:
                "برای حذف سرور ابتدا باید یک ایمیل معتبر برای حساب خود ثبت کنید.",
            }),
          );
          return;
        }

        const existing = groupDeleteCodes.get(msg.chatId);
        if (
          existing &&
          Date.now() < existing.sentAt + RESET_CODE_RESEND_COOLDOWN_MS
        ) {
          ws.send(
            JSON.stringify({
              type: "group-delete-error",
              chatId: msg.chatId,
              message:
                "کد قبلاً ارسال شده. لطفاً کمی صبر کنید و دوباره تلاش کنید.",
            }),
          );
          return;
        }

        const code = generateResetCode();
        groupDeleteCodes.set(msg.chatId, {
          code,
          expiresAt: Date.now() + RESET_CODE_TTL_MS,
          sentAt: Date.now(),
          requestedBy: currentUsername,
        });
        sendGroupDeleteCodeEmail(
          ownerUser.email,
          currentUsername,
          server.name,
          code,
        );

        ws.send(
          JSON.stringify({
            type: "group-delete-code-sent",
            chatId: msg.chatId,
            message: "کد تایید حذف سرور به ایمیل شما ارسال شد.",
          }),
        );
        break;
      }

      case "confirm-delete-group": {
        if (!currentUsername) return;
        const server = db.servers[msg.chatId];
        if (!server) return;
        if (server.owner !== currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب سرور می‌تواند آن را حذف کند.",
            }),
          );
          return;
        }

        const pending = groupDeleteCodes.get(msg.chatId);
        const code = (msg.code || "").trim();
        if (
          !pending ||
          pending.code !== code ||
          Date.now() > pending.expiresAt
        ) {
          ws.send(
            JSON.stringify({
              type: "group-delete-error",
              chatId: msg.chatId,
              message: "کد نامعتبر یا منقضی شده است.",
            }),
          );
          return;
        }

        groupDeleteCodes.delete(msg.chatId);
        const members = server.members.slice();
        const serverName = server.name;
        delete db.servers[msg.chatId];
        saveDb(db);

        members.forEach((member) => {
          const clientWs = getWsByUsername(member);
          if (clientWs && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(
              JSON.stringify({
                type: "group-deleted",
                chatId: msg.chatId,
                chatName: serverName,
              }),
            );
          }
        });
        break;
      }

      case "add-contact": {
        if (!currentUsername || !db.users[currentUsername]) return;
        const target = msg.contactUsername.trim();

        if (target === currentUsername) {
          ws.send(
            JSON.stringify({
              type: "contact-error",
              message: "نمی‌توانید خودتان را به عنوان مخاطب اضافه کنید.",
            }),
          );
          return;
        }
        if (!db.users[target]) {
          ws.send(
            JSON.stringify({
              type: "contact-error",
              message: "کاربری با این نام یافت نشد.",
            }),
          );
          return;
        }

        const userContacts = db.users[currentUsername].contacts;
        if (!userContacts.includes(target)) {
          userContacts.push(target);
          saveDb(db);
        }

        ws.send(
          JSON.stringify({
            type: "contacts-list",
            contacts: enrichContacts(currentUsername, userContacts),
          }),
        );
        break;
      }

      case "fetch-chat-history": {
        if (!currentUsername) return;
        const target = msg.targetUser;
        if (!target) return;

        if (
          !(target.startsWith("group_") || target.startsWith("announcement_"))
        ) {
          const toUpgrade = db.messages.filter(
            (m) =>
              m.sender === target &&
              m.recipient === currentUsername &&
              m.status === "sent",
          );
          if (toUpgrade.length > 0) {
            toUpgrade.forEach((m) => (m.status = "delivered"));
            saveDb(db);
            const senderWs = getWsByUsername(target);
            if (senderWs && senderWs.readyState === WebSocket.OPEN) {
              senderWs.send(
                JSON.stringify({
                  type: "message-status",
                  status: "delivered",
                  messageIds: toUpgrade.map((m) => m.id),
                }),
              );
            }
          }
        }
        const fullList = getFullMessageListFor(currentUsername, target);
        const { page, hasMore } = paginateMessages(fullList, null);

        ws.send(
          JSON.stringify({
            type: "chat-history",
            targetUser: target,
            history: page,
            hasMore,
          }),
        );
        break;
      }
      case "fetch-more-history": {
        if (!currentUsername) return;
        const target = msg.targetUser;
        const beforeMessageId = msg.beforeMessageId;
        if (!target || !beforeMessageId) return;

        const fullList = getFullMessageListFor(currentUsername, target);
        const { page, hasMore } = paginateMessages(fullList, beforeMessageId);

        ws.send(
          JSON.stringify({
            type: "more-chat-history",
            targetUser: target,
            history: page,
            hasMore,
          }),
        );
        break;
      }
      case "mark-read": {
        if (!currentUsername) return;
        const target = msg.targetUser;
        if (!target) return;

        if (target.startsWith("group_") || target.startsWith("announcement_")) {
          return;
        }

        let changed = false;
        db.messages.forEach((m) => {
          if (
            m.sender === target &&
            m.recipient === currentUsername &&
            m.status !== "read"
          ) {
            m.status = "read";
            changed = true;
          }
        });

        if (changed) {
          saveDb(db);
          const senderWs = getWsByUsername(target);
          if (senderWs && senderWs.readyState === WebSocket.OPEN) {
            senderWs.send(
              JSON.stringify({
                type: "messages-read",
                reader: currentUsername,
              }),
            );
          }
        }
        break;
      }

      case "edit-message": {
        if (!currentUsername) return;
        const { messageId, newText, targetUser } = msg;

        if (
          targetUser.startsWith("group_") ||
          targetUser.startsWith("announcement_")
        ) {
          const chat = db.chats[targetUser];
          if (!chat) return;
          const message = chat.messages.find((m) => m.id === messageId);
          if (
            !message ||
            message.sender !== currentUsername ||
            message.isSystem
          )
            return;
          message.text = newText;
          message.edited = true;
          saveDb(db);
          chat.members.forEach((member) => {
            const clientWs = getWsByUsername(member);
            if (clientWs && clientWs.readyState === WebSocket.OPEN) {
              clientWs.send(
                JSON.stringify({ type: "message-edited", messageId, newText }),
              );
            }
          });
        } else {
          const message = db.messages.find((m) => m.id === messageId);
          if (
            !message ||
            message.sender !== currentUsername ||
            message.isSystem
          )
            return;
          message.text = newText;
          message.edited = true;
          saveDb(db);
          [message.sender, message.recipient].forEach((uname) => {
            const clientWs = getWsByUsername(uname);
            if (clientWs && clientWs.readyState === WebSocket.OPEN) {
              clientWs.send(
                JSON.stringify({ type: "message-edited", messageId, newText }),
              );
            }
          });
        }
        break;
      }

      case "delete-message": {
        if (!currentUsername) return;
        const { messageId, targetUser, scope } = msg;

        if (
          targetUser.startsWith("group_") ||
          targetUser.startsWith("announcement_")
        ) {
          const chat = db.chats[targetUser];
          if (chat) {
            const message = chat.messages.find((m) => m.id === messageId);
            if (message && !message.isSystem) {
              if (scope === "everyone" && message.sender === currentUsername) {
                message.isDeletedForEveryone = true;
                message.text = "این پیام حذف شده است";
                message.fileData = null;
                message.fileName = null;
                chat.members.forEach((member) => {
                  const clientWs = getWsByUsername(member);
                  if (clientWs && clientWs.readyState === WebSocket.OPEN) {
                    clientWs.send(
                      JSON.stringify({
                        type: "message-deleted",
                        messageId,
                        targetUser,
                      }),
                    );
                  }
                });
              } else if (scope === "me") {
                if (!message.deletedFor) message.deletedFor = [];
                message.deletedFor.push(currentUsername);
                ws.send(
                  JSON.stringify({
                    type: "message-deleted",
                    messageId,
                    targetUser,
                  }),
                );
              }
              saveDb(db);
            }
          }
        } else {
          const message = db.messages.find((m) => m.id === messageId);
          if (message && !message.isSystem) {
            if (scope === "everyone" && message.sender === currentUsername) {
              message.isDeletedForEveryone = true;
              message.text = "این پیام حذف شده است";
              message.fileData = null;
              message.fileName = null;
              saveDb(db);
              [message.sender, message.recipient].forEach((uname) => {
                const clientWs = getWsByUsername(uname);
                if (clientWs && clientWs.readyState === WebSocket.OPEN) {
                  clientWs.send(
                    JSON.stringify({
                      type: "message-deleted",
                      messageId,
                      targetUser,
                    }),
                  );
                }
              });
            } else if (scope === "me") {
              if (!message.deletedFor) message.deletedFor = [];
              message.deletedFor.push(currentUsername);
              saveDb(db);
              ws.send(
                JSON.stringify({
                  type: "message-deleted",
                  messageId,
                  targetUser,
                }),
              );
            }
          }
        }
        break;
      }

      case "missed-call": {
        if (!currentUsername) return;
        const recipient = msg.recipient;
        const messageObj = {
          id: "msg_" + Date.now() + Math.random(),
          sender: currentUsername,
          recipient: recipient,
          text: "📞 تماس از دست رفته",
          isMissedCall: true,
          isSystem: true,
          timestamp: Date.now(),
          deletedFor: [],
        };
        db.messages.push(messageObj);
        saveDb(db);

        for (let [clientWs, uname] of clients.entries()) {
          if (uname === recipient || uname === currentUsername) {
            clientWs.send(
              JSON.stringify({ type: "new-message", message: messageObj }),
            );
          }
        }
        break;
      }

      case "send-message": {
        if (!currentUsername) return;
        const recipient = msg.recipient;
        const isGroupTarget =
          recipient.startsWith("group_") ||
          recipient.startsWith("announcement_");

        let storedFileUrl = null;
        if (msg.fileData) {
          try {
            storedFileUrl = saveBase64File(msg.fileData, msg.fileName);
          } catch (err) {
            console.error("Failed to save uploaded file:", err);
          }
        }

        const messageObj = {
          id: "msg_" + Date.now() + Math.random(),
          clientId: msg.clientId || null,
          sender: currentUsername,
          recipient: recipient,
          text: msg.text || "",
          fileData: storedFileUrl,
          fileName: msg.fileName || null,
          fileType: msg.fileType || null,
          isVoice: msg.isVoice || false,
          latitude: msg.latitude || null,
          longitude: msg.longitude || null,
          replyTo: msg.replyTo || null,
          replyToPreview: buildReplyPreview(msg.replyTo),
          forwardedFrom: msg.forwardedFrom || null,
          timestamp: Date.now(),
          status: !isGroupTarget && isOnline(recipient) ? "delivered" : "sent",
          deletedFor: [],
        };

        if (isGroupTarget) {
          const chat = db.chats[recipient];
          if (chat) {
            chat.messages.push(messageObj);
            saveDb(db);
            chat.members.forEach((member) => {
              const clientWs = getWsByUsername(member);
              if (clientWs && clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(
                  JSON.stringify({
                    type: "new-message",
                    message: messageObj,
                    chatId: recipient,
                  }),
                );
              }
            });
          }
        } else {
          if (
            db.users[recipient] &&
            !db.users[recipient].contacts.includes(currentUsername)
          ) {
            db.users[recipient].contacts.push(currentUsername);
            saveDb(db);

            const recipientWs = getWsByUsername(recipient);
            if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
              recipientWs.send(
                JSON.stringify({
                  type: "contacts-list",
                  contacts: enrichContacts(
                    recipient,
                    db.users[recipient].contacts,
                  ),
                }),
              );
            }
          }

          db.messages.push(messageObj);
          saveDb(db);

          for (let [clientWs, uname] of clients.entries()) {
            if (uname === recipient || uname === currentUsername) {
              clientWs.send(
                JSON.stringify({ type: "new-message", message: messageObj }),
              );
            }
          }
        }
        break;
      }

      case "call-invite": {
        if (!currentUsername) return;
        const targetWs = getWsByUsername(msg.target);

        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          if (targetWs.room) {
            ws.send(
              JSON.stringify({
                type: "call-decline",
                from: msg.target,
                reason: "busy",
              }),
            );
            return;
          }
          targetWs.send(
            JSON.stringify({
              type: "call-invite",
              from: currentUsername,
              callType: msg.callType === "video" ? "video" : "audio",
            }),
          );
          break;
        }

        const targetUser = db.users[msg.target];
        const callType = msg.callType === "video" ? "video" : "audio";

        if (!firebaseEnabled || !targetUser || !targetUser.pushToken) {
          ws.send(
            JSON.stringify({
              type: "call-decline",
              from: msg.target,
              reason: "offline",
            }),
          );
          return;
        }

        const timeoutHandle = setTimeout(() => {
          const pending = pendingCalls.get(msg.target);
          if (pending && pending.from === currentUsername) {
            pendingCalls.delete(msg.target);
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(
                JSON.stringify({
                  type: "call-decline",
                  from: msg.target,
                  reason: "offline",
                }),
              );
            }
          }
        }, PENDING_CALL_TIMEOUT_MS);

        pendingCalls.set(msg.target, {
          from: currentUsername,
          callType,
          expiresAt: Date.now() + PENDING_CALL_TIMEOUT_MS,
          timeoutHandle,
        });

        sendCallPush(
          targetUser.pushToken,
          currentUsername,
          callType,
          msg.target,
        ).catch((err) => {
          console.error("FCM push failed for", msg.target, err.message || err);
        });
        break;
      }

      case "call-cancel": {
        if (!currentUsername) return;
        const targetWs = getWsByUsername(msg.target);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({ type: "call-cancel", from: currentUsername }),
          );
        }
        const pending = pendingCalls.get(msg.target);
        if (pending && pending.from === currentUsername) {
          pendingCalls.delete(msg.target);
          clearTimeout(pending.timeoutHandle);
          const targetUser = db.users[msg.target];
          if (firebaseEnabled && targetUser && targetUser.pushToken) {
            messaging
              .send({
                token: targetUser.pushToken,
                android: { priority: "high" },
                data: { type: "call-cancel", caller: currentUsername },
              })
              .catch((err) =>
                console.error("FCM cancel push failed:", err.message || err),
              );
          }
        }
        break;
      }

      case "call-decline": {
        if (!currentUsername) return;
        const targetWs = getWsByUsername(msg.target);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({ type: "call-decline", from: currentUsername }),
          );
        }
        break;
      }

      case "join": {
        let room = msg.room;
        if (!rooms.has(room)) rooms.set(room, new Set());
        let roomClients = rooms.get(room);

        if (roomClients.size >= 2) {
          ws.send(JSON.stringify({ type: "full" }));
          return;
        }

        ws.room = room;
        let participants = [];
        for (let client of roomClients) {
          if (client !== ws) participants.push({ id: client.id });
        }
        ws.id = Math.random().toString(36).substring(2, 9);

        ws.send(JSON.stringify({ type: "all-participants", participants }));
        roomClients.add(ws);

        for (let client of roomClients) {
          if (client !== ws) {
            client.send(
              JSON.stringify({ type: "participant-joined", id: ws.id }),
            );
          }
        }
        break;
      }

      case "offer":
      case "answer":
      case "ice-candidate": {
        let targetWs = null;
        if (ws.room && rooms.has(ws.room)) {
          for (let client of rooms.get(ws.room)) {
            if (client.id === msg.target) targetWs = client;
          }
        }
        if (targetWs) {
          targetWs.send(JSON.stringify({ ...msg, from: ws.id }));
        }
        break;
      }

      case "leave": {
        leaveRoom(ws);
        break;
      }
    }
  });

  ws.on("close", () => {
    const uname = clients.get(ws);
    clients.delete(ws);
    leaveRoom(ws);

    if (uname && db.users[uname]) {
      if (!isOnline(uname)) {
        const lastSeen = Date.now();
        db.users[uname].lastSeen = lastSeen;
        saveDb(db);
        broadcastPresence(uname, false, lastSeen);
      }
    }
  });
});

function getUserServers(username) {
  return Object.values(db.servers)
    .filter((s) => s.members.includes(username))
    .map((s) => ({ id: s.id, name: s.name, icon: s.icon || null }));
}

function leaveRoom(ws) {
  if (ws.room && rooms.has(ws.room)) {
    let roomClients = rooms.get(ws.room);
    roomClients.delete(ws);
    for (let client of roomClients) {
      client.send(JSON.stringify({ type: "participant-left", id: ws.id }));
    }
    if (roomClients.size === 0) rooms.delete(ws.room);
    ws.room = null;
  }
}

const PORT = process.env.PORT || 14156;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
