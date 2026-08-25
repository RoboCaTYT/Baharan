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

// ---------------------------------------------------------------------------
// Email (nodemailer) setup
// ---------------------------------------------------------------------------
// Configure via environment variables so no secrets live in source control:
//   SMTP_HOST, SMTP_PORT, SMTP_SECURE ("true"/"false"), SMTP_USER, SMTP_PASS
//   MAIL_FROM (optional, defaults to SMTP_USER)
// If SMTP_HOST/SMTP_USER/SMTP_PASS aren't set, email addresses are still
// required and validated at signup/set-email time — we just skip actually
// sending anything (logged to the console instead) so local/dev setups
// without SMTP credentials keep working.
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

// ---------------------------------------------------------------------------
// Password reset codes
// ---------------------------------------------------------------------------
// username -> { code, expiresAt }
// In-memory only (on purpose): a lost/expired code just means the user
// requests a new one, so there's no need to persist these across restarts.
const resetCodes = new Map();
const RESET_CODE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const RESET_CODE_RESEND_COOLDOWN_MS = 60 * 1000; // 1 minute between sends

// ---------------------------------------------------------------------------
// Group/channel deletion codes (owner-only, email-verified)
// ---------------------------------------------------------------------------
// chatId -> { code, expiresAt, sentAt, requestedBy }
const groupDeleteCodes = new Map();

function findUserByEmail(email) {
  const lower = email.trim().toLowerCase();
  const entry = Object.entries(db.users).find(
    ([, u]) => u.email && u.email.toLowerCase() === lower,
  );
  return entry ? { username: entry[0], user: entry[1] } : null;
}

function generateResetCode() {
  // 6-digit numeric code, zero-padded.
  return String(crypto.randomInt(0, 1000000)).padStart(6, "0");
}

async function sendResetCodeEmail(toEmail, username, code) {
  if (!mailEnabled) {
    // No SMTP configured — log so local/dev testing can still proceed.
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

// Sends the "confirm group deletion" code to the group owner's email. Reuses
// the same 6-digit code scheme as password reset, but scoped to a chatId
// instead of a username so multiple pending deletions can't collide.
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
      text: `سلام ${username}،\n\nکد تایید حذف گروه/کانال «${chatName}»: ${code}\n\nاین کد تا ۱۰ دقیقه دیگر معتبر است. اگر این درخواست را شما نداده‌اید، این ایمیل را نادیده بگیرید و گروه شما حذف نخواهد شد.\n\nبهاران`,
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

// username -> { from, callType, callerWs, expiresAt, timeoutHandle }
// Tracks a call we pushed to a phone whose WebSocket wasn't connected, so we
// can deliver it as a normal call-invite the moment that phone reconnects.
const pendingCalls = new Map();
const PENDING_CALL_TIMEOUT_MS = 30000;

// ---------------------------------------------------------------------------
// File storage helpers
// ---------------------------------------------------------------------------
// Previously, uploaded images/videos/voice notes/avatars were stored as raw
// base64 strings directly inside database.json and re-sent in full every
// time chat-history (or contacts / auth data) was pushed over the socket.
// That meant opening a chat with a handful of images meant shipping several
// MB of duplicated base64 text through the WebSocket on every single load.
// Instead, uploads are now decoded once and written to disk as real files
// under public/uploads, and only a small URL string is stored/sent from then
// on. The browser can cache real files far better than re-parsed data URLs.

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

// Look up any message (direct or in a group/announcement) by id — used to
// build reply previews so the client can show what a reply is quoting
// without needing to keep the whole history in memory client-side.
function findMessageById(id) {
  let found = db.messages.find((m) => m.id === id);
  if (found) return found;
  for (const chatId in db.chats) {
    found = db.chats[chatId].messages.find((m) => m.id === id);
    if (found) return found;
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

// Finds the most recent direct message between `currentUsername` and
// `otherUsername` (respecting per-user "delete for me"), in the same
// {text, timestamp, sender} shape getUserChats() already uses for groups.
// This is what lets the sidebar sort direct chats by recency correctly —
// without it, every direct contact looked equally "stale" to the client.
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

// Builds the list of group/announcement chats a user belongs to, in the
// shape the client's normalizeChatItem()/setInitialContactsAndChats()
// expects (id, name, type, lastMessage, unreadCount). Without this, chats
// only ever reached the client via the live "group-created" push sent at
// creation/add-member time — so they vanished from the sidebar on every
// reload/reconnect even though they were still sitting in database.json.
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
        // Per-member unread counts aren't tracked for groups/channels yet
        // (see the "mark-read" handler below), so this stays 0 for now.
        unreadCount: 0,
      };
    });
}

async function sendCallPush(token, fromUsername, callType, targetUsername) {
  if (!firebaseEnabled) return;
  // Data-only message (no "notification" field) so Android always hands it
  // to CallPushService's onMessageReceived — including while the app is
  // fully killed — instead of the OS auto-displaying a generic system
  // notification we don't control.
  await messaging.send({
    token,
    android: {
      priority: "high",
      ttl: PENDING_CALL_TIMEOUT_MS,
    },
    data: {
      type: "call-invite",
      // NOTE: "from" is a reserved key in the FCM data payload (it collides
      // with FCM's own message envelope) and silently gets rejected with
      // "Invalid data payload key: from" — so the caller's username is sent
      // as "caller" instead. Keep this key name in sync with what
      // CallPushService.java reads on the Android side.
      caller: fromUsername,
      callType: callType === "video" ? "video" : "audio",
      target: targetUsername,
    },
  });
}

// If `username` has a pending push-woken call waiting, deliver it now that
// their WebSocket has (re)connected, and clear the pending state either way.
function deliverPendingCallIfAny(ws, username) {
  const pending = pendingCalls.get(username);
  if (!pending) return;
  pendingCalls.delete(username);
  clearTimeout(pending.timeoutHandle);
  if (Date.now() > pending.expiresAt) return; // caller already gave up
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

// Marks every direct message addressed to `username` that's still sitting
// at status "sent" as "delivered" (their client/device now has it), and
// tells each sender so their outgoing bubble can flip from a single check
// to a double grey check. Called whenever a user's socket (re)connects —
// covers messages that arrived while they were completely offline.
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
    const initialData = { users: {}, messages: [], chats: {} };
    fs.writeFileSync(DB_FILE, JSON.stringify(initialData, null, 2));
    return initialData;
  }
  try {
    const data = fs.readFileSync(DB_FILE, "utf8");
    const parsed = JSON.parse(data);
    if (!parsed.chats) parsed.chats = {};
    return parsed;
  } catch (err) {
    console.error("Error reading database file, resetting:", err);
    return { users: {}, messages: [], chats: {} };
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

// How many messages a single chat-history / more-chat-history response
// returns. Keeping this small (instead of always sending the full
// conversation) is what keeps opening a long-running chat cheap on the
// server and the wire — older messages are only fetched on demand as the
// user scrolls up.
const MESSAGES_PAGE_SIZE = 10;

// Slices `fullList` (oldest -> newest) into a single page of at most
// MESSAGES_PAGE_SIZE messages ending just before `beforeMessageId` (or the
// very end of the list, if no beforeMessageId is given — i.e. "the most
// recent page"). Returns { page, hasMore } where hasMore tells the client
// whether an older page still exists to fetch.
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

// Builds the list of a user's direct contacts in the shape the client's
// normalizeContactItem() expects (username, profilePic, online, lastSeen,
// lastMessage). `currentUsername` is whichever user this list is being sent
// *to* — needed so we know both which side of each conversation to read and
// whose "delete for me" flags to respect when picking the last message.
//
// lastMessage previously wasn't included here at all, so on every
// login/sync-check/add-contact/etc. every direct contact came back with
// lastMessage: null — the client's sort-by-most-recent-message logic had
// nothing to sort direct chats by, so they landed in an effectively random
// order (while groups/channels, which do get lastMessage via getUserChats,
// sorted correctly). Computing it here fixes that.
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

// Builds the payload sent back to a client for the members modal: every
// member's username plus their rank — owner ("صاحب"), admin ("ادمین"), or
// plain member (no badge). A chat has exactly one owner (its creator) and
// any number of admins the owner has promoted.
function buildChatMembersPayload(chat) {
  return chat.members.map((m) => ({
    username: m,
    isOwner: chat.owner === m,
    isAdmin: chat.admins.includes(m),
  }));
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
            theme: "dark",
          }),
        );
        broadcastPresence(currentUsername, true, null);
        sendWelcomeEmail(signupEmail, currentUsername);
        break;
      }

      case "login": {
        // The "username" field doubles as a login identifier: it can be an
        // actual username, or (if it looks like an email / doesn't match
        // any username) the email address attached to an account.
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
            theme: user.theme || "dark",
          }),
        );
        broadcastPresence(currentUsername, true, null);
        // In case a call was pushed to this phone via FCM while it was
        // asleep and it's only reconnecting now, deliver it immediately.
        deliverPendingCallIfAny(ws, currentUsername);
        // Any direct messages that arrived while we were fully offline are
        // now on our device — flip them to "delivered" and let the senders
        // know so their ticks update.
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
            // Decode and persist to disk instead of storing the raw base64
            // string, so profile pics stop bloating every contacts-list /
            // auth-success / sync-data payload sent to everyone who has
            // this user as a contact.
            const url = saveBase64File(msg.profilePic, "avatar.png");
            db.users[currentUsername].profilePic =
              url || db.users[currentUsername].profilePic;
          } else {
            db.users[currentUsername].profilePic = msg.profilePic; // null or already a URL
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

      // Lets an already-logged-in user (who signed up before email was
      // required, or who never finished the "add your email" popup) attach
      // an email address to their account.
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

      // Step 1 of the "پسورد خود را فراموش کردم" flow: given an email,
      // email a 6-digit code that step 2 (reset-password) will check.
      // Responds identically whether or not the email is registered, so a
      // caller can't use this to probe which addresses have accounts.
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

      // Step 2: verify the emailed code and set a new password.
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

      case "create-group": {
        if (!currentUsername) return;
        const chatId = "group_" + Date.now();
        const members = Array.from(
          new Set([...(msg.members || []), currentUsername]),
        );
        db.chats[chatId] = {
          id: chatId,
          name: msg.groupName || "گروه جدید",
          type: "group",
          members,
          // The creator becomes the group's owner ("صاحب") — a distinct,
          // singular rank above "ادمین". Only the owner can promote/demote
          // admins or delete the group (see make-chat-admin,
          // remove-chat-admin, request-delete-group below).
          owner: currentUsername,
          admins: [],
          messages: [],
        };
        saveDb(db);

        members.forEach((member) => {
          const clientWs = getWsByUsername(member);
          if (clientWs && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(
              JSON.stringify({ type: "group-created", chat: db.chats[chatId] }),
            );
          }
        });
        break;
      }

      case "create-announcement": {
        if (!currentUsername) return;
        const chatId = "announcement_" + Date.now();
        const members = Array.from(
          new Set([...(msg.members || []), currentUsername]),
        );
        db.chats[chatId] = {
          id: chatId,
          name: msg.channelName || "کانال اطلاع‌رسانی",
          type: "announcement",
          members,
          owner: currentUsername,
          admins: [],
          messages: [],
        };
        saveDb(db);

        members.forEach((member) => {
          const clientWs = getWsByUsername(member);
          if (clientWs && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(
              JSON.stringify({ type: "group-created", chat: db.chats[chatId] }),
            );
          }
        });
        break;
      }

      // Returns the member list (with rank flags) for a group or
      // announcement so the client can render the members modal. Any
      // member of a group can view it; announcements stay owner/admin-only
      // since they've always worked that way.
      case "fetch-chat-members": {
        if (!currentUsername) return;
        const chat = db.chats[msg.chatId];
        if (!chat || !chat.members.includes(currentUsername)) return;
        const requesterIsAdmin = chat.admins.includes(currentUsername);
        const requesterIsOwner = chat.owner === currentUsername;
        if (
          chat.type === "announcement" &&
          !requesterIsAdmin &&
          !requesterIsOwner
        ) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "دسترسی غیرمجاز: فقط ادمین‌ها و صاحب گروه",
            }),
          );
          return;
        }
        ws.send(
          JSON.stringify({
            type: "chat-members",
            chatId: msg.chatId,
            members: buildChatMembersPayload(chat),
            isAdmin: requesterIsAdmin,
            isOwner: requesterIsOwner,
          }),
        );
        break;
      }

      // Owner or admin: add an existing user to a group/announcement after
      // it's already been created.
      case "add-chat-member": {
        if (!currentUsername) return;
        const chat = db.chats[msg.chatId];
        if (!chat) return;
        const canManage =
          chat.owner === currentUsername ||
          chat.admins.includes(currentUsername);
        if (!canManage) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب گروه یا ادمین‌ها می‌توانند عضو اضافه کنند.",
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
        if (chat.members.includes(newMember)) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "این کاربر قبلاً عضو است.",
            }),
          );
          return;
        }

        chat.members.push(newMember);
        saveDb(db);

        const newMemberWs = getWsByUsername(newMember);
        if (newMemberWs && newMemberWs.readyState === WebSocket.OPEN) {
          newMemberWs.send(JSON.stringify({ type: "group-created", chat }));
        }

        ws.send(
          JSON.stringify({
            type: "chat-members",
            chatId: msg.chatId,
            members: buildChatMembersPayload(chat),
            isAdmin: chat.admins.includes(currentUsername),
            isOwner: chat.owner === currentUsername,
          }),
        );
        break;
      }

      // Owner-only: promote an existing member of a group/announcement to
      // "ادمین". No one but the owner ("صاحب") can do this.
      case "make-chat-admin": {
        if (!currentUsername) return;
        const chat = db.chats[msg.chatId];
        if (!chat) return;
        if (chat.owner !== currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب گروه می‌تواند ادمین تعیین کند.",
            }),
          );
          return;
        }
        const targetMember = msg.member;
        if (!targetMember || !chat.members.includes(targetMember)) return;
        if (targetMember === chat.owner) return; // owner is already top rank
        if (!chat.admins.includes(targetMember)) {
          chat.admins.push(targetMember);
          saveDb(db);
        }

        ws.send(
          JSON.stringify({
            type: "chat-members",
            chatId: msg.chatId,
            members: buildChatMembersPayload(chat),
            isAdmin: chat.admins.includes(currentUsername),
            isOwner: true,
          }),
        );
        break;
      }

      // Owner-only: demote an admin back down to a plain member. Just like
      // promoting, no one but the owner can do this.
      case "remove-chat-admin": {
        if (!currentUsername) return;
        const chat = db.chats[msg.chatId];
        if (!chat) return;
        if (chat.owner !== currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب گروه می‌تواند ادمین را عزل کند.",
            }),
          );
          return;
        }
        const targetMember = msg.member;
        if (!targetMember) return;
        const idx = chat.admins.indexOf(targetMember);
        if (idx !== -1) {
          chat.admins.splice(idx, 1);
          saveDb(db);
        }

        ws.send(
          JSON.stringify({
            type: "chat-members",
            chatId: msg.chatId,
            members: buildChatMembersPayload(chat),
            isAdmin: chat.admins.includes(currentUsername),
            isOwner: true,
          }),
        );
        break;
      }

      // Step 1 of owner-only group deletion: the owner must have a verified
      // email on file, and we email a 6-digit confirmation code to it
      // before anything is actually deleted.
      case "request-delete-group": {
        if (!currentUsername) return;
        const chat = db.chats[msg.chatId];
        if (!chat) return;
        if (chat.owner !== currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب گروه می‌تواند آن را حذف کند.",
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
                "برای حذف گروه ابتدا باید یک ایمیل معتبر برای حساب خود ثبت کنید.",
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
          chat.name,
          code,
        );

        ws.send(
          JSON.stringify({
            type: "group-delete-code-sent",
            chatId: msg.chatId,
            message: "کد تایید حذف گروه به ایمیل شما ارسال شد.",
          }),
        );
        break;
      }

      // Step 2: verify the emailed code and, only then, permanently delete
      // the group/channel for every member.
      case "confirm-delete-group": {
        if (!currentUsername) return;
        const chat = db.chats[msg.chatId];
        if (!chat) return;
        if (chat.owner !== currentUsername) {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "فقط صاحب گروه می‌تواند آن را حذف کند.",
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
        const members = chat.members.slice();
        const chatName = chat.name;
        delete db.chats[msg.chatId];
        saveDb(db);

        members.forEach((member) => {
          const clientWs = getWsByUsername(member);
          if (clientWs && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(
              JSON.stringify({
                type: "group-deleted",
                chatId: msg.chatId,
                chatName,
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
          // Opening this chat means any of the other person's messages
          // that were still sitting at "sent" (they were offline when
          // sent) have now reached us — upgrade them to "delivered" and
          // let the sender know so their ticks update.
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

        // Only the most recent MESSAGES_PAGE_SIZE messages are sent up
        // front — older history is fetched on demand via
        // "fetch-more-history" as the user scrolls up, so opening a chat
        // with thousands of messages stays cheap.
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

      // Loads the next older page (MESSAGES_PAGE_SIZE messages) of a chat
      // already open on the client, for messages older than
      // `beforeMessageId`. Sent when the user scrolls near the top of the
      // message list.
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

      // Telegram-style read receipts. Sent by the client whenever it opens a
      // conversation (see openChat() in index.html). We flip the status to
      // "read" on every message the OTHER person sent to us in that
      // conversation, then let them know via "messages-read" so their
      // outgoing ticks turn blue.
      case "mark-read": {
        if (!currentUsername) return;
        const target = msg.targetUser;
        if (!target) return;

        if (target.startsWith("group_") || target.startsWith("announcement_")) {
          // Per-member read receipts for groups/channels aren't tracked in this
          // version yet — safely ignored for now.
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
          // System messages (e.g. missed-call notices) can never be edited.
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
          // System messages (e.g. missed-call notices) can never be edited.
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
            // System messages (e.g. missed-call notices) can never be
            // deleted — neither "for me" nor "for everyone".
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
          // System messages (e.g. missed-call notices) can never be
          // deleted — neither "for me" nor "for everyone".
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
          // System messages can't be edited or deleted by anyone.
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

        // Decode any incoming base64 attachment to a real file on disk and
        // store only its URL. This is what keeps chat-history payloads (and
        // every future re-render of this message) small — previously the
        // full base64 blob was stored and re-sent on every chat load.
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
          // Snapshot of the original message's sender + text, computed once
          // at send time, so the client can render "replying to X: ..."
          // without needing to keep the whole history around locally.
          replyToPreview: buildReplyPreview(msg.replyTo),
          forwardedFrom: msg.forwardedFrom || null,
          timestamp: Date.now(),
          // WhatsApp-style single/double-tick lifecycle for direct chats:
          // "sent" as soon as we've stored it, upgraded to "delivered" here
          // immediately if the recipient's socket is already open (their
          // device effectively has it right away), and later to "read" via
          // the "mark-read" handler above. Group/announcement chats don't
          // track per-member delivery/read state yet, so they just stay at
          // "sent".
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

        // Target's WebSocket isn't connected right now — screen off, Doze,
        // or the app process was killed. Try to wake their phone with a
        // push instead of immediately giving up on the call.
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

        // If we're mid-wake (pushed via FCM, target hasn't reconnected yet),
        // clear the pending call and tell their phone to stand down too, in
        // case it wakes up a moment later than this cancel.
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
