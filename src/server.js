import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.REALTIME_PORT || process.env.PORT || 3010);
const CODE_TTL_MS = (() => {
  const raw = process.env.POCKET_CODE_TTL_MS;
  if (raw == null || raw === "") return null;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
})();
const CONTROLLER_GRACE_MS = Number(process.env.POCKET_CONTROLLER_GRACE_MS || 30 * 1000);

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server, path: "/ws" });

const sockets = new Map();
const presenceByUser = new Map();
const pocketByCode = new Map();
const pocketSessions = new Map();
const controllerTokens = new Map();
const challenges = new Map();
const friendRequests = new Map();
const friendshipsByUser = new Map();

function now() {
  return Date.now();
}

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function makeCode() {
  for (let i = 0; i < 10; i += 1) {
    const code = String(Math.floor(100000 + Math.random() * 900000));
    if (!pocketByCode.has(code)) return code;
  }
  return String(Math.floor(100000 + Math.random() * 900000));
}

function safeSend(ws, message) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  ws.send(JSON.stringify(message));
}

function broadcastToUsers(userIds, message) {
  for (const userId of userIds) {
    const socketIds = presenceByUser.get(userId);
    if (!socketIds) continue;
    for (const socketId of socketIds) safeSend(sockets.get(socketId)?.ws, message);
  }
}

function sendToUser(userId, message) {
  broadcastToUsers([String(userId || "")], message);
}

function sendError(ws, code, message) {
  safeSend(ws, { type: "error", code, message });
}

function registerPresence(socket, userId) {
  if (!userId) return;
  socket.userId = userId;
  socket.displayName = socket.displayName || "";
  const set = presenceByUser.get(userId) ?? new Set();
  set.add(socket.id);
  presenceByUser.set(userId, set);
}

function unregisterPresence(socket) {
  if (!socket.userId) return;
  const set = presenceByUser.get(socket.userId);
  if (!set) return;
  set.delete(socket.id);
  if (!set.size) presenceByUser.delete(socket.userId);
}

function getActiveUsers() {
  return Array.from(presenceByUser.keys()).map((userId) => {
    const socketId = Array.from(presenceByUser.get(userId) ?? [])[0];
    const socket = socketId ? sockets.get(socketId) : null;
    return {
      userId,
      displayName: socket?.displayName || "",
      online: true,
    };
  });
}

function getActiveUserIds() {
  return Array.from(presenceByUser.keys());
}

function cleanupExpiredCodes() {
  const t = now();
  for (const [code, entry] of pocketByCode.entries()) {
    if (entry.expiresAt != null && entry.expiresAt <= t) pocketByCode.delete(code);
  }
}

function cleanupDisconnectedControllers() {
  const t = now();
  for (const session of pocketSessions.values()) {
    for (const [slot, player] of session.players.entries()) {
      if (!player.connected && player.disconnectedAt && t - player.disconnectedAt > CONTROLLER_GRACE_MS) {
        session.players.delete(slot);
        if (player.controllerToken) controllerTokens.delete(player.controllerToken);
        safeSend(sockets.get(session.hostSocketId)?.ws, {
          type: "pocket_player_left",
          platformSessionId: session.platformSessionId,
          playerSlot: slot,
        });
      }
    }
  }
}

setInterval(() => {
  cleanupExpiredCodes();
  cleanupDisconnectedControllers();
}, 5000).unref();

function handleHostRegister(socket, msg) {
  const platformSessionId = String(msg.platformSessionId || id("platform"));
  const gameId = String(msg.gameId || "");
  const maxPlayers = Math.max(1, Math.min(Number(msg.maxPlayers || 1), 16));
  const requestedJoinCode = String(msg.joinCode || "").trim();
  const joinCode = (() => {
    if (/^\d{6}$/.test(requestedJoinCode)) {
      const existing = pocketByCode.get(requestedJoinCode);
      if (!existing || existing.platformSessionId === platformSessionId) return requestedJoinCode;
    }
    return makeCode();
  })();

  const previous = pocketSessions.get(platformSessionId);
  if (previous?.joinCode) pocketByCode.delete(previous.joinCode);

  const session = {
    platformSessionId,
    gameId,
    hostSocketId: socket.id,
    hostUserId: socket.userId || String(msg.hostUserId || ""),
    maxPlayers,
    joinCode,
    controllerHostSlot: null,
    controllerHostUserId: null,
    players: new Map(),
    createdAt: now(),
  };

  pocketSessions.set(platformSessionId, session);
  pocketByCode.set(joinCode, { platformSessionId, expiresAt: CODE_TTL_MS ? now() + CODE_TTL_MS : null });
  socket.platformSessionId = platformSessionId;

  safeSend(socket.ws, {
    type: "host_registered",
    platformSessionId,
    joinCode,
    expiresAt: CODE_TTL_MS ? new Date(now() + CODE_TTL_MS).toISOString() : null,
  });
}

function handleHostRenewCode(socket) {
  const session = pocketSessions.get(socket.platformSessionId);
  if (!session) return sendError(socket.ws, "session_not_found", "Register host before renewing code.");
  if (session.joinCode) pocketByCode.delete(session.joinCode);
  const joinCode = makeCode();
  session.joinCode = joinCode;
  pocketByCode.set(joinCode, { platformSessionId: session.platformSessionId, expiresAt: CODE_TTL_MS ? now() + CODE_TTL_MS : null });
  safeSend(socket.ws, {
    type: "host_code_renewed",
    platformSessionId: session.platformSessionId,
    joinCode,
    expiresAt: CODE_TTL_MS ? new Date(now() + CODE_TTL_MS).toISOString() : null,
  });
}

function handleGameReady(socket, msg) {
  const session = pocketSessions.get(socket.platformSessionId);
  if (!session) return sendError(socket.ws, "session_not_found", "Register host before game_ready.");
  const nextMaxPlayers = Math.max(1, Math.min(Number(msg.maxPlayers || session.maxPlayers), 16));
  session.maxPlayers = Math.max(session.maxPlayers || 1, nextMaxPlayers);
  session.controllerSchema = msg.controllerSchema || null;
  safeSend(socket.ws, { type: "game_ready_ack", platformSessionId: session.platformSessionId });
}

function handleControllerJoin(socket, msg) {
  cleanupExpiredCodes();
  const code = String(msg.joinCode || "").trim();
  const codeEntry = pocketByCode.get(code);
  if (!codeEntry) return sendError(socket.ws, "invalid_code", "Invalid or expired join code.");
  const session = pocketSessions.get(codeEntry.platformSessionId);
  if (!session) return sendError(socket.ws, "session_not_found", "Game session is not available.");

  let slot = null;
  for (let i = 1; i <= session.maxPlayers; i += 1) {
    if (!session.players.has(i)) {
      slot = i;
      break;
    }
  }
  if (!slot) {
    socket.waitingController = { platformSessionId: session.platformSessionId };
    return safeSend(socket.ws, {
      type: "controller_waiting",
      platformSessionId: session.platformSessionId,
      gameId: session.gameId,
      message: "Game Room is Full. Wait.",
    });
  }

  const controllerToken = id("controller");
  const joiningUserId = socket.userId || String(msg.userId || "");
  if (session.controllerHostSlot == null) session.controllerHostSlot = slot;
  if (session.controllerHostUserId == null && joiningUserId) session.controllerHostUserId = joiningUserId;
  session.players.set(slot, {
    socketId: socket.id,
    controllerToken,
    userId: joiningUserId,
    displayName: String(msg.displayName || ""),
    avatarPath: String(msg.avatarPath || ""),
    avatarUrl: String(msg.avatarUrl || ""),
    connected: true,
    joinedAt: now(),
  });
  controllerTokens.set(controllerToken, { platformSessionId: session.platformSessionId, slot, expiresAt: Infinity });
  socket.controller = { platformSessionId: session.platformSessionId, slot, controllerToken };

  safeSend(socket.ws, {
    type: "controller_joined",
    platformSessionId: session.platformSessionId,
    gameId: session.gameId,
    playerSlot: slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    controllerToken,
    schema: session.controllerSchema || null,
  });
  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_player_joined",
    platformSessionId: session.platformSessionId,
    playerSlot: slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    displayName: String(msg.displayName || ""),
    avatarPath: String(msg.avatarPath || ""),
    avatarUrl: String(msg.avatarUrl || ""),
    userId: joiningUserId,
  });
}

function handleControllerReconnect(socket, msg) {
  const controllerToken = String(msg.controllerToken || "").trim();
  const token = controllerTokens.get(controllerToken);
  if (!token) return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");
  const session = pocketSessions.get(token.platformSessionId);
  const player = session?.players.get(token.slot);
  if (!session || !player || player.controllerToken !== controllerToken) {
    controllerTokens.delete(controllerToken);
    return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");
  }

  player.socketId = socket.id;
  player.connected = true;
  player.disconnectedAt = null;
  socket.controller = { platformSessionId: session.platformSessionId, slot: token.slot, controllerToken };
  safeSend(socket.ws, {
    type: "controller_reconnected",
    platformSessionId: session.platformSessionId,
    gameId: session.gameId,
    playerSlot: token.slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    controllerToken,
    schema: session.controllerSchema || null,
  });
  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_player_reconnected",
    platformSessionId: session.platformSessionId,
    playerSlot: token.slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    displayName: String(player.displayName || ""),
    avatarPath: String(player.avatarPath || ""),
    avatarUrl: String(player.avatarUrl || ""),
    userId: String(player.userId || ""),
  });
}

function handleControllerInput(socket, msg) {
  const controller = socket.controller;
  if (!controller) return sendError(socket.ws, "not_joined", "Controller has not joined a session.");
  const session = pocketSessions.get(controller.platformSessionId);
  if (!session) return sendError(socket.ws, "session_not_found", "Game session is not available.");
  const player = session.players.get(controller.slot) || {};

  const input = msg.input || {};
  const wantsProfileUpdate = String(input?.control || "") === "profile_update";
  const displayNameOverride = String(msg.displayName || "").trim();
  const avatarPathOverride = String(msg.avatarPath || "").trim();
  const avatarUrlOverride = String(msg.avatarUrl || "").trim();
  const userIdOverride = String(msg.userId || "").trim();

  if (wantsProfileUpdate || displayNameOverride || avatarPathOverride || avatarUrlOverride) {
    if (displayNameOverride) player.displayName = displayNameOverride;
    if (avatarPathOverride) player.avatarPath = avatarPathOverride;
    if (avatarUrlOverride) player.avatarUrl = avatarUrlOverride;
    if (userIdOverride) player.userId = userIdOverride;
  }

  if (wantsProfileUpdate) {
    safeSend(sockets.get(session.hostSocketId)?.ws, {
      type: "pocket_player_updated",
      platformSessionId: session.platformSessionId,
      playerSlot: controller.slot,
      hostSlot: session.controllerHostSlot,
      hostUserId: session.controllerHostUserId,
      displayName: String(player.displayName || ""),
      avatarPath: String(player.avatarPath || ""),
      avatarUrl: String(player.avatarUrl || ""),
      userId: String(player.userId || ""),
    });
  }
  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_input",
    platformSessionId: session.platformSessionId,
    playerSlot: controller.slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    displayName: String(player.displayName || ""),
    avatarPath: String(player.avatarPath || ""),
    avatarUrl: String(player.avatarUrl || ""),
    userId: String(player.userId || ""),
    input,
    sequence: msg.sequence ?? null,
  });
}

function handleControllerUpdateProfile(socket, msg) {
  const controllerToken = String(msg.controllerToken || socket.controller?.controllerToken || "").trim();
  const token = controllerTokens.get(controllerToken);
  if (!token) return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");

  const session = pocketSessions.get(token.platformSessionId);
  const player = session?.players.get(token.slot);
  if (!session || !player || player.controllerToken !== controllerToken) {
    controllerTokens.delete(controllerToken);
    return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");
  }

  const displayName = String(msg.displayName || player.displayName || "");
  const avatarPath = String(msg.avatarPath || player.avatarPath || "");
  const avatarUrl = String(msg.avatarUrl || avatarPath || player.avatarUrl || "");
  const userId = String(msg.userId || player.userId || "");

  player.displayName = displayName;
  player.avatarPath = avatarPath;
  player.avatarUrl = avatarUrl;
  if (userId) player.userId = userId;

  safeSend(socket.ws, { type: "controller_profile_updated", playerSlot: token.slot });
  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_player_updated",
    platformSessionId: session.platformSessionId,
    playerSlot: token.slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    displayName,
    avatarPath,
    avatarUrl,
    userId,
  });
}

function handleControllerPlaySound(socket, msg) {
  const controllerToken = String(msg.controllerToken || socket.controller?.controllerToken || "").trim();
  const token = controllerTokens.get(controllerToken);
  if (!token) return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");

  const session = pocketSessions.get(token.platformSessionId);
  const player = session?.players.get(token.slot);
  if (!session || !player || player.controllerToken !== controllerToken) {
    controllerTokens.delete(controllerToken);
    return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");
  }

  const soundId = String(msg.soundId || "").trim();
  if (!soundId) return sendError(socket.ws, "invalid_sound", "soundId is required.");
  const soundUrl = String(msg.soundUrl || "").trim();
  const soundTitle = String(msg.soundTitle || "").trim();
  const gifUrl = String(msg.gifUrl || "").trim();

  console.log("[pocket] controller_play_sound", {
    platformSessionId: session.platformSessionId,
    slot: token.slot,
    soundId,
  });

  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_sound",
    platformSessionId: session.platformSessionId,
    playerSlot: token.slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    soundId,
    soundUrl,
    soundTitle,
    gifUrl,
    displayName: String(player.displayName || ""),
    avatarPath: String(player.avatarPath || ""),
    avatarUrl: String(player.avatarUrl || ""),
    userId: String(player.userId || ""),
    ts: now(),
  });
}

function handleControllerSelectEmoji(socket, msg) {
  const controllerToken = String(msg.controllerToken || socket.controller?.controllerToken || "").trim();
  const token = controllerTokens.get(controllerToken);
  if (!token) return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");

  const session = pocketSessions.get(token.platformSessionId);
  const player = session?.players.get(token.slot);
  if (!session || !player || player.controllerToken !== controllerToken) {
    controllerTokens.delete(controllerToken);
    return sendError(socket.ws, "invalid_controller_token", "Controller session expired.");
  }

  const emojiId = String(msg.emojiId || "").trim();
  if (!emojiId) return sendError(socket.ws, "invalid_emoji", "emojiId is required.");
  const emoji = String(msg.emoji || "").trim();
  const gifUrl = String(msg.gifUrl || "").trim();

  console.log("[pocket] controller_select_emoji", {
    platformSessionId: session.platformSessionId,
    slot: token.slot,
    emojiId,
  });

  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_emoji",
    platformSessionId: session.platformSessionId,
    playerSlot: token.slot,
    hostSlot: session.controllerHostSlot,
    hostUserId: session.controllerHostUserId,
    emojiId,
    emoji,
    gifUrl,
    displayName: String(player.displayName || ""),
    avatarPath: String(player.avatarPath || ""),
    avatarUrl: String(player.avatarUrl || ""),
    userId: String(player.userId || ""),
    ts: now(),
  });
}

function handleControllerLaunchGame(socket, msg) {
  const controller = socket.controller;
  if (!controller) return sendError(socket.ws, "not_joined", "Controller has not joined a session.");
  const session = pocketSessions.get(controller.platformSessionId);
  if (!session) return sendError(socket.ws, "session_not_found", "Game session is not available.");
  safeSend(sockets.get(session.hostSocketId)?.ws, {
    type: "pocket_launch_game",
    platformSessionId: session.platformSessionId,
    playerSlot: controller.slot,
    gameId: String(msg.gameId || ""),
    slug: String(msg.slug || ""),
    title: String(msg.title || ""),
  });
}

function handleChallengeCreate(socket, msg) {
  const challengeId = id("challenge");
  const gameId = String(msg.gameId || "");
  const maxPlayers = Math.max(2, Math.min(Number(msg.maxPlayers || 2), 16));
  const invitedUsers = Array.isArray(msg.inviteUserIds) && msg.inviteUserIds.length
    ? msg.inviteUserIds.map(String)
    : getActiveUserIds().filter((userId) => userId !== socket.userId);

  const challenge = {
    challengeId,
    gameId,
    hostUserId: socket.userId || String(msg.hostUserId || ""),
    maxPlayers,
    players: new Map(),
    status: "inviting",
    createdAt: now(),
  };
  if (challenge.hostUserId) challenge.players.set(challenge.hostUserId, { ready: true, score: 0 });
  challenges.set(challengeId, challenge);

  broadcastToUsers(invitedUsers, {
    type: "challenge_invite",
    challengeId,
    gameId,
    hostUserId: challenge.hostUserId,
    maxPlayers,
  });
  safeSend(socket.ws, { type: "challenge_created", challengeId, invitedUsers });
}

function handleChallengeDecline(socket, msg) {
  const challengeId = String(msg.challengeId || "");
  const challenge = challenges.get(challengeId);
  if (!challenge) return sendError(socket.ws, "challenge_not_found", "Challenge not found.");
  const userId = socket.userId || String(msg.userId || "");
  if (challenge.hostUserId) {
    sendToUser(challenge.hostUserId, { type: "challenge_declined", challengeId, userId });
  }
  safeSend(socket.ws, { type: "challenge_decline_ack", challengeId });
}

function handleChallengeCancel(socket, msg) {
  const challengeId = String(msg.challengeId || "");
  const challenge = challenges.get(challengeId);
  if (!challenge) return sendError(socket.ws, "challenge_not_found", "Challenge not found.");
  const userId = socket.userId || String(msg.userId || "");
  if (challenge.hostUserId && challenge.hostUserId !== userId) {
    return sendError(socket.ws, "forbidden", "Only the challenge host can cancel.");
  }
  challenges.delete(challengeId);
  broadcastToUsers(Array.from(challenge.players.keys()), { type: "challenge_cancelled", challengeId });
  safeSend(socket.ws, { type: "challenge_cancel_ack", challengeId });
}

function handleChallengeAccept(socket, msg) {
  const challengeId = String(msg.challengeId || "");
  const challenge = challenges.get(challengeId);
  if (!challenge) return sendError(socket.ws, "challenge_not_found", "Challenge not found.");
  if (challenge.players.size >= challenge.maxPlayers) return sendError(socket.ws, "challenge_full", "Challenge is full.");

  const userId = socket.userId || String(msg.userId || "");
  if (!userId) return sendError(socket.ws, "missing_user", "User id is required.");
  challenge.players.set(userId, { ready: true, score: 0 });

  const players = Array.from(challenge.players.keys()).map((playerId, index) => ({ playerId, slot: index + 1 }));
  broadcastToUsers(players.map((p) => p.playerId), { type: "challenge_player_joined", challengeId, players });

  if (players.length >= challenge.maxPlayers) {
    challenge.status = "started";
    broadcastToUsers(players.map((p) => p.playerId), {
      type: "challenge_start",
      challengeId,
      gameId: challenge.gameId,
      players,
      seed: crypto.randomBytes(8).toString("hex"),
    });
  }
}

function handleChallengeState(socket, msg) {
  const challengeId = String(msg.challengeId || "");
  const challenge = challenges.get(challengeId);
  if (!challenge) return;
  const userId = socket.userId || String(msg.userId || "");
  const player = challenge.players.get(userId);
  if (!player) return;
  player.score = Number(msg.score ?? msg.rankScore ?? player.score ?? 0);
  player.state = msg.state || {};
  const leaderboard = Array.from(challenge.players.entries())
    .map(([playerId, data]) => ({ playerId, score: Number(data.score || 0) }))
    .sort((a, b) => b.score - a.score);
  broadcastToUsers(Array.from(challenge.players.keys()), { type: "challenge_leaderboard", challengeId, leaderboard });
  if (msg.type === "challenge_result") {
    player.result = msg.result || null;
    player.completed = !!msg.completed;
    const results = leaderboard.map((entry, index) => ({
      ...entry,
      placement: index + 1,
      result: challenge.players.get(entry.playerId)?.result ?? null,
    }));
    broadcastToUsers(Array.from(challenge.players.keys()), { type: "challenge_result", challengeId, results });
  }
}

function friendshipKey(a, b) {
  return [String(a || ""), String(b || "")].sort().join(":");
}

function addFriendship(a, b) {
  const userA = String(a || "");
  const userB = String(b || "");
  if (!userA || !userB || userA === userB) return;
  const setA = friendshipsByUser.get(userA) ?? new Set();
  const setB = friendshipsByUser.get(userB) ?? new Set();
  setA.add(userB);
  setB.add(userA);
  friendshipsByUser.set(userA, setA);
  friendshipsByUser.set(userB, setB);
}

function friendSummary(userId) {
  const socketId = Array.from(presenceByUser.get(userId) ?? [])[0];
  const socket = socketId ? sockets.get(socketId) : null;
  return {
    userId,
    displayName: socket?.displayName || "",
    online: presenceByUser.has(userId),
  };
}

function handleFriendRequestCreate(socket, msg) {
  const requesterId = socket.userId || String(msg.requesterId || "");
  const receiverId = String(msg.receiverId || "").trim();
  if (!requesterId) return sendError(socket.ws, "missing_user", "Requester user id is required.");
  if (!receiverId) return sendError(socket.ws, "missing_receiver", "Receiver user id is required.");
  if (requesterId === receiverId) return sendError(socket.ws, "invalid_receiver", "Cannot add yourself.");

  const requestId = id("friend_request");
  const request = {
    requestId,
    requesterId,
    receiverId,
    status: "pending",
    createdAt: now(),
  };
  friendRequests.set(requestId, request);
  sendToUser(receiverId, { type: "friend_request_received", request });
  safeSend(socket.ws, { type: "friend_request_created", request });
}

function handleFriendRequestAccept(socket, msg) {
  const requestId = String(msg.requestId || "").trim();
  const userId = socket.userId || String(msg.userId || "");
  const request = friendRequests.get(requestId);
  if (!request) return sendError(socket.ws, "friend_request_not_found", "Friend request not found.");
  if (request.receiverId !== userId) return sendError(socket.ws, "forbidden", "Only the receiver can accept.");
  request.status = "accepted";
  addFriendship(request.requesterId, request.receiverId);
  const payload = {
    type: "friend_request_accepted",
    request,
    friendshipKey: friendshipKey(request.requesterId, request.receiverId),
  };
  sendToUser(request.requesterId, payload);
  sendToUser(request.receiverId, payload);
}

function handleFriendRequestReject(socket, msg) {
  const requestId = String(msg.requestId || "").trim();
  const userId = socket.userId || String(msg.userId || "");
  const request = friendRequests.get(requestId);
  if (!request) return sendError(socket.ws, "friend_request_not_found", "Friend request not found.");
  if (request.receiverId !== userId) return sendError(socket.ws, "forbidden", "Only the receiver can reject.");
  request.status = "rejected";
  sendToUser(request.requesterId, { type: "friend_request_rejected", request });
  safeSend(socket.ws, { type: "friend_request_reject_ack", request });
}

function handleFriendsList(socket, msg) {
  const userId = socket.userId || String(msg.userId || "");
  if (!userId) return sendError(socket.ws, "missing_user", "User id is required.");
  const friendIds = Array.from(friendshipsByUser.get(userId) ?? []);
  safeSend(socket.ws, {
    type: "friends_list",
    userId,
    friends: friendIds.map(friendSummary),
  });
}

function handleMessage(socket, msg) {
  switch (msg.type) {
    case "client_register":
      socket.displayName = String(msg.displayName || "");
      registerPresence(socket, String(msg.userId || ""));
      safeSend(socket.ws, { type: "client_registered", userId: socket.userId || null });
      break;
    case "host_register":
      socket.displayName = String(msg.displayName || "");
      registerPresence(socket, String(msg.hostUserId || msg.userId || ""));
      handleHostRegister(socket, msg);
      break;
    case "host_renew_code":
      handleHostRenewCode(socket);
      break;
    case "game_ready":
      handleGameReady(socket, msg);
      break;
    case "controller_join":
      socket.displayName = String(msg.displayName || "");
      registerPresence(socket, String(msg.userId || ""));
      handleControllerJoin(socket, msg);
      break;
    case "controller_reconnect":
      handleControllerReconnect(socket, msg);
      break;
    case "controller_input":
      handleControllerInput(socket, msg);
      break;
    case "controller_update_profile":
      handleControllerUpdateProfile(socket, msg);
      break;
    case "controller_play_sound":
      handleControllerPlaySound(socket, msg);
      break;
    case "controller_select_emoji":
      handleControllerSelectEmoji(socket, msg);
      break;
    case "controller_launch_game":
      handleControllerLaunchGame(socket, msg);
      break;
    case "challenge_create":
      handleChallengeCreate(socket, msg);
      break;
    case "challenge_accept":
      handleChallengeAccept(socket, msg);
      break;
    case "challenge_decline":
      handleChallengeDecline(socket, msg);
      break;
    case "challenge_cancel":
      handleChallengeCancel(socket, msg);
      break;
    case "challenge_state":
    case "challenge_result":
      handleChallengeState(socket, msg);
      break;
    case "presence_active_users":
      safeSend(socket.ws, { type: "presence_active_users", users: getActiveUsers() });
      break;
    case "friend_request_create":
      handleFriendRequestCreate(socket, msg);
      break;
    case "friend_request_accept":
      handleFriendRequestAccept(socket, msg);
      break;
    case "friend_request_reject":
      handleFriendRequestReject(socket, msg);
      break;
    case "friends_list":
      handleFriendsList(socket, msg);
      break;
    default:
      sendError(socket.ws, "unknown_type", `Unknown message type: ${msg.type}`);
  }
}

wss.on("connection", (ws) => {
  const socket = { id: id("socket"), ws, connectedAt: now(), userId: "" };
  sockets.set(socket.id, socket);
  safeSend(ws, { type: "connected", socketId: socket.id });

  ws.on("message", (raw) => {
    let msg = null;
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return sendError(ws, "invalid_json", "Message must be JSON.");
    }
    if (!msg || typeof msg.type !== "string") return sendError(ws, "invalid_message", "Message type is required.");
    handleMessage(socket, msg);
  });

  ws.on("close", () => {
    unregisterPresence(socket);
    if (socket.platformSessionId) {
      const session = pocketSessions.get(socket.platformSessionId);
      if (session?.hostSocketId === socket.id) {
        pocketSessions.delete(socket.platformSessionId);
        if (session.joinCode) pocketByCode.delete(session.joinCode);
        for (const player of session.players.values()) safeSend(sockets.get(player.socketId)?.ws, { type: "host_disconnected" });
      }
    }
    if (socket.controller) {
      const session = pocketSessions.get(socket.controller.platformSessionId);
      const player = session?.players.get(socket.controller.slot);
      if (player) {
        player.connected = false;
        player.disconnectedAt = now();
        safeSend(sockets.get(session.hostSocketId)?.ws, {
          type: "pocket_player_left",
          platformSessionId: session.platformSessionId,
          playerSlot: socket.controller.slot,
        });
      }
    }
    sockets.delete(socket.id);
  });
});

server.listen(PORT, () => {
  console.log(`[realtime] listening on http://localhost:${PORT} ws://localhost:${PORT}/ws`);
});
