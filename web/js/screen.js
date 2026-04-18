import { screenCanvas, screenStageDemoHint, screenStageFullscreenBtn, screenStageMode, screenStageShell, screenStageSummary, screenStageToggleBtn } from "./dom.js";
import { getLang, t } from "./i18n.js";
import {
  classifyLane,
  createEmptyCounts,
  entityTypeForEvent,
  focusLabel,
  loadSpriteCatalog,
  MAX_BEAMS,
  MAX_RIPPLES,
  MAX_SPARKS,
  NODE_RADIUS,
  screenState,
  spriteKeyForEvent,
  summarizeState,
  toneForEvent,
} from "./screen_state.js";
import { addAmbientParticles, bindScreenCanvas, ensureCanvasSize, lanePosition, laneX, laneY, renderScreen, stepScreen } from "./screen_render.js";

let ctx = null;
let rafId = 0;

function hashValue(text) {
  const source = String(text || "");
  let hash = 0;
  for (let index = 0; index < source.length; index += 1) {
    hash = ((hash << 5) - hash + source.charCodeAt(index)) | 0;
  }
  return Math.abs(hash);
}

function laneRelayoutBounds(lane) {
  const order = ["generated", "activation", "listing", "negotiation", "success", "failure"];
  const laneIndex = Math.max(0, order.indexOf(lane));
  const current = laneX(order[laneIndex]);
  const prev = laneIndex > 0 ? laneX(order[laneIndex - 1]) : 72;
  const next = laneIndex < order.length - 1 ? laneX(order[laneIndex + 1]) : (screenCanvas?.clientWidth || 1280) - 72;
  const width = screenCanvas?.clientWidth || 1280;
  const height = screenCanvas?.clientHeight || 720;
  const left = Math.max(92, laneIndex === 0 ? 110 : (prev + current) / 2 - 18);
  const right = Math.min(width - 92, laneIndex === order.length - 1 ? width - 110 : (current + next) / 2 + 18);
  return {
    left,
    right,
    top: 112,
    bottom: height - 136,
  };
}

function relayoutLaneNodes(lane) {
  const laneNodes = screenState.nodes
    .filter((node) => node.lane === lane)
    .sort((a, b) => hashValue(a.entityKey || a.label) - hashValue(b.entityKey || b.label));
  if (!laneNodes.length) {
    return;
  }

  const bounds = laneRelayoutBounds(lane);
  const placed = [];
  laneNodes.forEach((node, orderIndex) => {
    const seed = hashValue(`${lane}:${node.entityKey || node.label}:${orderIndex}`);
    const minDistance = node.entityType === "property" ? 34 : 40;
    let bestCandidate = null;

    for (let attempt = 0; attempt < 40; attempt += 1) {
      const rx = Math.sin(seed * 0.00021 + (attempt + 1) * 17.137) * 43758.5453;
      const ry = Math.sin(seed * 0.00037 + (attempt + 1) * 29.417) * 24634.6345;
      const x = bounds.left + (rx - Math.floor(rx)) * Math.max(30, bounds.right - bounds.left);
      const y = bounds.top + (ry - Math.floor(ry)) * Math.max(30, bounds.bottom - bounds.top);
      const nearest = placed.reduce((min, item) => {
        const dx = x - item.x;
        const dy = y - item.y;
        return Math.min(min, Math.sqrt(dx * dx + dy * dy));
      }, Infinity);
      if (nearest >= minDistance) {
        bestCandidate = { x, y };
        break;
      }
      if (!bestCandidate || nearest > bestCandidate.score) {
        bestCandidate = { x, y, score: nearest };
      }
    }

    const x = bestCandidate?.x ?? laneX(lane);
    const y = bestCandidate?.y ?? ((bounds.top + bounds.bottom) / 2);
    node.anchorX = x;
    node.anchorY = y;
    node.x = x;
    node.y = y;
    placed.push({ x, y });
  });
}

function laneForProgressPhase(phase) {
  if (["month_start", "intervention", "macro", "bulletin", "finance", "rental"].includes(phase)) return "generated";
  if (["active_participants", "activation"].includes(phase)) return "activation";
  if (["listing_adjustment"].includes(phase)) return "listing";
  if (["matching"].includes(phase)) return "negotiation";
  if (["settlement", "summary", "month_end"].includes(phase)) return "success";
  return "generated";
}

function isStageEntityEvent(eventType, payload = {}) {
  if (payload.agent_id != null || payload.buyer_id != null || payload.property_id != null) {
    return true;
  }
  return ["AGENT_GENERATED", "PROPERTY_GENERATED", "AGENT_ACTIVATED", "PROPERTY_LISTED"].includes(eventType);
}

function nodeTtlForEvent(eventType) {
  if (eventType === "AGENT_GENERATED" || eventType === "PROPERTY_GENERATED") {
    return null;
  }
  if (eventType === "AGENT_ACTIVATED" || eventType === "PROPERTY_LISTED") {
    return null;
  }
  if (eventType === "MATCH_ATTEMPT" || eventType.startsWith("NEGOTIATION")) {
    return 30000;
  }
  if (eventType === "DEAL_SUCCESS" || eventType === "DEAL_FAIL") {
    return 45000;
  }
  return 12000;
}

function entityKeyForEvent(event) {
  const payload = event.payload || {};
  if (payload.agent_id != null) return `agent:${payload.agent_id}`;
  if (payload.buyer_id != null) return `agent:${payload.buyer_id}`;
  if (payload.property_id != null) return `property:${payload.property_id}`;
  return null;
}

function updateHud() {
  const isFullscreen = document.fullscreenElement === screenStageShell;
  if (screenStageMode) {
    const playback = screenState.running ? (getLang() === "en" ? "live" : "实时") : (getLang() === "en" ? "paused" : "暂停");
    const fullscreen = isFullscreen ? (getLang() === "en" ? "full" : "全屏") : (getLang() === "en" ? "windowed" : "窗口");
    screenStageMode.textContent = `${playback} · ${fullscreen}`;
  }
  if (screenStageToggleBtn) {
    screenStageToggleBtn.textContent = screenState.running ? t("buttons.pause_motion") : t("buttons.resume_motion");
  }
  if (screenStageFullscreenBtn) {
    screenStageFullscreenBtn.textContent = isFullscreen ? (getLang() === "en" ? "Exit Fullscreen" : "退出全屏") : t("buttons.fullscreen");
  }
  if (screenStageDemoHint) {
    screenStageDemoHint.dataset.fullscreen = isFullscreen ? "true" : "false";
    screenStageDemoHint.dataset.running = screenState.running ? "true" : "false";
  }
  if (screenStageSummary) {
    screenStageSummary.textContent = [
      `${getLang() === "en" ? "Agents" : "生成"} ${screenState.counts.generatedAgents}`,
      `${getLang() === "en" ? "Listings" : "挂牌"} ${screenState.counts.listings}`,
      `${getLang() === "en" ? "Matches" : "撮合"} ${screenState.counts.matches}`,
      `${getLang() === "en" ? "Deals" : "成交"} ${screenState.counts.successes}`,
      `${getLang() === "en" ? "Breaks" : "破裂"} ${screenState.counts.failures}`,
      `${getLang() === "en" ? "Focus" : "焦点"} ${focusLabel()}`,
    ].join(" · ");
  }
}

function setFocus(lane, strength = 1) {
  screenState.focusLane = lane;
  screenState.focusStrength = Math.max(screenState.focusStrength, strength);
  screenState.cameraTargetX = laneX(lane) - (screenCanvas?.clientWidth || 1280) * 0.5;
}

function addNode(event, laneOverride = null, labelOverride = null, toneOverride = null, spriteOverride = null) {
  const eventType = event.event_type || "UNKNOWN";
  const lane = laneOverride || classifyLane(eventType);
  const payload = event.payload || {};
  const entityKey = entityKeyForEvent(event);
  const totalInLane = screenState.nodes.filter((node) => node.lane === lane).length + 1;
  const tone = toneOverride || toneForEvent(eventType, payload);
  const spriteKey = spriteOverride || spriteKeyForEvent(eventType, payload);
  const entityType = entityTypeForEvent(eventType, payload);
  const label = String(
    labelOverride
      || payload.name
      || (entityType === "property" && payload.property_id != null ? `房#${payload.property_id}` : null)
      || payload.property_label
      || payload.display_name
      || payload.property_id
      || payload.agent_id
      || eventType
  );
  const existing = entityKey ? screenState.nodes.find((node) => node.entityKey === entityKey) : null;
  if (existing) {
    existing.lane = lane;
    existing.eventType = eventType;
    existing.label = label;
    existing.tone = tone;
    existing.spriteKey = spriteKey;
    existing.entityType = entityType;
    existing.energy = 1;
    existing.ttl = nodeTtlForEvent(eventType);
    relayoutLaneNodes(lane);
    screenState.stageLabel = eventType.toLowerCase();
    setFocus(lane, eventType.startsWith("NEGOTIATION") ? 1 : 0.72);
    return;
  }
  const slot = lanePosition(lane, Math.max(0, totalInLane - 1), Math.max(totalInLane, 3), entityKey, entityType);
  screenState.nodes = [
    ...screenState.nodes.slice(-95),
    {
      id: `${eventType}-${event.event_id || Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      entityKey,
      lane,
      eventType,
      label,
      tone,
      spriteKey,
      entityType,
      anchorX: slot.x,
      anchorY: slot.y,
      x: slot.x,
      y: slot.y,
      radius: NODE_RADIUS,
      energy: 1,
      ttl: nodeTtlForEvent(eventType),
      driftSeed: Math.random() * Math.PI * 2,
    },
  ];
  relayoutLaneNodes(lane);
  screenState.stageLabel = eventType.toLowerCase();
  setFocus(lane, eventType.startsWith("NEGOTIATION") ? 1 : 0.72);
}

function addSnapshotNode(snapshotNode) {
  if (!snapshotNode) {
    return;
  }
  const lane = String(snapshotNode.lane || "generated");
  const totalInLane = screenState.nodes.filter((node) => node.lane === lane).length + 1;
  const entityType = String(snapshotNode.entity_type || "agent");
  const label = String(
    snapshotNode.label
      || (entityType === "property" && snapshotNode.property_id != null ? `房#${snapshotNode.property_id}` : null)
      || snapshotNode.entity_key
      || lane
  );
  const slot = lanePosition(
    lane,
    Math.max(0, totalInLane - 1),
    Math.max(totalInLane, 3),
    snapshotNode.entity_key || label,
    entityType,
  );
  screenState.nodes = [
    ...screenState.nodes.slice(-255),
    {
      id: `${snapshotNode.entity_key || entityType}-${Math.random().toString(16).slice(2, 8)}`,
      entityKey: snapshotNode.entity_key || null,
      lane,
      eventType: String(snapshotNode.stage_status || lane),
      label,
      tone: String(snapshotNode.tone || "#d5dce7"),
      spriteKey: null,
      entityType,
      anchorX: slot.x,
      anchorY: slot.y,
      x: slot.x,
      y: slot.y,
      radius: NODE_RADIUS,
      energy: 0.66,
      ttl: null,
      driftSeed: Math.random() * Math.PI * 2,
    },
  ];
  relayoutLaneNodes(lane);
}

function moveEntity(event, entityKey, lane, options = {}) {
  if (!entityKey) {
    return;
  }
  const existing = screenState.nodes.find((node) => node.entityKey === entityKey);
  if (!existing) {
    return;
  }
  const totalInLane = screenState.nodes.filter((node) => node.lane === lane).length;
  existing.lane = lane;
  existing.eventType = options.eventType || existing.eventType;
  if (options.label) {
    existing.label = options.label;
  }
  if (options.tone) {
    existing.tone = options.tone;
  }
  if (options.spriteKey) {
    existing.spriteKey = options.spriteKey;
  }
  existing.energy = 1;
  existing.ttl = null;
  relayoutLaneNodes(lane);
}

function addBeam(fromLane, toLane, tone) {
  screenState.beams = [
    ...screenState.beams.slice(-(MAX_BEAMS - 1)),
    {
      id: `beam-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      fromX: laneX(fromLane),
      toX: laneX(toLane),
      y: laneY(Math.floor(Math.random() * 4), 5),
      tone,
      progress: 0,
      arc: (Math.random() - 0.5) * 28,
    },
  ];
}

function addSpark(lane, tone) {
  screenState.sparks = [
    ...screenState.sparks.slice(-(MAX_SPARKS - 1)),
    {
      id: `spark-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      x: laneX(lane),
      y: laneY(Math.floor(Math.random() * 4), 5),
      tone,
      ttl: 950,
    },
  ];
}

function addRipple(lane, tone, intensity = 1) {
  screenState.ripples = [
    ...screenState.ripples.slice(-(MAX_RIPPLES - 1)),
    {
      id: `ripple-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      x: laneX(lane),
      y: (screenCanvas?.clientHeight || 720) * 0.5,
      tone,
      radius: 36,
      ttl: 860 + intensity * 160,
    },
  ];
}

function bumpCounts(eventType) {
  if (eventType === "AGENT_GENERATED") screenState.counts.generatedAgents += 1;
  if (eventType === "PROPERTY_GENERATED") screenState.counts.generatedProperties += 1;
  if (eventType === "AGENT_ACTIVATED") screenState.counts.activations += 1;
  if (eventType === "PROPERTY_LISTED") screenState.counts.listings += 1;
  if (eventType === "MATCH_ATTEMPT") screenState.counts.matches += 1;
  if (eventType.startsWith("NEGOTIATION")) screenState.counts.negotiations += 1;
  if (eventType === "DEAL_SUCCESS") screenState.counts.successes += 1;
  if (eventType === "DEAL_FAIL") screenState.counts.failures += 1;
}

function frame(ts) {
  if (!screenState.running) {
    renderScreen();
    rafId = window.requestAnimationFrame(frame);
    return;
  }
  const delta = screenState.lastTs ? Math.min(48, ts - screenState.lastTs) : 16;
  screenState.lastTs = ts;
  stepScreen(delta);
  renderScreen();
  rafId = window.requestAnimationFrame(frame);
}

export function initScreenStage() {
  if (!screenCanvas) {
    return;
  }
  ctx = screenCanvas.getContext("2d");
  bindScreenCanvas(screenCanvas, ctx);
  loadSpriteCatalog();
  ensureCanvasSize();
  addAmbientParticles();
  updateHud();
  window.render_game_to_text = summarizeState;
  window.advanceTime = (ms) => {
    const steps = Math.max(1, Math.round(Number(ms || 0) / (1000 / 60)));
    for (let index = 0; index < steps; index += 1) {
      stepScreen(1000 / 60);
    }
    renderScreen();
  };
  window.addEventListener("resize", ensureCanvasSize);
  document.addEventListener("fullscreenchange", () => {
    ensureCanvasSize();
    updateHud();
    renderScreen();
  });
  window.addEventListener("keydown", (event) => {
    if (event.code === "Space") {
      const tagName = document.activeElement?.tagName || "";
      if (tagName !== "INPUT" && tagName !== "TEXTAREA" && tagName !== "SELECT") {
        event.preventDefault();
        toggleScreenStageMotion();
      }
      return;
    }
    if (event.key.toLowerCase() === "f") {
      const tagName = document.activeElement?.tagName || "";
      if (tagName !== "INPUT" && tagName !== "TEXTAREA" && tagName !== "SELECT") {
        event.preventDefault();
        toggleScreenStageFullscreen();
      }
    }
  });
  if (!rafId) {
    rafId = window.requestAnimationFrame(frame);
  }
}

export function resetScreenStage() {
  screenState.nodes = [];
  screenState.beams = [];
  screenState.sparks = [];
  screenState.ripples = [];
  screenState.lastTs = 0;
  screenState.stageLabel = "idle";
  screenState.focusLane = "generated";
  screenState.focusStrength = 0.24;
  screenState.cameraX = 0;
  screenState.cameraTargetX = 0;
  screenState.cameraShake = 0;
  screenState.counts = createEmptyCounts();
  addAmbientParticles();
  updateHud();
  renderScreen();
}

export function hydrateScreenStage(status) {
  resetScreenStage();
  const stageSnapshot = status?.stage_snapshot;
  if (Array.isArray(stageSnapshot?.nodes) && stageSnapshot.nodes.length > 0) {
    for (const node of stageSnapshot.nodes) {
      addSnapshotNode(node);
    }
    if (stageSnapshot?.counts && typeof stageSnapshot.counts === "object") {
      screenState.counts = {
        ...createEmptyCounts(),
        ...stageSnapshot.counts,
      };
    }
    if (stageSnapshot?.focus_lane) {
      screenState.focusLane = String(stageSnapshot.focus_lane);
      screenState.cameraTargetX = laneX(screenState.focusLane) - (screenCanvas?.clientWidth || 1280) * 0.5;
    }
    screenState.stageLabel = String(status?.status || screenState.stageLabel || "idle");
    updateHud();
    renderScreen();
    return;
  }
  const replayEvents = Array.isArray(status?.stage_replay_events) ? status.stage_replay_events : [];
  for (const event of replayEvents) {
    ingestScreenEvent(event);
  }
  screenState.stageLabel = String(status?.status || screenState.stageLabel || "idle");
  updateHud();
  renderScreen();
}

export function toggleScreenStageMotion() {
  screenState.running = !screenState.running;
  updateHud();
}

export async function toggleScreenStageFullscreen() {
  if (!screenStageShell) {
    return;
  }
  if (document.fullscreenElement === screenStageShell) {
    await document.exitFullscreen();
  } else {
    await screenStageShell.requestFullscreen();
  }
  ensureCanvasSize();
  updateHud();
  renderScreen();
}

export function ingestScreenEvent(event) {
  if (!event || !event.event_type) {
    return;
  }
  const eventType = event.event_type;
  const lane = classifyLane(eventType);
  const tone = toneForEvent(eventType, event.payload || {});

  if (eventType === "STATUS_SNAPSHOT") {
    screenState.stageLabel = String(event.payload?.status?.status || "status_snapshot");
    updateHud();
    renderScreen();
    return;
  }

  if (eventType === "RUN_PROGRESS") {
    const phase = String(event.payload?.detail?.phase || event.payload?.stage || "system");
    const targetLane = laneForProgressPhase(phase);
    screenState.stageLabel = phase;
    setFocus(targetLane, 0.88);
    addRipple(targetLane, "#f4d47d", 0.9);
    updateHud();
    renderScreen();
    return;
  }

  if (eventType === "RUN_STARTED") {
    resetScreenStage();
    screenState.stageLabel = "run_started";
    setFocus("generated", 0.68);
    updateHud();
    renderScreen();
    return;
  }

  if (eventType === "MONTH_END") {
    setFocus("negotiation", 0.9);
    addSpark("negotiation", "#f4d47d");
    addRipple("negotiation", "#f4d47d", 1.2);
    screenState.stageLabel = "month_end";
    updateHud();
    renderScreen();
    return;
  }

  if (eventType === "RUN_FINISHED" || eventType === "RUN_FAILED") {
    const targetLane = eventType === "RUN_FINISHED" ? "success" : "failure";
    const targetTone = eventType === "RUN_FINISHED" ? "#89f2a6" : "#ff8d8d";
    setFocus(targetLane, 1);
    addSpark(targetLane, targetTone);
    addRipple(targetLane, targetTone, 1.4);
    screenState.cameraShake = 16;
    screenState.stageLabel = eventType.toLowerCase();
    updateHud();
    renderScreen();
    return;
  }

  if (!isStageEntityEvent(eventType, event.payload || {})) {
    setFocus(lane, 0.72);
    addRipple(lane, tone, 0.72);
    screenState.stageLabel = eventType.toLowerCase();
    updateHud();
    renderScreen();
    return;
  }

  addNode(event);
  bumpCounts(eventType);
  addRipple(lane, tone, eventType.startsWith("NEGOTIATION") ? 1.1 : 0.9);

  if (eventType === "MATCH_ATTEMPT") {
    moveEntity(event, entityKeyForEvent(event), "negotiation", {
      eventType,
      label: event.payload?.buyer_name || event.payload?.name || null,
      tone,
      spriteKey: spriteKeyForEvent(eventType, event.payload || {}),
    });
    if (event.payload?.property_id != null) {
      moveEntity(event, `property:${event.payload.property_id}`, "listing", {
        eventType: "PROPERTY_LISTED",
        label: event.payload?.property_label || event.payload?.display_name || null,
        tone: "#8dc2ff",
        spriteKey: "property",
      });
    }
    addBeam("listing", "negotiation", tone);
  } else if (eventType.startsWith("NEGOTIATION")) {
    moveEntity(event, entityKeyForEvent(event), "negotiation", {
      eventType,
      label: event.payload?.buyer_name || event.payload?.name || null,
      tone,
      spriteKey: spriteKeyForEvent(eventType, event.payload || {}),
    });
    if (event.payload?.property_id != null) {
      moveEntity(event, `property:${event.payload.property_id}`, "negotiation", {
        eventType,
        label: event.payload?.property_label || event.payload?.display_name || null,
        tone: "#8dc2ff",
        spriteKey: "property",
      });
    }
    addBeam("activation", "negotiation", tone);
  } else if (eventType === "DEAL_SUCCESS") {
    moveEntity(event, entityKeyForEvent(event), "success", {
      eventType,
      label: event.payload?.buyer_name || event.payload?.name || null,
      tone,
      spriteKey: spriteKeyForEvent(eventType, event.payload || {}),
    });
    if (event.payload?.property_id != null) {
      moveEntity(event, `property:${event.payload.property_id}`, "success", {
        eventType,
        label: event.payload?.property_label || event.payload?.display_name || null,
        tone: "#89f2a6",
        spriteKey: "property",
      });
    }
    addBeam("negotiation", "success", tone);
    screenState.cameraShake = 14;
    setFocus("success", 1);
  } else if (eventType === "DEAL_FAIL") {
    moveEntity(event, entityKeyForEvent(event), "failure", {
      eventType,
      label: event.payload?.buyer_name || event.payload?.name || null,
      tone,
      spriteKey: spriteKeyForEvent(eventType, event.payload || {}),
    });
    if (event.payload?.property_id != null) {
      moveEntity(event, `property:${event.payload.property_id}`, "failure", {
        eventType,
        label: event.payload?.property_label || event.payload?.display_name || null,
        tone: "#ff8d8d",
        spriteKey: "property",
      });
    }
    addBeam("negotiation", "failure", tone);
    screenState.cameraShake = 14;
    setFocus("failure", 1);
  } else if (lane === "generated") {
    addSpark("generated", tone);
  } else if (lane === "listing") {
    addSpark("listing", tone);
  } else {
    addSpark(lane, tone);
  }

  updateHud();
  renderScreen();
}

export function refreshScreenStageHud() {
  updateHud();
  renderScreen();
}
