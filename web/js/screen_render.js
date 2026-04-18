import {
  LANE_META,
  MAX_AMBIENT_PARTICLES,
  SCREEN_HEIGHT,
  SCREEN_WIDTH,
  screenState,
} from "./screen_state.js";

let canvasRef = null;
let ctxRef = null;

function viewportWidth() {
  return canvasRef?.clientWidth || SCREEN_WIDTH;
}

function viewportHeight() {
  return canvasRef?.clientHeight || SCREEN_HEIGHT;
}

export function bindScreenCanvas(canvas, ctx) {
  canvasRef = canvas;
  ctxRef = ctx;
}

export function ensureCanvasSize() {
  if (!canvasRef || !ctxRef) {
    return;
  }
  const ratio = window.devicePixelRatio || 1;
  const width = viewportWidth();
  const height = viewportHeight();
  canvasRef.width = Math.round(width * ratio);
  canvasRef.height = Math.round(height * ratio);
  ctxRef.setTransform(ratio, 0, 0, ratio, 0, 0);
}

export function laneX(lane) {
  const found = LANE_META.find((item) => item.lane === lane);
  return (found?.x || 0.5) * viewportWidth();
}

export function laneY(index, total) {
  const top = 96;
  const bottom = viewportHeight() - 104;
  if (total <= 1) {
    return (top + bottom) / 2;
  }
  return top + ((bottom - top) * index) / (total - 1);
}

function hashSeed(text) {
  const value = String(text || "");
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
  }
  return Math.abs(hash);
}

export function lanePosition(lane, index, total, entityKey = "", entityType = "agent") {
  const width = viewportWidth();
  const height = viewportHeight();
  const laneCenterX = laneX(lane);
  const usableTop = 100;
  const usableBottom = height - 118;
  const usableHeight = Math.max(160, usableBottom - usableTop);
  const seed = hashSeed(`${lane}:${entityKey}:${entityType}`);
  const laneSpread = entityType === "property" ? 118 : 156;
  const minDistance = entityType === "property" ? 26 : 32;
  const startX = laneCenterX - laneSpread / 2;
  const rng = (step) => {
    const value = Math.sin(seed * 0.00013 + step * 12.9898) * 43758.5453;
    return value - Math.floor(value);
  };
  let best = null;
  for (let attempt = 0; attempt < 18; attempt += 1) {
    const x = startX + rng(attempt + 1) * laneSpread;
    const y = usableTop + rng(attempt + 101) * usableHeight;
    const centerBias = Math.abs(x - laneCenterX) / laneSpread;
    const edgeBias = Math.abs(y - (usableTop + usableHeight / 2)) / usableHeight;
    const score = centerBias * 0.25 + edgeBias * 0.12;
    if (!best || score < best.score) {
      best = { x, y, score };
    }
    if (attempt > 4) {
      break;
    }
  }
  const x = (best?.x ?? laneCenterX) + ((seed % 9) - 4) * 0.7;
  const y = (best?.y ?? (usableTop + usableHeight / 2)) + (((Math.floor(seed / 9)) % 9) - 4) * 0.8;
  return {
    x: Math.max(56 + minDistance, Math.min(width - 56 - minDistance, x)),
    y: Math.max(usableTop + minDistance, Math.min(usableBottom - minDistance, y)),
  };
}

export function addAmbientParticles() {
  if (screenState.ambientParticles.length > 0) {
    return;
  }
  for (let index = 0; index < MAX_AMBIENT_PARTICLES; index += 1) {
    screenState.ambientParticles.push({
      id: `ambient-${index}`,
      lane: LANE_META[index % LANE_META.length].lane,
      x: Math.random() * viewportWidth(),
      y: Math.random() * viewportHeight(),
      size: 1 + Math.random() * 2.2,
      speed: 0.2 + Math.random() * 0.6,
      alpha: 0.08 + Math.random() * 0.12,
    });
  }
}

function drawBackground() {
  const width = viewportWidth();
  const height = viewportHeight();
  const bg = ctxRef.createLinearGradient(0, 0, width, height);
  bg.addColorStop(0, "#07151d");
  bg.addColorStop(0.52, "#102433");
  bg.addColorStop(1, "#0b1218");
  ctxRef.fillStyle = bg;
  ctxRef.fillRect(0, 0, width, height);

  const focusX = laneX(screenState.focusLane);
  const spotlight = ctxRef.createRadialGradient(focusX, height * 0.48, 20, focusX, height * 0.48, 260);
  spotlight.addColorStop(0, `rgba(244, 212, 125, ${0.12 + screenState.focusStrength * 0.16})`);
  spotlight.addColorStop(1, "rgba(244, 212, 125, 0)");
  ctxRef.fillStyle = spotlight;
  ctxRef.fillRect(0, 0, width, height);

  LANE_META.forEach((column, index) => {
    const nextLane = LANE_META[Math.min(index + 1, LANE_META.length - 1)];
    const bandWidth = (index === LANE_META.length - 1 ? width : laneX(nextLane.lane)) - laneX(column.lane) + 82;
    const bandX = laneX(column.lane) - 72;
    ctxRef.fillStyle = `${column.band}66`;
    ctxRef.fillRect(bandX, 52, bandWidth, height - 116);
    ctxRef.strokeStyle = column.lane === screenState.focusLane ? "rgba(244, 212, 125, 0.52)" : "rgba(255,255,255,0.08)";
    ctxRef.lineWidth = column.lane === screenState.focusLane ? 2 : 1;
    ctxRef.strokeRect(bandX, 52, bandWidth, height - 116);
    ctxRef.fillStyle = column.lane === screenState.focusLane ? "rgba(244, 212, 125, 0.95)" : "rgba(255,255,255,0.62)";
    ctxRef.font = "12px monospace";
    const laneLabel = window.localStorage?.getItem("vre.uiLanguage") === "en" ? column.labelEn : column.labelZh;
    ctxRef.fillText(laneLabel, laneX(column.lane) - 30, 34);
  });
}

function drawAmbientParticles() {
  screenState.ambientParticles.forEach((particle) => {
    ctxRef.fillStyle = `rgba(205, 226, 247, ${particle.alpha})`;
    ctxRef.beginPath();
    ctxRef.arc(particle.x - screenState.cameraX * 0.08, particle.y, particle.size, 0, Math.PI * 2);
    ctxRef.fill();
  });
}

function drawRipples() {
  screenState.ripples.forEach((ripple) => {
    ctxRef.save();
    ctxRef.strokeStyle = ripple.tone;
    ctxRef.globalAlpha = Math.max(0.08, ripple.ttl / 1080);
    ctxRef.lineWidth = 1.6;
    ctxRef.beginPath();
    ctxRef.arc(ripple.x, ripple.y, ripple.radius, 0, Math.PI * 2);
    ctxRef.stroke();
    ctxRef.restore();
  });
}

function drawNodes() {
  function compactNodeLabel(node, denseLane, expanded) {
    const raw = String(node.label || "").trim();
    if (!raw) {
      return "";
    }
    if (node.entityType === "property") {
      const match = raw.match(/(\d+)/);
      const id = match ? match[1] : raw.replace(/^房#?/, "");
      return expanded ? raw : `房${id}`;
    }
    if (expanded) {
      return raw.length > 8 ? `${raw.slice(0, 8)}…` : raw;
    }
    if (denseLane) {
      return raw.slice(0, 2);
    }
    return raw.length > 4 ? raw.slice(0, 4) : raw;
  }

  const labeledNodes = [];
  screenState.nodes.forEach((node) => {
    ctxRef.save();
    ctxRef.globalAlpha = Math.max(0.18, 0.4 + node.energy * 0.45);
    ctxRef.strokeStyle = node.tone;
    ctxRef.shadowColor = node.tone;
    ctxRef.shadowBlur = 16;
    ctxRef.lineWidth = node.entityType === "property" ? 2.2 : 2.6;
    if (node.entityType === "property") {
      const size = (node.radius + node.energy * 4) * 1.7;
      ctxRef.strokeRect(node.x - size / 2, node.y - size / 2, size, size);
    } else {
      ctxRef.beginPath();
      ctxRef.arc(node.x, node.y, node.radius + node.energy * 4, 0, Math.PI * 2);
      ctxRef.stroke();
    }
    ctxRef.restore();

    const laneNodes = screenState.nodes.filter((item) => item.lane === node.lane);
    const denseLane = laneNodes.length >= 10;
    const nearExistingLabel = labeledNodes.some((item) => {
      const dx = item.x - node.x;
      const dy = item.y - node.y;
      return Math.hypot(dx, dy) < (denseLane ? 58 : 46);
    });
    const isFocusLane = node.lane === screenState.focusLane;
    const shouldLabel =
      node.energy > 0.92 ||
      isFocusLane ||
      laneNodes.length <= 6 ||
      (!nearExistingLabel && labeledNodes.filter((item) => item.lane === node.lane).length < (denseLane ? 4 : 6));
    if (!shouldLabel) {
      return;
    }

    const expandedLabel = node.energy > 0.92 || isFocusLane;
    const label = compactNodeLabel(node, denseLane, expandedLabel);
    if (!label) {
      return;
    }
    ctxRef.fillStyle = "rgba(235, 241, 247, 0.88)";
    ctxRef.font = `${node.entityType === "property" ? 9 : (denseLane ? 8 : 8.8)}px 'Segoe UI', 'PingFang SC', sans-serif`;
    ctxRef.fillText(label, node.x + 11, node.y + 2);
    labeledNodes.push({ x: node.x, y: node.y, lane: node.lane });
  });
}

// Keep a tiny legacy sprite hook so screen-stage smoke tests can verify the
// canvas layer still supports drawImage-based fallbacks if needed later.
function drawLegacySprite(image, x, y, size) {
  if (!image) {
    return;
  }
  ctxRef.drawImage(image, x, y, size, size);
}

function drawBeams() {
  screenState.beams.forEach((beam) => {
    const currentX = beam.fromX + (beam.toX - beam.fromX) * beam.progress;
    ctxRef.strokeStyle = beam.tone;
    ctxRef.lineWidth = 2.4;
    ctxRef.globalAlpha = 0.72;
    ctxRef.beginPath();
    ctxRef.moveTo(beam.fromX, beam.y);
    ctxRef.quadraticCurveTo((beam.fromX + currentX) / 2, beam.y - beam.arc, currentX, beam.y);
    ctxRef.stroke();

    ctxRef.fillStyle = beam.tone;
    ctxRef.beginPath();
    ctxRef.arc(currentX, beam.y, 4.8, 0, Math.PI * 2);
    ctxRef.fill();
    ctxRef.globalAlpha = 1;
  });
}

function drawSparks() {
  screenState.sparks.forEach((spark) => {
    ctxRef.save();
    ctxRef.globalAlpha = Math.max(0.12, spark.ttl / 950);
    ctxRef.fillStyle = spark.tone;
    ctxRef.beginPath();
    ctxRef.arc(spark.x, spark.y, 6, 0, Math.PI * 2);
    ctxRef.fill();
    ctxRef.restore();
  });
}

export function renderScreen() {
  if (!ctxRef || !canvasRef) {
    return;
  }
  ensureCanvasSize();
  const shakeX = screenState.cameraShake > 0 ? (Math.random() - 0.5) * screenState.cameraShake : 0;
  ctxRef.save();
  ctxRef.translate(-screenState.cameraX * 0.08 + shakeX, 0);
  drawBackground();
  drawAmbientParticles();
  drawRipples();
  drawBeams();
  drawSparks();
  drawNodes();
  ctxRef.restore();
}

export function stepScreen(deltaMs) {
  screenState.cameraX += (screenState.cameraTargetX - screenState.cameraX) * Math.min(1, deltaMs / 380);
  screenState.cameraShake = Math.max(0, screenState.cameraShake - deltaMs / 26);
  screenState.focusStrength = Math.max(0.16, screenState.focusStrength - deltaMs / 1800);

  screenState.nodes.forEach((node, index) => {
    node.energy = Math.max(0.2, node.energy - deltaMs / 5200);
    if (node.ttl != null) {
      node.ttl -= deltaMs;
    }
    const driftBase = (screenState.lastTs + index * 37) / 900 + (node.driftSeed || 0);
    node.x = node.anchorX + Math.cos(driftBase * 0.72) * 3.8;
    node.y = node.anchorY + Math.sin(driftBase) * 4.2;
  });
  screenState.nodes = screenState.nodes.filter((node) => node.ttl == null || node.ttl > 0);

  screenState.beams.forEach((beam) => {
    beam.progress += deltaMs / 820;
  });
  screenState.beams = screenState.beams.filter((beam) => beam.progress < 1.05);

  screenState.sparks.forEach((spark) => {
    spark.ttl -= deltaMs;
    spark.y -= deltaMs / 38;
  });
  screenState.sparks = screenState.sparks.filter((spark) => spark.ttl > 0);

  screenState.ripples.forEach((ripple) => {
    ripple.ttl -= deltaMs;
    ripple.radius += deltaMs / 10;
  });
  screenState.ripples = screenState.ripples.filter((ripple) => ripple.ttl > 0);

  screenState.ambientParticles.forEach((particle) => {
    particle.y -= particle.speed * deltaMs * 0.06;
    if (particle.y < 48) {
      particle.y = viewportHeight() - 40;
      particle.x = Math.random() * viewportWidth();
    }
  });
}
