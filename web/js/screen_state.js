import { getLang } from "./i18n.js";

export const SCREEN_WIDTH = 1280;
export const SCREEN_HEIGHT = 720;
export const MAX_BEAMS = 24;
export const MAX_SPARKS = 48;
export const MAX_RIPPLES = 18;
export const MAX_AMBIENT_PARTICLES = 72;
export const NODE_RADIUS = 10;
export const SPRITE_SIZE = 28;
export const SPRITE_SOURCES = {
  agentNormal: "/web/assets/icons/agent-normal.svg",
  agentSmart: "/web/assets/icons/agent-smart.svg",
  property: "/web/assets/icons/property.svg",
};

export const LANE_META = [
  { lane: "generated", labelZh: "生成", labelEn: "Generation", x: 0.16, band: "#16384b" },
  { lane: "activation", labelZh: "激活", labelEn: "Activation", x: 0.3, band: "#153c34" },
  { lane: "listing", labelZh: "挂牌", labelEn: "Listings", x: 0.48, band: "#163255" },
  { lane: "negotiation", labelZh: "谈判", labelEn: "Negotiation", x: 0.62, band: "#1d2f5a" },
  { lane: "success", labelZh: "成功", labelEn: "Success", x: 0.8, band: "#173f2f" },
  { lane: "failure", labelZh: "失败", labelEn: "Failure", x: 0.89, band: "#4a1d26" },
];

export const screenState = {
  running: true,
  lastTs: 0,
  stageLabel: "idle",
  focusLane: "generated",
  focusStrength: 0.24,
  cameraX: 0,
  cameraTargetX: 0,
  cameraShake: 0,
  nodes: [],
  beams: [],
  sparks: [],
  ripples: [],
  ambientParticles: [],
  counts: createEmptyCounts(),
};

export const spriteImages = {};

export function createEmptyCounts() {
  return {
    generatedAgents: 0,
    generatedProperties: 0,
    activations: 0,
    listings: 0,
    matches: 0,
    negotiations: 0,
    successes: 0,
    failures: 0,
  };
}

export function loadSpriteCatalog() {
  return spriteImages;
}

export function classifyLane(eventType) {
  if (eventType === "AGENT_GENERATED" || eventType === "PROPERTY_GENERATED") return "generated";
  if (eventType === "PROPERTY_LISTED") return "listing";
  if (eventType === "AGENT_ACTIVATED") return "activation";
  if (eventType.startsWith("NEGOTIATION")) return "negotiation";
  if (eventType === "DEAL_SUCCESS") return "success";
  if (eventType === "DEAL_FAIL") return "failure";
  return "generated";
}

export function entityTypeForEvent(eventType, payload = {}) {
  if (eventType.includes("PROPERTY") || payload.property_id != null) {
    return "property";
  }
  return "agent";
}

export function toneForEvent(eventType, payload) {
  if (eventType === "AGENT_GENERATED") {
    return payload?.agent_type === "smart" ? "#f4d47d" : "#85d6c0";
  }
  if (eventType === "PROPERTY_GENERATED" || eventType === "PROPERTY_LISTED") return "#8dc2ff";
  if (eventType === "AGENT_ACTIVATED") return "#9cf0b5";
  if (eventType.startsWith("NEGOTIATION")) return "#7cc9ff";
  if (eventType === "DEAL_SUCCESS") return "#89f2a6";
  if (eventType === "DEAL_FAIL") return "#ff8d8d";
  return "#d5dce7";
}

export function spriteKeyForEvent(eventType, payload) {
  if (eventType === "AGENT_GENERATED" || eventType === "AGENT_ACTIVATED") {
    return payload?.agent_type === "smart" ? "agentSmart" : "agentNormal";
  }
  if (eventType === "PROPERTY_GENERATED" || eventType === "PROPERTY_LISTED") {
    return "property";
  }
  if (eventType === "MATCH_ATTEMPT" || eventType.startsWith("NEGOTIATION")) {
    return payload?.buyer_agent_type === "smart" ? "agentSmart" : "agentNormal";
  }
  if (eventType === "DEAL_SUCCESS" || eventType === "DEAL_FAIL") {
    return payload?.buyer_agent_type === "smart" ? "agentSmart" : "property";
  }
  return null;
}

export function focusLabel() {
  const lane = LANE_META.find((item) => item.lane === screenState.focusLane);
  const laneLabel = getLang() === "en" ? (lane?.labelEn || screenState.focusLane) : (lane?.labelZh || screenState.focusLane);
  return `${laneLabel} / ${screenState.stageLabel}`;
}

export function summarizeState() {
  return JSON.stringify({
    coordinate_system: "origin=top-left,x->right,y->down",
    running: screenState.running,
    stage_label: screenState.stageLabel,
    focus_lane: screenState.focusLane,
    camera_x: Number(screenState.cameraX.toFixed(1)),
    counts: screenState.counts,
    nodes: screenState.nodes.slice(-8).map((node) => ({
      lane: node.lane,
      eventType: node.eventType,
      label: node.label,
      x: Number(node.x.toFixed(1)),
      y: Number(node.y.toFixed(1)),
    })),
    beams: screenState.beams.length,
    sparks: screenState.sparks.length,
    ripples: screenState.ripples.length,
  });
}
