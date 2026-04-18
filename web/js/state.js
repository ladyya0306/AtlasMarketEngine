export const appState = {
  laneStats: {
    activation: 0,
    success: 0,
    failure: 0,
    system: 0,
  },
  activationNodesByAgent: new Map(),
  agentTypesById: new Map(),
  generatedNodesByAgent: new Map(),
  listedPropertyNodesById: new Map(),
  negotiationGroupNodesById: new Map(),
  negotiationAttemptNodesByKey: new Map(),
  negotiationClosedNodesByKey: new Map(),
  negotiationQuoteNodesById: new Map(),
  negotiationQuoteScores: new Map(),
  negotiationReplayNodesById: new Map(),
  negotiationReplayExpanded: new Set(),
  negotiationQuoteFocusLimit: 2,
  negotiationArchiveSnippets: [],
  announcedMonth: null,
  laneMonth: null,
  chartHistory: [],
  screenProgressEntries: [],
  presetHistory: [],
  archivePayloads: [],
  lastStatus: null,
  lastMonthPayload: null,
  lastFinalReviewStatus: null,
  lastFinalSummary: null,
  reviewOutcomes: [],
  reviewAgentStats: new Map(),
  reviewPropertyStats: new Map(),
  reviewFailureStats: new Map(),
};

export function resetLaneStats() {
  appState.laneStats = {
    activation: 0,
    success: 0,
    failure: 0,
    system: 0,
  };
  appState.activationNodesByAgent.clear();
  appState.agentTypesById.clear();
  appState.generatedNodesByAgent.clear();
  appState.listedPropertyNodesById.clear();
  appState.negotiationGroupNodesById.clear();
  appState.negotiationAttemptNodesByKey.clear();
  appState.negotiationClosedNodesByKey.clear();
  appState.negotiationQuoteNodesById.clear();
  appState.negotiationQuoteScores.clear();
  appState.negotiationReplayNodesById.clear();
  appState.negotiationReplayExpanded.clear();
  appState.negotiationArchiveSnippets = [];
  appState.screenProgressEntries = [];
  appState.laneMonth = null;
  appState.presetHistory = [];
  appState.archivePayloads = [];
  appState.lastStatus = null;
  appState.lastMonthPayload = null;
  appState.lastFinalReviewStatus = null;
  appState.lastFinalSummary = null;
  appState.reviewOutcomes = [];
  appState.reviewAgentStats.clear();
  appState.reviewPropertyStats.clear();
  appState.reviewFailureStats.clear();
}
