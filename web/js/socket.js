import { socketIndicator } from "./dom.js";
import { t } from "./i18n.js";

export function connectSocket(onEvent) {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);

  socket.addEventListener("open", () => {
    socketIndicator.textContent = t("socket.connected");
  });

  socket.addEventListener("close", () => {
    socketIndicator.textContent = t("socket.disconnected");
    setTimeout(() => connectSocket(onEvent), 1500);
  });

  socket.addEventListener("message", (raw) => {
    onEvent(JSON.parse(raw.data));
  });

  return socket;
}
