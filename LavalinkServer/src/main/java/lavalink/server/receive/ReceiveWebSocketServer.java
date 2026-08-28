package lavalink.server.receive;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Component
public final class ReceiveWebSocketServer extends AbstractWebSocketHandler {
    static final String BUILD_IDENTITY = "lavalink-3.7.13+red.5-audio-receive.8";
    private static final Logger log = LoggerFactory.getLogger(ReceiveWebSocketServer.class);
    private static final int OUTBOUND_CAPACITY = 8192;

    private final ObjectMapper mapper;
    private final ReceiveCoordinator coordinator;
    private final AtomicReference<WebSocketSession> client = new AtomicReference<>();
    private final BlockingQueue<OutboundMessage> outbound =
            new ArrayBlockingQueue<>(OUTBOUND_CAPACITY);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread sender;

    public ReceiveWebSocketServer(ObjectMapper mapper, ReceiveCoordinator coordinator) {
        this.mapper = mapper;
        this.coordinator = coordinator;
        coordinator.attach(this);
        sender = new Thread(this::senderLoop, "audio-receive-websocket-sender");
        sender.setDaemon(true);
        sender.start();
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        if (!client.compareAndSet(null, session)) {
            session.close(CloseStatus.POLICY_VIOLATION.withReason("only one receive client is permitted"));
            return;
        }
        Map<String, Object> hello = new LinkedHashMap<>();
        hello.put("op", "hello");
        hello.put("protocol", 1);
        hello.put("build", BUILD_IDENTITY);
        hello.put("capabilities", Arrays.asList(
                "pcm_s16le_48000_stereo", "ordered_barriers", "transport_generation",
                "receive_diagnostics"));
        if (!sendControl(session, hello)) {
            closeSession(session, "outbound queue unavailable during hello");
        }
        log.info("[AudioReceive] Client connected id={} remote={}; transport owner=Koe",
                session.getId(), session.getRemoteAddress());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        if (client.get() != session) {
            return;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> control = mapper.readValue(message.getPayload(), Map.class);
            coordinator.handleControl(session, control);
        } catch (Exception exception) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("op", "error");
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> original = mapper.readValue(message.getPayload(), Map.class);
                if (original.get("requestId") != null) {
                    error.put("requestId", original.get("requestId").toString());
                }
            } catch (Exception ignored) {
                // The original message was not an object; there is no request to correlate.
            }
            error.put("message", "invalid control message: " + exception.getMessage());
            sendControl(session, error);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        if (client.compareAndSet(session, null)) {
            discardMessagesFor(session);
            coordinator.clientDisconnected(session, "receive WebSocket closed: " + status);
        }
        log.info("[AudioReceive] Client disconnected id={} status={}", session.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.warn("[AudioReceive] WebSocket transport error for client {}", session.getId(), exception);
        closeSession(session, "receive WebSocket transport error");
    }

    boolean sendControl(WebSocketSession target, Map<String, ?> payload) {
        try {
            return enqueue(target, new TextMessage(mapper.writeValueAsString(payload)));
        } catch (Exception exception) {
            log.error("[AudioReceive] Could not serialize control message", exception);
            return false;
        }
    }

    boolean sendBinary(WebSocketSession target, byte[] payload) {
        return enqueue(target, new BinaryMessage(payload));
    }

    private boolean enqueue(WebSocketSession target, WebSocketMessage<?> message) {
        if (target == null || client.get() != target || !target.isOpen()) {
            return false;
        }
        if (!outbound.offer(new OutboundMessage(target, message))) {
            closeSession(target, "bounded outbound queue overflowed");
            return false;
        }
        return true;
    }

    private void senderLoop() {
        while (!closed.get()) {
            try {
                OutboundMessage delivery = outbound.take();
                WebSocketSession session = delivery.session;
                if (client.get() == session && session.isOpen()) {
                    try {
                        session.sendMessage(delivery.message);
                    } catch (IOException | IllegalStateException exception) {
                        log.warn("[AudioReceive] WebSocket send failed", exception);
                        closeSession(session, "receive WebSocket send failed");
                    }
                }
            } catch (InterruptedException exception) {
                if (closed.get()) {
                    return;
                }
            }
        }
    }

    private void closeCurrent(String reason) {
        WebSocketSession session = client.get();
        if (session != null) {
            closeSession(session, reason);
        }
    }

    private void closeSession(WebSocketSession session, String reason) {
        if (!client.compareAndSet(session, null)) {
            return;
        }
        discardMessagesFor(session);
        if (session != null) {
            try {
                session.close(CloseStatus.SERVER_ERROR.withReason(reason));
            } catch (IOException exception) {
                log.debug("Error closing failed receive client", exception);
            }
            coordinator.clientDisconnected(session, reason);
        }
    }

    private void discardMessagesFor(WebSocketSession session) {
        outbound.removeIf(delivery -> delivery.session == session);
    }

    @PreDestroy
    public void shutdown() {
        closed.set(true);
        sender.interrupt();
        closeCurrent("Lavalink is shutting down");
    }

    private static final class OutboundMessage {
        final WebSocketSession session;
        final WebSocketMessage<?> message;

        OutboundMessage(WebSocketSession session, WebSocketMessage<?> message) {
            this.session = session;
            this.message = message;
        }
    }
}
