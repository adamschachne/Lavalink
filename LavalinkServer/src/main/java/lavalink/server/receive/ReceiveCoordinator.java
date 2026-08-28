package lavalink.server.receive;

import com.sedmelluq.discord.lavaplayer.natives.opus.OpusDecoder;
import lavalink.server.io.SocketContext;
import moe.kyokobot.koe.ReceivedAudioFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import javax.annotation.PreDestroy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

@Component
public final class ReceiveCoordinator {
    private static final Logger log = LoggerFactory.getLogger(ReceiveCoordinator.class);
    private static final int SAMPLE_RATE = 48_000;
    private static final int CHANNELS = 2;
    private static final int SAMPLE_WIDTH = 2;
    private static final int WORK_CAPACITY = 8192;
    private static final byte[] MAGIC = new byte[]{'A', 'R', 'X', '1'};
    private static final Set<String> WINDOW_COUNTERS = Set.of(
            "udpPackets", "plausibleAudioPackets", "outboundSsrcPackets",
            "unknownSsrcPackets", "transportDecryptFailures", "daveDecryptFailures",
            "dispatchedFrames", "outboundFrameAttempts", "outboundDaveFailures",
            "outboundTransportFailures", "outboundPacketBuildFailures", "outboundPackets");

    private final ConcurrentHashMap<Long, SocketContext> contexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ActiveRecording> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<StreamKey, OpusDecoder> decoders = new ConcurrentHashMap<>();
    private final Set<Long> stopping = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<Work> work = new ArrayBlockingQueue<>(WORK_CAPACITY);
    private final ExecutorService barrierEnqueuer = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "audio-receive-barrier-enqueuer");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread decoderWorker;
    private volatile ReceiveWebSocketServer server;

    public ReceiveCoordinator() {
        decoderWorker = new Thread(this::decoderLoop, "audio-receive-opus-decoder");
        decoderWorker.setDaemon(true);
        decoderWorker.start();
    }

    void attach(ReceiveWebSocketServer server) {
        if (this.server != null) {
            throw new IllegalStateException("receive WebSocket server attached twice");
        }
        this.server = Objects.requireNonNull(server);
    }

    public void registerContext(SocketContext context, long guildId) {
        SocketContext previous = contexts.put(guildId, context);
        if (previous != null && previous != context) {
            failRecording(previous, guildId, "Lavalink player ownership changed");
        }
    }

    public void unregisterContext(SocketContext context, long guildId, String reason) {
        if (contexts.remove(guildId, context)) {
            failRecording(context, guildId, reason);
        }
    }

    void handleControl(WebSocketSession client, Map<String, Object> message) {
        String op = requiredString(message, "op");
        if ("start".equals(op)) {
            start(client, message);
        } else if ("stop".equals(op)) {
            stop(client, message);
        } else if ("status".equals(op)) {
            status(client, message);
        } else {
            error(client, message.get("requestId"), "unsupported operation " + op);
        }
    }

    private void start(WebSocketSession client, Map<String, Object> message) {
        final String requestId = requiredString(message, "requestId");
        final UUID recordingId = UUID.fromString(requiredString(message, "recordingId"));
        final long guildId = parseId(message, "guildId");
        final long channelId = parseId(message, "channelId");
        final long botUserId = parseId(message, "botUserId");
        final SocketContext context = contexts.get(guildId);
        if (context == null) {
            error(client, requestId, "voice transport is not ready for guild " + guildId);
            return;
        }
        if (stopping.contains(guildId)) {
            error(client, requestId, "the previous recording stop barrier is still draining");
            return;
        }
        try {
            context.executeOnVoiceTransport(guildId, () -> {
                try {
                    if (active.containsKey(guildId)) {
                        error(client, requestId, "a recording is already active for guild " + guildId);
                        return;
                    }
                    long generation = context.getVoiceTransportGeneration(guildId);
                    if (generation <= 0) {
                        error(client, requestId, "voice transport is not ready for guild " + guildId);
                        return;
                    }
                    context.setVoiceReceiveEnabled(guildId, true);
                    long marker = System.nanoTime();
                    Map<String, Long> diagnostics =
                            context.getVoiceReceiveDiagnostics(guildId);
                    ActiveRecording recording = new ActiveRecording(
                            recordingId, guildId, channelId, botUserId, generation, marker,
                            context, client, diagnostics);
                    if (active.putIfAbsent(guildId, recording) != null) {
                        error(client, requestId, "a recording is already active for guild " + guildId);
                        return;
                    }
                    Map<String, Object> ack = response("start_ack", requestId, recording);
                    ack.put("markerNs", marker);
                    addDiagnostics(ack, recording, diagnostics);
                    if (!sendControl(recording, ack)) {
                        active.remove(guildId, recording);
                        recording.failed.set(true);
                        disableReceive(recording);
                        scheduleCleanup(recording);
                        return;
                    }
                    log.info("[AudioReceive] Start barrier guild={} channel={} recording={} "
                                    + "generation={} diagnostics={}",
                            guildId, channelId, recordingId, generation, diagnostics);
                } catch (RuntimeException exception) {
                    try {
                        context.setVoiceReceiveEnabled(guildId, false);
                    } catch (RuntimeException ignored) {
                        // The transport may have disappeared with the failed start.
                    }
                    error(client, requestId, "voice transport is not ready: " + exception.getMessage());
                }
            });
        } catch (RuntimeException exception) {
            error(client, requestId, "voice transport is not ready: " + exception.getMessage());
        }
    }

    private void stop(WebSocketSession client, Map<String, Object> message) {
        final String requestId = requiredString(message, "requestId");
        final UUID recordingId = UUID.fromString(requiredString(message, "recordingId"));
        final long guildId = parseId(message, "guildId");
        final ActiveRecording recording = active.get(guildId);
        if (recording == null || !recording.id.equals(recordingId)
                || recording.client != client) {
            error(client, requestId, "no matching recording is active for guild " + guildId);
            return;
        }
        try {
            if (!stopping.add(guildId)) {
                error(client, requestId, "a stop barrier is already draining for this guild");
                return;
            }
            recording.context.executeOnVoiceTransport(guildId, () -> {
                try {
                    if (!active.remove(guildId, recording)) {
                        stopping.remove(guildId);
                        error(client, requestId, "recording changed before stop barrier");
                        return;
                    }
                    long marker = System.nanoTime();
                    Map<String, Long> diagnostics =
                            recording.context.getVoiceReceiveDiagnostics(guildId);
                    recording.context.setVoiceReceiveEnabled(guildId, false);
                    barrierEnqueuer.execute(
                            () -> putBarrier(new StopWork(
                                    recording, requestId, marker, diagnostics)));
                } catch (RuntimeException exception) {
                    stopping.remove(guildId);
                    active.remove(guildId, recording);
                    recording.failed.set(true);
                    disableReceive(recording);
                    scheduleCleanup(recording);
                    error(client, requestId, "ordered stop failed: " + exception.getMessage());
                }
            });
        } catch (RuntimeException exception) {
            stopping.remove(guildId);
            if (active.remove(guildId, recording)) {
                recording.failed.set(true);
                disableReceive(recording);
                fail(recording, "voice transport stopped before stop barrier");
                scheduleCleanup(recording);
            }
            error(client, requestId, "ordered stop failed: " + exception.getMessage());
        }
    }

    private void status(WebSocketSession client, Map<String, Object> message) {
        final String requestId = requiredString(message, "requestId");
        final UUID recordingId = UUID.fromString(requiredString(message, "recordingId"));
        final long guildId = parseId(message, "guildId");
        final ActiveRecording recording = active.get(guildId);
        if (recording == null || !recording.id.equals(recordingId)
                || recording.client != client) {
            error(client, requestId, "no matching recording is active for guild " + guildId);
            return;
        }
        try {
            recording.context.executeOnVoiceTransport(guildId, () -> {
                Map<String, Long> diagnostics =
                        recording.context.getVoiceReceiveDiagnostics(guildId);
                Map<String, Object> ack = response("status_ack", requestId, recording);
                ack.put("acceptedFrames", recording.accepted.get());
                ack.put("emittedFrames", recording.emitted.get());
                ack.put("lossCount", recording.loss.get());
                ack.put("statistics", statistics(recording));
                addDiagnostics(ack, recording, diagnostics);
                sendControl(recording, ack);
            });
        } catch (RuntimeException exception) {
            error(client, requestId, "diagnostic snapshot failed: " + exception.getMessage());
        }
    }

    public void acceptFrame(SocketContext context, ReceivedAudioFrame frame) {
        ActiveRecording recording = active.get(frame.getGuildId());
        if (recording == null || recording.context != context
                || frame.getUserId() == recording.botUserId) {
            return;
        }
        if (frame.getReceivedNanos() < recording.startMarkerNanos) {
            return;
        }
        if (frame.getChannelId() != recording.channelId) {
            recording.channelMismatches.incrementAndGet();
            recording.loss.incrementAndGet();
            sendLoss(recording, "channelMismatch", 1,
                    "received audio from a different voice channel");
            return;
        }
        if (frame.getTransportGeneration() != recording.generation) {
            generationChanged(context, frame.getGuildId(), frame.getChannelId(),
                    frame.getTransportGeneration());
            return;
        }
        if (!work.offer(new FrameWork(recording, frame))) {
            recording.decodeQueueDrops.incrementAndGet();
            recording.loss.incrementAndGet();
            sendLoss(recording, "decodeQueueDrop", 1,
                    "bounded Opus decode queue overflowed");
        } else {
            recording.accepted.incrementAndGet();
        }
    }

    public void generationChanged(SocketContext context, long guildId, long channelId,
                                  long generation) {
        ActiveRecording recording = active.get(guildId);
        if (recording == null || recording.context != context
                || recording.channelId != channelId) {
            return;
        }
        long previousGeneration = recording.generation;
        recording.generation = generation;
        recording.generationChanges.incrementAndGet();
        try {
            recording.context.executeOnVoiceTransport(recording.guildId,
                    () -> recording.context.setVoiceReceiveEnabled(recording.guildId, true));
        } catch (RuntimeException exception) {
            log.warn("[AudioReceive] Could not re-enable receive after transport generation "
                    + "change guild={} generation={}", guildId, generation, exception);
        }
        barrierEnqueuer.execute(() -> putBarrier(
                new GenerationWork(recording, channelId, previousGeneration, generation)));
    }

    void clientDisconnected(WebSocketSession client, String reason) {
        active.forEach((guildId, recording) -> {
            if (recording.client == client && active.remove(guildId, recording)) {
                recording.failed.set(true);
                disableReceive(recording);
                scheduleCleanup(recording);
                log.warn("[AudioReceive] Recording {} failed: {}", recording.id, reason);
            }
        });
    }

    private void decoderLoop() {
        while (!closed.get()) {
            try {
                Work item = work.take();
                if (item instanceof FrameWork) {
                    decode((FrameWork) item);
                } else if (item instanceof StopWork) {
                    acknowledgeStop((StopWork) item);
                } else if (item instanceof GenerationWork) {
                    announceGeneration((GenerationWork) item);
                } else if (item instanceof CleanupWork) {
                    ActiveRecording recording = ((CleanupWork) item).recording;
                    closeDecoders(recording.guildId, recording.generation);
                }
            } catch (InterruptedException exception) {
                if (closed.get()) {
                    return;
                }
            } catch (Throwable exception) {
                log.error("[AudioReceive] Decoder worker recovered from an error", exception);
            }
        }
    }

    private void decode(FrameWork item) {
        ReceivedAudioFrame frame = item.frame;
        ActiveRecording recording = item.recording;
        if (recording.failed.get()) {
            return;
        }
        try {
            byte[] opus = frame.getOpus();
            int expectedSamples = OpusDecoder.getPacketFrameSize(SAMPLE_RATE, opus, 0, opus.length);
            if (expectedSamples <= 0 || expectedSamples > 5760) {
                throw new IllegalArgumentException("invalid Opus packet duration");
            }
            StreamKey key = new StreamKey(frame.getGuildId(), frame.getTransportGeneration(),
                    frame.getSsrc());
            OpusDecoder decoder = decoders.computeIfAbsent(key,
                    ignored -> new OpusDecoder(SAMPLE_RATE, CHANNELS));
            ByteBuffer input = ByteBuffer.allocateDirect(opus.length);
            input.put(opus).flip();
            ShortBuffer output = ByteBuffer.allocateDirect(expectedSamples * CHANNELS * SAMPLE_WIDTH)
                    .order(ByteOrder.nativeOrder()).asShortBuffer();
            int samples = decoder.decode(input, output);
            byte[] pcm = new byte[samples * CHANNELS * SAMPLE_WIDTH];
            for (int index = 0; index < samples * CHANNELS; index++) {
                short sample = output.get();
                pcm[index * 2] = (byte) (sample & 0xff);
                pcm[index * 2 + 1] = (byte) ((sample >>> 8) & 0xff);
            }
            byte[] packet = encode(recording, frame, samples, pcm);
            if (!sendBinary(recording, packet)) {
                recording.outboundDrops.incrementAndGet();
                recording.loss.incrementAndGet();
            } else {
                recording.emitted.incrementAndGet();
            }
        } catch (Throwable exception) {
            StreamKey key = new StreamKey(frame.getGuildId(), frame.getTransportGeneration(),
                    frame.getSsrc());
            recording.loss.incrementAndGet();
            recording.decodeFailures.incrementAndGet();
            closeDecoder(key);
            sendLoss(recording, "decodeFailure", 1,
                    "Opus decode failed: " + exception.getMessage());
            log.warn("[AudioReceive] Opus decode failed guild={} user={} ssrc={}",
                    frame.getGuildId(), frame.getUserId(),
                    Integer.toUnsignedLong(frame.getSsrc()), exception);
        }
    }

    private byte[] encode(ActiveRecording recording, ReceivedAudioFrame frame, int samples, byte[] pcm) {
        ByteBuffer packet = ByteBuffer.allocate(76 + pcm.length).order(ByteOrder.BIG_ENDIAN);
        packet.put(MAGIC);
        packet.put((byte) 1);
        packet.put((byte) 1);
        packet.putShort((short) (frame.isDave() ? 1 : 0));
        packet.putLong(recording.id.getMostSignificantBits());
        packet.putLong(recording.id.getLeastSignificantBits());
        packet.putLong(frame.getGuildId());
        packet.putLong(frame.getUserId());
        packet.putInt(frame.getSsrc());
        packet.putLong(frame.getTransportGeneration());
        packet.putShort((short) frame.getSequence());
        packet.putShort((short) 0);
        packet.putInt((int) frame.getRtpTimestamp());
        packet.putLong(Math.max(0, frame.getReceivedNanos() - recording.startMarkerNanos));
        packet.putShort((short) samples);
        packet.put((byte) CHANNELS);
        packet.put((byte) SAMPLE_WIDTH);
        packet.putInt(pcm.length);
        packet.put(pcm);
        return packet.array();
    }

    private void acknowledgeStop(StopWork stop) {
        Map<String, Object> ack = response("stop_ack", stop.requestId, stop.recording);
        ack.put("markerNs", stop.markerNanos);
        ack.put("acceptedFrames", stop.recording.accepted.get());
        ack.put("emittedFrames", stop.recording.emitted.get());
        ack.put("lossCount", stop.recording.loss.get());
        ack.put("statistics", statistics(stop.recording));
        addDiagnostics(ack, stop.recording, stop.diagnostics);
        sendControl(stop.recording, ack);
        stopping.remove(stop.recording.guildId);
        closeDecoders(stop.recording.guildId, stop.recording.generation);
        log.info("[AudioReceive] Stop barrier guild={} recording={} accepted={} emitted={} "
                        + "loss={} diagnostics={} window={}",
                stop.recording.guildId, stop.recording.id, stop.recording.accepted.get(),
                stop.recording.emitted.get(), stop.recording.loss.get(), stop.diagnostics,
                windowDiagnostics(stop.recording.startDiagnostics, stop.diagnostics));
    }

    private void announceGeneration(GenerationWork change) {
        Map<String, Object> event = response("generation_changed", null, change.recording);
        event.put("channelId", Long.toString(change.channelId));
        event.put("newGeneration", change.generation);
        sendControl(change.recording, event);
        closeDecoders(change.recording.guildId, change.previousGeneration);
    }

    private void closeDecoder(StreamKey key) {
        OpusDecoder decoder = decoders.remove(key);
        if (decoder != null) {
            decoder.close();
        }
    }

    private void closeDecoders(long guildId, long generation) {
        decoders.forEach((key, decoder) -> {
            if (key.guildId == guildId && key.generation == generation
                    && decoders.remove(key, decoder)) {
                decoder.close();
            }
        });
    }

    private void sendLoss(ActiveRecording recording, String kind, long count, String reason) {
        Map<String, Object> loss = response("loss", null, recording);
        loss.put("kind", kind);
        loss.put("count", count);
        loss.put("reason", reason);
        sendControl(recording, loss);
    }

    private static Map<String, Long> statistics(ActiveRecording recording) {
        Map<String, Long> statistics = new LinkedHashMap<>();
        statistics.put("decodeFailures", recording.decodeFailures.get());
        statistics.put("decodeQueueDrops", recording.decodeQueueDrops.get());
        statistics.put("outboundDrops", recording.outboundDrops.get());
        statistics.put("channelMismatches", recording.channelMismatches.get());
        statistics.put("generationChanges", recording.generationChanges.get());
        return statistics;
    }

    private void failRecording(SocketContext context, long guildId, String reason) {
        ActiveRecording recording = active.get(guildId);
        if (recording != null && recording.context == context
                && active.remove(guildId, recording)) {
            recording.failed.set(true);
            disableReceive(recording);
            fail(recording, reason);
            scheduleCleanup(recording);
        }
    }

    private void scheduleCleanup(ActiveRecording recording) {
        barrierEnqueuer.execute(() -> putBarrier(new CleanupWork(recording)));
    }

    private void fail(ActiveRecording recording, String reason) {
        Map<String, Object> event = response("error", null, recording);
        event.put("message", reason);
        sendControl(recording, event);
    }

    private void disableReceive(ActiveRecording recording) {
        try {
            recording.context.executeOnVoiceTransport(recording.guildId,
                    () -> recording.context.setVoiceReceiveEnabled(recording.guildId, false));
        } catch (RuntimeException exception) {
            log.debug("[AudioReceive] Voice receive was already unavailable for guild {}",
                    recording.guildId, exception);
        }
    }

    private void putBarrier(Work barrier) {
        while (!closed.get()) {
            try {
                work.put(barrier);
                return;
            } catch (InterruptedException exception) {
                if (closed.get()) {
                    return;
                }
            }
        }
    }

    private void error(WebSocketSession client, Object requestId, String message) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("op", "error");
        if (requestId != null) {
            error.put("requestId", requestId.toString());
        }
        error.put("message", message);
        ReceiveWebSocketServer current = server;
        if (current != null) {
            current.sendControl(client, error);
        }
    }

    private Map<String, Object> response(String op, String requestId, ActiveRecording recording) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("op", op);
        if (requestId != null) {
            response.put("requestId", requestId);
        }
        response.put("recordingId", recording.id.toString());
        response.put("guildId", Long.toString(recording.guildId));
        response.put("channelId", Long.toString(recording.channelId));
        response.put("generation", recording.generation);
        return response;
    }

    private void addDiagnostics(Map<String, Object> response, ActiveRecording recording,
                                Map<String, Long> current) {
        response.put("diagnostics", current);
        response.put("windowDiagnostics",
                windowDiagnostics(recording.startDiagnostics, current));
    }

    private static Map<String, Long> windowDiagnostics(Map<String, Long> start,
                                                       Map<String, Long> current) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (String key : WINDOW_COUNTERS) {
            long before = start.getOrDefault(key, 0L);
            long after = current.getOrDefault(key, 0L);
            result.put(key, Math.max(0, after - before));
        }
        return result;
    }

    private boolean sendControl(ActiveRecording recording, Map<String, ?> message) {
        ReceiveWebSocketServer current = server;
        return current != null && current.sendControl(recording.client, message);
    }

    private boolean sendBinary(ActiveRecording recording, byte[] packet) {
        ReceiveWebSocketServer current = server;
        return current != null && current.sendBinary(recording.client, packet);
    }

    private static String requiredString(Map<String, Object> message, String key) {
        Object value = message.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new IllegalArgumentException("missing string " + key);
        }
        return (String) value;
    }

    private static long parseId(Map<String, Object> message, String key) {
        return Long.parseUnsignedLong(requiredString(message, key));
    }

    @PreDestroy
    public void shutdown() {
        closed.set(true);
        decoderWorker.interrupt();
        barrierEnqueuer.shutdownNow();
        decoders.values().forEach(OpusDecoder::close);
        decoders.clear();
        active.clear();
        stopping.clear();
        work.clear();
    }

    private interface Work {}

    private static final class FrameWork implements Work {
        final ActiveRecording recording;
        final ReceivedAudioFrame frame;
        FrameWork(ActiveRecording recording, ReceivedAudioFrame frame) {
            this.recording = recording;
            this.frame = frame;
        }
    }

    private static final class StopWork implements Work {
        final ActiveRecording recording;
        final String requestId;
        final long markerNanos;
        final Map<String, Long> diagnostics;
        StopWork(ActiveRecording recording, String requestId, long markerNanos,
                 Map<String, Long> diagnostics) {
            this.recording = recording;
            this.requestId = requestId;
            this.markerNanos = markerNanos;
            this.diagnostics = diagnostics;
        }
    }

    private static final class GenerationWork implements Work {
        final ActiveRecording recording;
        final long channelId;
        final long previousGeneration;
        final long generation;
        GenerationWork(ActiveRecording recording, long channelId,
                       long previousGeneration, long generation) {
            this.recording = recording;
            this.channelId = channelId;
            this.previousGeneration = previousGeneration;
            this.generation = generation;
        }
    }

    private static final class CleanupWork implements Work {
        final ActiveRecording recording;
        CleanupWork(ActiveRecording recording) {
            this.recording = recording;
        }
    }

    private static final class ActiveRecording {
        final UUID id;
        final long guildId;
        final long channelId;
        final long botUserId;
        volatile long generation;
        final long startMarkerNanos;
        final SocketContext context;
        final WebSocketSession client;
        final Map<String, Long> startDiagnostics;
        final AtomicLong accepted = new AtomicLong();
        final AtomicLong emitted = new AtomicLong();
        final AtomicLong loss = new AtomicLong();
        final AtomicLong decodeFailures = new AtomicLong();
        final AtomicLong decodeQueueDrops = new AtomicLong();
        final AtomicLong outboundDrops = new AtomicLong();
        final AtomicLong channelMismatches = new AtomicLong();
        final AtomicLong generationChanges = new AtomicLong();
        final AtomicBoolean failed = new AtomicBoolean();
        ActiveRecording(UUID id, long guildId, long channelId, long botUserId,
                          long generation, long startMarkerNanos, SocketContext context,
                         WebSocketSession client, Map<String, Long> startDiagnostics) {
            this.id = id;
            this.guildId = guildId;
            this.channelId = channelId;
            this.botUserId = botUserId;
            this.generation = generation;
            this.startMarkerNanos = startMarkerNanos;
            this.context = context;
            this.client = client;
            this.startDiagnostics = new LinkedHashMap<>(startDiagnostics);
        }
    }

    private static final class StreamKey {
        final long guildId;
        final long generation;
        final int ssrc;
        StreamKey(long guildId, long generation, int ssrc) {
            this.guildId = guildId;
            this.generation = generation;
            this.ssrc = ssrc;
        }
        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof StreamKey)) return false;
            StreamKey key = (StreamKey) other;
            return guildId == key.guildId && generation == key.generation && ssrc == key.ssrc;
        }
        @Override public int hashCode() { return Objects.hash(guildId, generation, ssrc); }
    }
}
