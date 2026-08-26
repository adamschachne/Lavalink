package dev.arbjerg.lavalink.api;

import java.util.Arrays;
import java.util.Objects;

/** Immutable Koe-received Opus frame exposed to Lavalink plugins. */
public final class VoiceFrame {
    private final long guildId;
    private final long channelId;
    private final long userId;
    private final int ssrc;
    private final int sequence;
    private final long rtpTimestamp;
    private final long transportGeneration;
    private final long receivedNanos;
    private final boolean marker;
    private final boolean dave;
    private final int[] csrcs;
    private final int extensionProfile;
    private final int extensionWords;
    private final byte[] opus;

    public VoiceFrame(long guildId, long channelId, long userId, int ssrc, int sequence,
                      long rtpTimestamp, long transportGeneration, long receivedNanos,
                      boolean marker, boolean dave, int[] csrcs, int extensionProfile,
                      int extensionWords, byte[] opus) {
        this.guildId = guildId;
        this.channelId = channelId;
        this.userId = userId;
        this.ssrc = ssrc;
        this.sequence = sequence;
        this.rtpTimestamp = rtpTimestamp;
        this.transportGeneration = transportGeneration;
        this.receivedNanos = receivedNanos;
        this.marker = marker;
        this.dave = dave;
        this.csrcs = Arrays.copyOf(Objects.requireNonNull(csrcs), csrcs.length);
        this.extensionProfile = extensionProfile;
        this.extensionWords = extensionWords;
        this.opus = Arrays.copyOf(Objects.requireNonNull(opus), opus.length);
    }

    public long getGuildId() { return guildId; }
    public long getChannelId() { return channelId; }
    public long getUserId() { return userId; }
    public int getSsrc() { return ssrc; }
    public int getSequence() { return sequence; }
    public long getRtpTimestamp() { return rtpTimestamp; }
    public long getTransportGeneration() { return transportGeneration; }
    public long getReceivedNanos() { return receivedNanos; }
    public boolean isMarker() { return marker; }
    public boolean isDave() { return dave; }
    public int[] getCsrcs() { return Arrays.copyOf(csrcs, csrcs.length); }
    public int getExtensionProfile() { return extensionProfile; }
    public int getExtensionWords() { return extensionWords; }
    public byte[] getOpus() { return Arrays.copyOf(opus, opus.length); }
}
