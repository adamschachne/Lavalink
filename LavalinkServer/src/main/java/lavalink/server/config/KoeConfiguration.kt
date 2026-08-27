package lavalink.server.config

import moe.kyokobot.koe.KoeOptions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KoeConfiguration {

    private val log: Logger = LoggerFactory.getLogger(KoeConfiguration::class.java)

    @Bean
    fun koeOptions(): KoeOptions = KoeOptions.builder().apply {
        // Keep inbound subscriptions closed until the receive plugin opens an
        // ordered recording window on the existing Koe transport.
        setDeafened(true)
        setEnableDAVELogSink(true)
        // JDA-NAS sends through a native UDP socket separate from Koe's
        // connected DatagramChannel. Discord then returns media to that new
        // source port, but the send-only native queue cannot feed those
        // datagrams into Koe's receive pipeline. Retain Koe's default Netty
        // poller so playback and receive share one bidirectional UDP socket.
        log.info("JDA-NAS disabled: bidirectional Audio uses Koe's Netty UDP transport")
    }.create()
}
