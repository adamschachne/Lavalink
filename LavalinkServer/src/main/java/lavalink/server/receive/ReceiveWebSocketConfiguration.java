package lavalink.server.receive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class ReceiveWebSocketConfiguration implements WebSocketConfigurer {
    private static final Logger log = LoggerFactory.getLogger(ReceiveWebSocketConfiguration.class);
    private final ReceiveWebSocketServer server;
    private final String password;

    public ReceiveWebSocketConfiguration(ReceiveWebSocketServer server,
                                         @Value("${lavalink.server.password}") String password) {
        this.server = server;
        this.password = password;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(server, "/audio/receive/v1")
                .addInterceptors(new ReceiveHandshakeInterceptor(password));
        log.info("[AudioReceive] Authenticated receive endpoint registered at /audio/receive/v1");
    }
}
