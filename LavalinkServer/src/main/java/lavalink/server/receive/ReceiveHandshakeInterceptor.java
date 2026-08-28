package lavalink.server.receive;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

final class ReceiveHandshakeInterceptor implements HandshakeInterceptor {
    private final byte[] password;

    ReceiveHandshakeInterceptor(String password) {
        this.password = password.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler handler, Map<String, Object> attributes) {
        String supplied = request.getHeaders().getFirst("Authorization");
        boolean valid = supplied != null && MessageDigest.isEqual(
                password, supplied.getBytes(StandardCharsets.UTF_8));
        if (!valid) {
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
        }
        return valid;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler handler, Exception exception) {
        // No state is established before authentication succeeds.
    }
}
