package lavalink.server.receive;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReceiveComponentScanTest {
    @Test
    void serverScanIncludesReceiveComponents() {
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Component.class));

        Set<String> components = scanner.findCandidateComponents("lavalink.server").stream()
                .map(definition -> definition.getBeanClassName())
                .collect(Collectors.toSet());

        assertTrue(components.containsAll(Set.of(
                ReceiveCoordinator.class.getName(),
                ReceiveWebSocketConfiguration.class.getName(),
                ReceiveWebSocketServer.class.getName())));
    }
}
