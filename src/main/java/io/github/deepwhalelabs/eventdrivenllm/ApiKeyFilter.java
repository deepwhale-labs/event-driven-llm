package io.github.deepwhalelabs.eventdrivenllm;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiKeyFilter extends OncePerRequestFilter {
    private final byte[] key;
    public ApiKeyFilter(@Value("${app.api-key:}") String key) { this.key = key.getBytes(StandardCharsets.UTF_8); }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getServletPath();
        boolean protectedPath = path.startsWith("/api/") || path.startsWith("/actuator/");
        boolean health = path.equals("/actuator/health") || path.equals("/actuator/health/liveness") || path.equals("/actuator/health/readiness");
        String provided = request.getHeader("X-API-Key");
        if (key.length > 0 && protectedPath && !health
                && (provided == null || !MessageDigest.isEqual(key, provided.getBytes(StandardCharsets.UTF_8)))) {
            response.setStatus(401);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"A valid X-API-Key header is required\"}");
            return;
        }
        response.setHeader("X-Content-Type-Options", "nosniff");
        if (path.startsWith("/api/")) response.setHeader("Cache-Control", "no-store");
        chain.doFilter(request, response);
    }
}
