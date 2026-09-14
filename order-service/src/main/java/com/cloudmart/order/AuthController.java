package com.cloudmart.order;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.security.Key;
import java.util.Date;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    public static final Key SECRET_KEY = Keys.hmacShaKeyFor("cloudmart-super-secret-jwt-token-key-256-bits!".getBytes());

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String password = body.get("password");

        // Basic credentials check for scaffolding verification
        if (username == null || password == null || !password.equals("password123")) {
            return ResponseEntity.status(401).body(Map.of("error", "Invalid credentials"));
        }

        String role = "admin".equalsIgnoreCase(username) ? "ROLE_ADMIN" : "ROLE_USER";

        String token = Jwts.builder()
                .setSubject(username)
                .claim("role", role)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + 86400000))
                .signWith(SECRET_KEY)
                .compact();

        return ResponseEntity.ok(Map.of(
                "token", token,
                "username", username,
                "role", role
        ));
    }
}
