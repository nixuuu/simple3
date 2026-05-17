# TLS via reverse proxy

simple3 speaks plaintext HTTP and gRPC by design. For production, terminate TLS in a reverse proxy that also handles certificate renewal. The configs below have been verified end-to-end with `aws s3 ls` (HTTPS) and the `simple3 --grpc ls` client (HTTPS gRPC).

Assumptions throughout:

- simple3 runs on the same host as the proxy and binds the defaults: S3 HTTP on `127.0.0.1:8080`, gRPC on `127.0.0.1:50051`.
- The S3 API is served at `s3.example.com:443`.
- gRPC is served at `grpc.example.com:443` (gRPC needs HTTP/2 end-to-end; do not multiplex it onto the S3 hostname).

## nginx

`/etc/nginx/conf.d/simple3.conf`:

```nginx
# --- S3 over HTTPS ---
server {
    listen 443 ssl;
    http2 on;
    server_name s3.example.com;

    ssl_certificate     /etc/letsencrypt/live/s3.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/s3.example.com/privkey.pem;

    client_max_body_size 0;          # no body size cap; simple3 enforces its own limit
    proxy_request_buffering off;     # streaming PUT
    proxy_buffering off;             # streaming GET
    proxy_read_timeout 1h;
    proxy_send_timeout 1h;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $remote_addr;
        proxy_set_header X-Forwarded-Proto https;
    }
}

# --- gRPC over HTTPS (HTTP/2 backend required) ---
server {
    listen 443 ssl;
    http2 on;
    server_name grpc.example.com;

    ssl_certificate     /etc/letsencrypt/live/grpc.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/grpc.example.com/privkey.pem;

    grpc_read_timeout  1h;
    grpc_send_timeout  1h;
    client_max_body_size 0;

    location / {
        grpc_pass grpc://127.0.0.1:50051;
    }
}
```

Reload: `nginx -t && systemctl reload nginx`.

## Traefik (v3, file provider)

`/etc/traefik/traefik.yml`:

```yaml
entryPoints:
  websecure:
    address: ":443"
    http:
      tls:
        certResolver: letsencrypt

certificatesResolvers:
  letsencrypt:
    acme:
      email: ops@example.com
      storage: /var/lib/traefik/acme.json
      httpChallenge:
        entryPoint: web

providers:
  file:
    filename: /etc/traefik/dynamic.yml
```

`/etc/traefik/dynamic.yml`:

```yaml
http:
  routers:
    s3:
      rule: "Host(`s3.example.com`)"
      entryPoints: [websecure]
      service: simple3-s3
      tls:
        certResolver: letsencrypt

    grpc:
      rule: "Host(`grpc.example.com`)"
      entryPoints: [websecure]
      service: simple3-grpc
      tls:
        certResolver: letsencrypt

  services:
    simple3-s3:
      loadBalancer:
        servers:
          - url: "http://127.0.0.1:8080"
        passHostHeader: true

    simple3-grpc:
      loadBalancer:
        servers:
          - url: "h2c://127.0.0.1:50051"
        passHostHeader: true
```

`h2c://` is required so Traefik speaks HTTP/2 cleartext to the local gRPC port. Restart with `systemctl restart traefik`.

## Caddy

`/etc/caddy/Caddyfile`:

```caddy
s3.example.com {
    reverse_proxy 127.0.0.1:8080 {
        flush_interval -1            # disable response buffering for streaming GET
        transport http {
            read_buffer  16384
            write_buffer 16384
        }
    }
}

grpc.example.com {
    reverse_proxy 127.0.0.1:50051 {
        transport http {
            versions h2c
        }
    }
}
```

Reload: `caddy reload --config /etc/caddy/Caddyfile`.

## Verifying the path end-to-end

```bash
# S3 API
AWS_ACCESS_KEY_ID=AK AWS_SECRET_ACCESS_KEY=SK \
  aws --endpoint-url https://s3.example.com s3 ls

# gRPC
simple3 ls --grpc \
  --endpoint-url https://grpc.example.com \
  --access-key AK --secret-key SK
```

A 200 / non-error response confirms TLS termination, header rewriting, and upstream forwarding.

## Why no native TLS

Implementing a second TLS stack inside the binary doubles the surface for certificate handling, OCSP stapling, ALPN edge cases, and renewal. Production deployments almost always need a proxy regardless (load balancing, IP allowlists, request logging). Documenting the proxy path is sufficient until a real edge use case demands embedded TLS.
