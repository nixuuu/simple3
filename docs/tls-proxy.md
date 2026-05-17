# TLS via reverse proxy

simple3 speaks plaintext HTTP and gRPC by design. For production, terminate TLS in a reverse proxy that also handles certificate renewal. The configs below are derived from a local end-to-end run against the nginx variant — see [End-to-end verification](#end-to-end-verification) for the trace. Traefik and Caddy are the same routing pattern with different syntax; the gotcha (SigV4 Host header preservation) applies to all three.

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
        # SigV4 signs the Host header verbatim INCLUDING the port. nginx's
        # $host variable strips the port — use $http_host to forward the
        # exact header the client signed, or signature verification fails.
        proxy_set_header Host              $http_host;
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

## End-to-end verification

The S3-over-HTTPS path was verified locally with the nginx config above against
a real simple3 binary. Steps:

```bash
# 1. Self-signed cert for localhost
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 1 -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# 2. simple3 on host:18080, plaintext
simple3 --data-dir /tmp/tls-data serve --host 127.0.0.1 --port 18080 --grpc-port 0

# 3. nginx in a container, listening on host:18443, proxying to host:18080.
#    Mount cert.pem / key.pem and the nginx.conf above (with proxy_pass
#    rewritten to http://host.docker.internal:18080).
docker run -d --rm -p 18443:443 \
  -v /tmp/tls/cert.pem:/etc/nginx/certs/cert.pem:ro \
  -v /tmp/tls/key.pem:/etc/nginx/certs/key.pem:ro \
  -v /tmp/tls/nginx.conf:/etc/nginx/nginx.conf:ro \
  nginx:1.27-alpine

# 4. AWS CLI over HTTPS
export AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK
export AWS_CA_BUNDLE=/tmp/tls/cert.pem
aws --endpoint-url https://localhost:18443 s3 ls
# → 1970-01-01 01:00:00 tls-bucket

aws --endpoint-url https://localhost:18443 s3 cp ./payload.txt s3://tls-bucket/payload.txt
aws --endpoint-url https://localhost:18443 s3 cp s3://tls-bucket/payload.txt ./payload.dl
diff ./payload.txt ./payload.dl
# (no output → byte-identical)
```

The gRPC variant follows the same pattern (HTTP/2 backend, `h2c` upstream). It
was not run end-to-end locally; only validate the Host-header rule (SigV4 doesn't
apply to gRPC) and the HTTP/2-cleartext backend setting.

If the AWS CLI returns `SignatureDoesNotMatch`, the proxy is rewriting the Host
header. Check that the config uses `$http_host` (nginx), `passHostHeader: true`
(Traefik), or default behavior (Caddy preserves the Host).

## Why no native TLS

Implementing a second TLS stack inside the binary doubles the surface for certificate handling, OCSP stapling, ALPN edge cases, and renewal. Production deployments almost always need a proxy regardless (load balancing, IP allowlists, request logging). Documenting the proxy path is sufficient until a real edge use case demands embedded TLS.
