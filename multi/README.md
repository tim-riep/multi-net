# Geteilter Reverse Proxy für mehrere NetBird-Managements

Ein einziger NetBird Reverse Proxy bedient Services aus **mehreren** Management-Servern,
so wie Relay und STUN schon heute gemeinsam genutzt werden.

Dieses Repo ist ein Fork von [netbirdio/netbird](https://github.com/netbirdio/netbird).
Gegenüber Upstream ändert er nur Folgendes:

| Pfad | Was | Lizenz |
|------|-----|--------|
| `multi/mux/` | **mgmt-mux** – eigener Dienst, der sich gegenüber dem Proxy als *ein* Management ausgibt und an N Managements weiterleitet | eigener Code (Lizenz noch festlegen, siehe unten) |
| `proxy/internal/roundtrip/netbird.go` | ~25 Zeilen Änderung am Proxy (eigener Commit) | AGPL-3.0 (wie `proxy/` upstream) |
| `multi/deploy/` | docker-compose-Beispiel | – |

Basis: `netbirdio/netbird` Commit `d23f6315f9ca` (30.10.2026). Upstream-Updates holen:
`git fetch upstream && git merge upstream/main`.

## Funktionsweise

```
                        ┌──────────────┐  Token A   ┌──────────────┐
Internet ──► Proxy ───► │  mgmt-mux    │ ─────────► │ Management A │
   :443     (gepatcht)  │              │  Token B   ├──────────────┤
                        │ merged state │ ─────────► │ Management B │
                        └──────────────┘            └──────────────┘
   eingebetteter Client je Account ──────────────►  Management des Accounts
```

- **Mappings:** mgmt-mux hält zu jedem Management einen eigenen `SyncMappings`-Stream (Fallback
  `GetMappingUpdate`) mit dessen Proxy-Token und führt die Services zu einem Gesamtzustand zusammen.
  Der Proxy bekommt einen gemeinsamen Snapshot und danach nur noch Änderungen.
- **Resync eines Managements:** Nach einem Reconnect berechnet mgmt-mux selbst, was sich geändert hat.
  Entfernt werden nur Services *dieses* Managements, unveränderte Services werden nicht neu gesendet.
- **Ausfall eines Managements:** Dessen Services bleiben im Proxy aktiv, bis es wieder synchronisiert ist.
- **Routing:** Jeder Aufruf wird über `account_id` bzw. die Domain dem richtigen Management zugeordnet.
  Das gilt für `Authenticate`, `SendAccessLog`, `SendStatusUpdate`, `CreateProxyPeer`, `GetOIDCURL`,
  `ValidateSession`, `ValidateTunnelPeer` und die LLM-Aufrufe.
- **Eingebettete Clients:** Bei `CreateProxyPeer` schickt mgmt-mux den Header `x-nb-management-url` mit.
  Der Patch sorgt dafür, dass der Proxy den Client des Accounts dann mit diesem Management verbindet.
  Ohne den Header verhält sich der gepatchte Proxy exakt wie das Original.
- **Konflikte:** Belegt ein Management eine Domain, Service-ID oder Account-ID, die schon ein anderes
  Management belegt, wird das Mapping abgelehnt. Das steht dann im Log und im `/status`.

## Voraussetzungen auf den Managements

Die Einrichtung ist bei jedem Management dieselbe wie für einen einzelnen Proxy:

1. Ein Proxy-Token erzeugen (`token create --name shared-proxy` im Admin-Kommando eurer Installation).
2. Die Proxy-Domain (`--domain` / `NB_PROXY_DOMAIN`, z. B. `proxy.example.com`) ist überall dieselbe,
   und die Service-Domains zeigen per CNAME darauf.
3. Jede Domain darf nur in einem Management vergeben sein.

## Bauen

Aus dem Hauptverzeichnis des Repos:

```bash
docker build -f proxy/Dockerfile.multistage -t multi-proxy:latest .
```

```bash
docker build -t mgmt-mux:latest multi/mux
```

## Betrieb

`multi/mux/config.example.yaml` zeigt alle Optionen. `${VAR}` wird aus der Umgebung ersetzt, damit Tokens
nicht in der Datei stehen müssen. Proxy-Seite:

| Proxy-Einstellung | Wert |
|---|---|
| `NB_PROXY_MANAGEMENT_ADDRESS` | `http://mgmt-mux:50051`, mit TLS am Multiplexer `https://…` |
| `NB_PROXY_TOKEN` | = `proxy_token` des Multiplexers |
| `NB_PROXY_ALLOW_INSECURE=true` | nur ohne TLS zwischen Proxy und Multiplexer (lokal oder im internen Netz) |

Die Statusseite (`status_listen`) liefert unter `GET /status` für jedes Management:
`connected`, `synced`, Anzahl Services, abgelehnte Mappings und den letzten Fehler.

Ein vollständiges Beispiel liegt in `multi/deploy/docker-compose.yml`. Vorher
`cp multi/deploy/mux.env.example multi/deploy/mux.env` ausführen und die Datei befüllen.

## Grenzen

- **Ein Proxy pro Multiplexer.** Eine neue Proxy-Verbindung löst die alte ab. Bei mehreren
  Proxy-Instanzen bekommt jede ihren eigenen Multiplexer, etwa als Sidecar.
- **Kein persistenter Zustand.** Ist ein Management beim Neustart des Multiplexers länger als
  `initial_sync_timeout` nicht erreichbar, fehlen dessen Services im ersten Snapshot. Der Proxy baut
  sie dann ab. Sobald das Management erreichbar ist, kommen sie automatisch wieder.
- **Neue RPCs aus künftigen netbird-Versionen** beantwortet der Multiplexer mit `Unimplemented`, bis
  sie in `multi/mux/internal/mux/server.go` ergänzt sind. Bei Updates daher `proxy_service.proto` vergleichen.

## Tests

```bash
cd multi/mux && go test -race ./...
```

Das sind Unit-Tests und ein End-to-End-Test mit zwei Fake-Managements (eines davon nur mit dem alten
`GetMappingUpdate`). Optional gibt es einen Smoke-Test gegen das echte, gepatchte Proxy-Binary:

```bash
SMOKE_PROXY_BIN=/pfad/netbird-proxy SMOKE_CERT_DIR=/dir/mit/tls.crt+tls.key go test -run Smoke ./internal/mux/
```

## Lizenz

Upstream widerspricht sich selbst. Das `LICENSE` im Hauptverzeichnis nennt BSD-3 für alles außer
`management/`, `signal/`, `relay/` und `combined/`. In `proxy/LICENSE` liegt aber die **AGPL-3.0**.
Hier wird der Proxy deshalb als AGPL-3.0 behandelt.

- **Gepatchter Proxy (AGPL-3.0):** Wer ihn Nutzern übers Netz anbietet (Login-Seiten, Weiterleitung),
  muss ihnen den Quellcode der geänderten Version anbieten (AGPL §13). Dieses öffentliche Repo
  erfüllt das. Am besten verlinkst du es in der Doku deines Dienstes.
  Lizenz- und Copyright-Hinweise bleiben erhalten. Die Änderung ist im Commit und im Code mit
  Datum markiert (AGPL §5a).
- **mgmt-mux:** Das ist eigener Code. Er bindet nur die gRPC-Definitionen aus
  `shared/management/proto` ein, und die stehen unter **BSD-3**. Daher ist kein AGPL-Code enthalten,
  und die Lizenz ist frei wählbar. Ohne eigene `LICENSE` in `multi/mux/` greift formal der
  Repo-Default BSD-3 aus dem Hauptverzeichnis, dort aber mit NetBird als Copyright-Inhaber.
  Besser ist eine eigene `LICENSE` in `multi/mux/`.
  Bei Weitergabe von Binaries muss der BSD-Hinweis mitgeliefert werden. Das erledigt `multi/mux/NOTICE`,
  das auch ins Docker-Image kopiert wird.
- **Marke:** „NetBird“ und das Logo dürfen nicht als Name des Forks oder der Images verwendet werden
  (BSD-Klausel 3, Markenrecht). Die Images heißen deshalb `multi-proxy` und `mgmt-mux`. Vor einer
  Der Repo-Name `multi-net` ist unproblematisch.
- Das ist keine Rechtsberatung. Für den kommerziellen Einsatz empfiehlt sich eine kurze Rückfrage
  bei NetBird, ob `proxy/` wirklich unter AGPL stehen soll.
