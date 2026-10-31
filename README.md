# multi-net

Ein gemeinsamer Reverse Proxy für **mehrere Management-Server**, so wie Relay und STUN schon heute
von mehreren Managements gemeinsam genutzt werden.

> Fork von [netbirdio/netbird](https://github.com/netbirdio/netbird). Abgesehen von den unten
> genannten Dateien entspricht der Code dem Upstream-Projekt.

## Bestandteile

| Pfad | Was | Lizenz |
|------|-----|--------|
| `multi/mux/` | **mgmt-mux**: gibt sich gegenüber dem Proxy als *ein* Management aus und leitet an beliebig viele Managements weiter | eigener Code, siehe [Lizenz](#lizenz) |
| `proxy/internal/roundtrip/netbird.go` | ~25 Zeilen Änderung am Proxy | AGPL-3.0 |
| `multi/docker/` | Dockerfile für das Proxy-Image | – |
| `multi/deploy/` | docker-compose-Beispiel | – |
| `.github/workflows/multi-*.yml` | täglicher Upstream-Sync und Image-Build | – |

## Funktionsweise

```
                        ┌──────────────┐  Token A   ┌──────────────┐
Internet ──► Proxy ───► │  mgmt-mux    │ ─────────► │ Management A │
   :443                 │              │  Token B   ├──────────────┤
                        │ merged state │ ─────────► │ Management B │
                        └──────────────┘            └──────────────┘
   eingebetteter Client je Account ──────────────►  Management des Accounts
```

- **Mappings:** mgmt-mux hält zu jedem Management einen eigenen `SyncMappings`-Stream (Fallback
  `GetMappingUpdate`) mit dessen Proxy-Token und führt alle Services zu einem Gesamtzustand
  zusammen. Der Proxy bekommt einen gemeinsamen Snapshot und danach nur noch Änderungen.
- **Resync eines Managements:** Nach einem Reconnect berechnet mgmt-mux selbst, was sich geändert
  hat. Entfernt werden nur Services *dieses* Managements, unveränderte Services werden nicht neu
  gesendet.
- **Ausfall eines Managements:** Dessen Services bleiben aktiv, bis es wieder synchronisiert ist.
- **Routing:** Jeder Aufruf wird über `account_id` bzw. die Domain dem richtigen Management
  zugeordnet. Das gilt für `Authenticate`, `SendAccessLog`, `SendStatusUpdate`, `CreateProxyPeer`,
  `GetOIDCURL`, `ValidateSession`, `ValidateTunnelPeer` und die LLM-Aufrufe.
- **Eingebettete Clients:** Bei `CreateProxyPeer` schickt mgmt-mux den Header
  `x-nb-management-url` mit. Der Proxy verbindet den Client des Accounts dann mit diesem
  Management. Ohne den Header verhält sich der Proxy wie im Original.
- **Konflikte:** Belegt ein Management eine Domain, Service-ID oder Account-ID, die schon ein
  anderes Management belegt, wird das Mapping abgelehnt. Das steht dann im Log und im `/status`.

## Voraussetzungen auf den Managements

Die Einrichtung ist bei jedem Management dieselbe wie für einen einzelnen Proxy:

1. Ein Proxy-Token erzeugen (`token create --name shared-proxy` im Admin-Kommando der Installation).
2. Die Proxy-Domain (`NB_PROXY_DOMAIN`, z. B. `proxy.example.com`) ist überall dieselbe, und die
   Service-Domains zeigen per CNAME darauf.
3. Jede Domain darf nur in einem Management vergeben sein.

## Images

Die Images werden automatisch gebaut und in die GitHub Container Registry gepusht:

| Image | Inhalt |
|---|---|
| `ghcr.io/tim-riep/multi-proxy:<version>` | Proxy mit der Änderung aus diesem Fork |
| `ghcr.io/tim-riep/mgmt-mux:<version>` | Multiplexer |

`<version>` ist das Upstream-Release mit Suffix `-multi`, z. B. `v0.82.0-multi`. Daneben gibt es
`latest`. Beide Images gibt es für `linux/amd64` und `linux/arm64`.

Lokal bauen:

```bash
docker build -f multi/docker/proxy.Dockerfile -t multi-proxy .
```

```bash
docker build -t mgmt-mux multi/mux
```

## Automatisierung

**`multi-sync.yml`** läuft jeden Morgen um 04:00 UTC und lässt sich auch von Hand starten:

1. Merged `upstream/main` in `main`. Bei Konflikten bricht der Lauf ab, und GitHub meldet den
   Fehlschlag per Mail. Dieses README bleibt bei Konflikten erhalten (`.gitattributes`).
2. Baut den Proxy und testet ihn sowie mgmt-mux, dann wird gepusht.
3. Gibt es ein neues Upstream-Release, legt der Workflow den Tag `<release>-multi` an. Der Tag
   enthält genau das Release plus die eigenen Commits dieses Forks und startet damit den Build.
4. Deaktiviert vor dem Push alle Upstream-Workflows, denn jeder Push auf `main` würde sie sonst
   starten. Ein Workflow, den Upstream neu hinzufügt, kann ein einziges Mal anlaufen, bevor er
   nach dem Push deaktiviert wird.

**`multi-build.yml`** baut beide Images für jeden `v*-multi`-Tag. Von Hand lässt er sich mit
beliebigem Ref starten.

Einmalig einzurichten ist das Secret **`SYNC_TOKEN`**. Das ist ein Fine-grained Token, nur für
dieses Repository, mit den Rechten *Contents: Read and write* und *Workflows: Read and write*.
Der Standard-Token von Actions reicht nicht aus. Er darf keine Upstream-Änderungen an
`.github/workflows` pushen, und seine Pushes würden den Build nicht auslösen.

```bash
gh secret set SYNC_TOKEN --repo tim-riep/multi-net
```

## Betrieb

`multi/mux/config.example.yaml` zeigt alle Optionen. `${VAR}` wird aus der Umgebung ersetzt,
damit Tokens nicht in der Datei stehen müssen.

Proxy-Einstellungen:

| Einstellung | Wert |
|---|---|
| `NB_PROXY_MANAGEMENT_ADDRESS` | `http://mgmt-mux:50051`, mit TLS am Multiplexer `https://…` |
| `NB_PROXY_TOKEN` | gleicher Wert wie `proxy_token` des Multiplexers |
| `NB_PROXY_ALLOW_INSECURE=true` | nur ohne TLS zwischen Proxy und Multiplexer (lokal oder im internen Netz) |

Die Statusseite (`status_listen`) liefert unter `GET /status` je Management `connected`,
`synced`, die Zahl der Services, abgelehnte Mappings und den letzten Fehler.

Ein vollständiges Beispiel liegt in `multi/deploy/docker-compose.yml`. Vorher
`cp multi/deploy/mux.env.example multi/deploy/mux.env` ausführen und die Datei befüllen.

## Grenzen

- **Ein Proxy pro Multiplexer.** Eine neue Proxy-Verbindung löst die alte ab. Bei mehreren
  Proxy-Instanzen bekommt jede ihren eigenen Multiplexer, zum Beispiel als Sidecar.
- **Kein persistenter Zustand.** Startet der Multiplexer neu, während ein Management länger als
  `initial_sync_timeout` nicht erreichbar ist, fehlen dessen Services im ersten Snapshot. Sie
  kommen automatisch zurück, sobald das Management wieder erreichbar ist.
- **Neue RPCs künftiger Upstream-Versionen** beantwortet der Multiplexer mit `Unimplemented`,
  bis sie in `multi/mux/internal/mux/server.go` ergänzt sind. Neue Felder in den Mappings
  werden unverändert durchgereicht.

## Tests

```bash
cd multi/mux && go test -race ./...
```

Das sind Unit-Tests und ein End-to-End-Test mit zwei Fake-Managements, eines davon nur mit dem
alten `GetMappingUpdate`. Optional gibt es einen Smoke-Test gegen ein echtes Proxy-Binary:

```bash
SMOKE_PROXY_BIN=/pfad/proxy SMOKE_CERT_DIR=/dir/mit/tls.crt+tls.key go test -run Smoke ./internal/mux/
```

## Lizenz

- **Proxy (`proxy/`)** steht unter AGPL-3.0 (`proxy/LICENSE`). Wer den Proxy Nutzern übers Netz
  anbietet, muss ihnen den Quellcode der geänderten Version anbieten (AGPL §13). Dieses öffentliche
  Repo erfüllt das, am besten verlinkst du es in der Doku deines Dienstes. Jeder `-multi`-Tag
  verweist auf genau den Quellcode des passenden Images.
- **mgmt-mux** bindet nur die gRPC-Definitionen aus `shared/management/proto` ein. Diese stehen
  unter BSD-3. Bei Weitergabe von Binaries muss der BSD-Hinweis mitgeliefert werden. Dafür liegt
  `multi/mux/NOTICE` bei, das auch ins Image kopiert wird.
- **Alles Übrige** steht unter den Lizenzen des Upstream-Projekts, siehe `LICENSE` und `LICENSES/`.
- Die Namen und Logos des Upstream-Projekts sind Marken ihrer Inhaber. Dieser Fork wird nicht von
  ihnen herausgegeben und ist nicht mit ihnen verbunden.
