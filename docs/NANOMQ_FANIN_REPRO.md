# Rapport de repro : NanoMQ s’écroule en fan-in N→1 (vs Mosquitto)

Date : 2026-07-14  
Host : Intel i7-3770 (4 cœurs / 8 threads HT), Linux  
Objectif : comprendre pourquoi NanoMQ (benches publics > Mosquitto) livre beaucoup
moins qu’un Mosquitto mono-thread sur **notre** topologie plafond.

## Résumé

Sous emqtt-bench **pub N clients → 1 sub**, même topic, QoS0, payload 256 B,
broker pin sur **2 cœurs physiques** (`0,1`, pas une paire HT) :

- jusqu’à **~24k msg/s** d’offre, NanoMQ ≈ Mosquitto (ratio recv/offre ≈ 1) ;
- à **32k** (`-c 32 -I 1`), Mosquitto tient ~32k recv ; **NanoMQ s’effondre**
  (~2.5k–15k selon version/conf, souvent &lt; 10 % de l’offre) alors que le
  **pub** NanoMQ continue d’accepter ~32k (ingestion OK, livraison KO) ;
- en **fan-out** (8 pubs × 100 subs, 16 B), NanoMQ est à égalité ou légèrement
  devant — cohérent avec les pubs EMQ, **pas** avec notre plafond harness.

Hypothèse principale (alignée discussions upstream NanoMQ) : en multi-thread,
NanoMQ **absorbe** l’ingress plus vite que le drain d’**une** session sub, remplit
`max_mqueue_len`, puis **drop** ; Mosquitto couple mieux pub/sub sur un seul
thread et ne montre pas ce cliff à 32k dans le même setup.

## Versions

| Composant | Version / image |
|---|---|
| Mosquitto | `eclipse-mosquitto:2.0.20@sha256:21421af7b32bf9ce508e9090c8eb13bb81f410ca778dc205506180a6f862d0eb` |
| NanoMQ testé | `emqx/nanomq:0.22.10` et **`emqx/nanomq:0.25.2-2`** (plus récent Docker Hub) |
| Note | Tag demandé `0.25.3` : **inexistant** sur Docker Hub (`manifest unknown`). Dernier `0.25.x` vu : `0.25.2-2` (2026-06-30). |
| Loadgen | `emqx/emqtt-bench:latest@sha256:ae7f2d56cd49b14824c835140c808b093c5e3f2defb3a29b34b17560feb456cd` |
| Benches publics EMQ | NanoMQ **0.17.0**, Mosquitto 2.0.15, AWS c5.4xlarge **16 vCPU**, XMeter |

Binaire NanoMQ 0.25.2-2 : `NanoMQ Messaging Engine … v0.25.2-2`.

## Piège CPU (critique pour reproduire « en vrai »)

Sur cet host :

```
cpu0 siblings=0,4   # même cœur physique
cpu1 siblings=1,5
```

Le harness du repo assigne le broker à un **groupe physique** du type `0,4` =
**deux threads HT d’un seul cœur**. Pour NanoMQ multi-thread, préférer
**deux cœurs distincts** : `--cpuset-cpus 0,1` (pas `1,5` ni `0,4`).

Mesure de contrôle : pin HT `1,5` empire encore le fan-in NanoMQ ; passer à
`0,1` améliore un peu mais **ne supprime pas** le cliff à 32k.

## Comment lancer les deux brokers

Repo : `/home/yoch/mqtt-python-client-bench` (compose Mosquitto déjà en place).

### Mosquitto (ports bench `11883` / TLS `11884`)

```bash
cd /home/yoch/mqtt-python-client-bench
docker compose up -d mosquitto
# 2 cœurs physiques (pas HT) :
docker update --cpuset-cpus 0,1 "$(docker compose ps -q mosquitto)"
ss -ltn | grep 11883
```

Conf : [`mosquitto/mosquitto.conf`](../mosquitto/mosquitto.conf)
(`set_tcp_nodelay true`, `max_queued_messages 10000`, …).

### NanoMQ 0.25.2-2 (port `21883`)

Smoke officiel (écoute **1883** dans le bridge Docker) :

```bash
docker pull emqx/nanomq:0.25.2-2
docker rm -f nanomq 2>/dev/null
docker run -d --name nanomq emqx/nanomq:0.25.2-2
docker exec nanomq nanomq version
```

Banc comparable (host network, 2 cœurs physiques, listener dédié) :

```bash
mkdir -p /tmp/nanomq-repro
cat > /tmp/nanomq-repro/nanomq.conf <<'EOF'
system {
    num_taskq_thread = 2
    max_taskq_thread = 2
    parallel = 64
}
mqtt {
    property_size = 32
    max_packet_size = 256MB
    max_mqueue_len = 65535
    retry_interval = 10s
    keepalive_multiplier = 1.25
}
listeners.tcp {
    bind = "0.0.0.0:21883"
}
log {
    to = [console]
    level = warn
}
auth {
    allow_anonymous = true
    no_match = allow
    deny_action = ignore
}
EOF

docker rm -f nanomq 2>/dev/null
docker run -d --name nanomq --network host --cpuset-cpus 0,1 \
  -v /tmp/nanomq-repro/nanomq.conf:/opt/nanomq.conf:ro \
  --entrypoint nanomq emqx/nanomq:0.25.2-2 \
  start --conf /opt/nanomq.conf -S 65535 --log_level warn

ss -ltn | grep 21883
docker exec nanomq nanomq version
```

> Monter vers `/etc/nanomq.conf` a échoué sur 0.22 (conflit fichier/dir) ;
> `--conf /opt/…` évite ça. Warning « websocket config failed » possible si la
> conf minimale omet `listeners.ws` — le TCP `21883` démarre quand même.

## Comment lancer le loadgen (les 2 scénarios clés)

Variable image (depuis le venv du repo) :

```bash
cd /home/yoch/mqtt-python-client-bench
IMG=$(.venv/bin/python -c "from mqtt_client_bench.broker import EMQTT_BENCH_IMAGE; print(EMQTT_BENCH_IMAGE)")
```

**Important** :

- Offre ≈ `clients × 1000 / interval_ms` (`-I 1` → 1000 msg/s **par** client).
- En QoS0, le compteur `pub` emqtt-bench est **×2** : pour le débit réel pub,
  diviser par 2. Le **`recv`** n’a pas ce biais — s’y fier pour comparer.
- `-A true` sur sub (et pub) : mode socket « haute fréquence » (défaut `once`
  défavorise un sub saturé).

### A — Fan-in cliff (N pubs → 1 sub) — **scénario du bug**

```bash
run_fanin() {
  local name=$1 port=$2 pubs=$3
  local topic="repro/fanin/${name}/c${pubs}/$RANDOM"
  timeout 12s docker run --rm --network host "$IMG" \
    sub -h 127.0.0.1 -p "$port" -c 1 -t "$topic" -q 0 -V 5 -A true \
    | tee "/tmp/repro-${name}-c${pubs}-sub.log" &
  local sp=$!
  sleep 2
  timeout 8s docker run --rm --network host "$IMG" \
    pub -h 127.0.0.1 -p "$port" -c "$pubs" -I 1 -s 256 -q 0 -t "$topic" -V 5 -F 100 -A true \
    | tee "/tmp/repro-${name}-c${pubs}-pub.log" || true
  kill $sp 2>/dev/null; wait $sp 2>/dev/null
}

# Mosquitto :11883 — NanoMQ :21883
for c in 16 24 32; do
  run_fanin mosq 11883 $c
  run_fanin nano 21883 $c
done
```

Parser rapide (médiane 2e moitié des samples `recv`) :

```bash
python3 - <<'PY'
import re, statistics, pathlib, sys
for p in sorted(pathlib.Path('/tmp').glob('repro-*-sub.log')):
    rs=[float(m.group(1)) for m in re.finditer(r'\d+s recv total=\d+ rate=([0-9.]+)', p.read_text(errors='replace'))]
    if not rs: print(p.name, 'no recv'); continue
    t=rs[len(rs)//2:]
    print(p.name, 'recv_med', round(statistics.median(t),1), 'n', len(rs))
PY
```

### B — Fan-out (contrôle « NanoMQ devrait gagner »)

```bash
run_fanout() {
  local name=$1 port=$2
  local topic="repro/fanout/${name}/$RANDOM"
  timeout 12s docker run --rm --network host "$IMG" \
    sub -h 127.0.0.1 -p "$port" -c 100 -t "$topic" -q 0 -V 5 -A true \
    | tee "/tmp/repro-fo-${name}-sub.log" &
  local sp=$!
  sleep 2
  timeout 8s docker run --rm --network host "$IMG" \
    pub -h 127.0.0.1 -p "$port" -c 8 -I 1 -s 16 -q 0 -t "$topic" -V 5 -F 100 -A true \
    | tee "/tmp/repro-fo-${name}-pub.log" || true
  kill $sp 2>/dev/null; wait $sp 2>/dev/null
}
run_fanout mosq 11883
run_fanout nano 21883
```

`recv` agrégé attendu ≫ offre pub (théorique 8k×100 = 800k ; on mesure souvent
60–85k selon broker/CPU).

## Comparaison clé (mesures 2026-07-14)

Setup commun sauf mention : host-net, cpuset broker **`0,1`**, emqtt-bench
`-A true`, QoS0.

### NanoMQ 0.25.2-2 vs Mosquitto 2.0.20

| Tag | Offre | Mosquitto recv med | NanoMQ 0.25.2-2 recv med | Ratio Nano |
|---|---|---|---|---|
| Fan-in 16→1, 256 B | 16k | ~15999 | ~15992 | ~1.00 |
| Fan-in 24→1, 256 B | 24k | ~23992 | ~23415 | ~0.98 |
| Fan-in 32→1, 256 B | 32k | ~31988 | **~2518** | **~0.08** |
| Fan-out 8×100, 16 B | 8k ingress | ~65310 | ~65682 | ~1.00 |
| blob1m 4→1 | 4k nominal | ~135 | ~114 | ~0.85 |

À 32k fan-in, le **pub** NanoMQ reste ~32k (corrigé) : le broker **accepte**
toujours ; seul le **recv** d’un sub unique s’écroule.

### Contrôles utiles (0.22.10 / configs)

| Observation | Détail |
|---|---|
| Fan-in 8→1 / 16→1 | NanoMQ = Mosquitto (~8k / ~16k) |
| Cliff | Entre **24k** (OK) et **32k** (collapse) |
| Pin HT `1,5` | Pire que `0,1` pour NanoMQ |
| 4 taskq / 4 cœurs | N’a **pas** sauvé le fan-in 32→1 (souvent pire) |
| `max_mqueue_len=65535` | Peut remonter un peu le recv à 32k (ex. ~15k sur un run 0.22) mais **pas** au niveau Mosquitto |
| Sub Paho Python | Même ordre : Mosquitto &gt; NanoMQ en 32→1 (sub Python plus lent que emqtt-bench) |
| QoS1 32→1 | Les deux souffrent ; Mosquitto reste devant sur recv |

### Écart vs benches publics EMQ

Les pubs « NanoMQ ≫ Mosquitto » ([blog EMQ](https://www.emqx.com/en/blog/open-mqtt-benchmarking-comparison-mosquitto-vs-nanomq))
mesurent surtout :

- **fan-out** 5 pubs → 1000 subs (egress 250k) ;
- fan-in via **shared subscription** + centaines de consumers ;
- p2p 50k topics ;

sur **16 vCPU**, outil XMeter — pas « 32 publishers → 1 subscriber exact topic »
sur 2 cœurs.

## Pistes d’investigation (pour plancher)

1. **Drops session queue** : confirmer côté NanoMQ (logs `msg drop` / métriques
   HTTP si activées) quand recv s’écroule alors que pub rate tient.
2. **Backpressure** : à QoS0, NanoMQ n’ralentit pas les pubs → file puis drop
   ([discussion #1544](https://github.com/nanomq/nanomq/discussions/1544),
   [issue #1739](https://github.com/nanomq/nanomq/issues/1739)). Mosquitto se
   comporte autrement sous le même client.
3. **Un seul pipe sub** : le modèle actor parallélise mal le drain vers une
   unique session ; augmenter les threads n’aide pas (voire nuit).
4. **Conf** : jouer `max_mqueue_len`, `parallel`, `num_taskq_thread` ; éviter
   `max_packet_size=260MB` (Paho refuse CONNACK : borne MQTT property
   268435455 ≈ 256 MiB).
5. **Harness repo** : `allocate_cpuset` donne au broker un groupe SMT
   (`0,4`) — OK pour Mosquitto 1 thread ; trompeur pour tout broker multi-thread.

## Lien avec le bench client

Le plafond `broker_ceiling_ingress` du repo = topologie **fan-in → 1 ref sub**.
Un swap NanoMQ **ne lève pas** les gates `container_cpu_high` / trous ranking
dans ce cadre. Voir aussi [`CEILING_PROBES.md`](CEILING_PROBES.md).

## Fichiers / artefacts locaux

- Confs d’essai : `.nanomq-probe/` (gitignored si présent)
- Anciens raw : `logs/gate-nanomq-2t-summary.txt` (gitignored `logs/`)
- Mosquitto compose : [`docker-compose.yml`](../docker-compose.yml)
