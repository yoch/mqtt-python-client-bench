# Ceiling probes — broker vs client

Diagnostic runbook to push ingress offer past the default ~32k msgs/s and
separate **Mosquitto ceiling** from **SUT client ceiling**. These points are
`suite=full`, tagged `diagnostic`, and **non_comparable** (not ranking core).

## Piège 64k (double-comptage emqtt-bench QoS0)

**Ce n’était pas une offre réelle à 64k** — c’est un artefact de comptage.

Pour un publish QoS0 qui retourne `ok`, upstream `emqtt_bench.erl` :

1. `publish/2` fait `inc_counter(..., pub)` ;
2. `loop/5` refait `inc_counter(..., pub)` sur le même succès.

Donc chaque PUBLISH QoS0 est compté **deux fois** dans la série
`pub total=… rate=…`. La rate parsée ≈ **2 × taux réel**.

| Champ JSON | Signification |
|---|---|
| `loadgen.nominal_rate` | Offre configurée ≈ `clients × 1000 / interval_ms` |
| `loadgen.effective_offer_msgs_per_s` | **Référence d’offre** (= `nominal_rate` pour pub) |
| `loadgen.parsed_pub_rate_raw` / `parsed.median_rate` | Rate brute emqtt-bench (**ne pas comparer** en QoS0) |
| `loadgen.observed_pub_rate` | Rate corrigée (`raw / 2` en QoS0) |
| `loadgen.qos0_pub_counter_double_count` | `true` quand la correction s’applique |

Avec `-c 32 -I 1` : offre réelle = **32k**, pas 64k. Un client à ~30.5k suit
déjà ~95 % de l’offre — il faut **monter le nominal** (plus de clients) pour
chercher un plafond plus haut. Ne pas élargir le cpuset broker (Mosquitto 2.0
= 1 thread).

## Préconditions

- Broker géré local (Mosquitto du repo, `sys_interval 1`).
- Image emqtt-bench dispo ; Docker host network.
- Extra `paho` installé (probe `$SYS`).
- Profil `smoke` pour itérer ; `standard` pour un verdict plus stable.

## Matrice

| Scénario | Topologie | Offre (`loadgen_clients` / target) | Primaire |
|---|---|---|---|
| `broker_ceiling_ingress` | `broker_ceiling` (emqtt pub + emqtt sub) | 32 / 64 / 128 → 32k / 64k / 128k | `recv` ref sub |
| `client_ceiling_ingress` | `subscriber_ingress` + `--client` | même grille | delivered SUT |

Publish capacity reste couverte par `pub_qos_sweep_telemetry` (déjà SUT-limité).

## Commandes

```bash
# Plafond broker (pas de SUT Python — --client ignoré côté workers)
mqtt-client-bench run \
  --suite full \
  --scenario broker_ceiling_ingress \
  --profile smoke \
  --client paho \
  --output results/broker-ceiling-smoke.json

# Plafond client (substituer gmqtt / awscrt / …)
mqtt-client-bench run \
  --suite full \
  --scenario client_ceiling_ingress \
  --profile smoke \
  --client gmqtt \
  --output results/client-ceiling-gmqtt-smoke.json
```

Un seul cran d’offre :

```bash
# Via le catalogue : les variants fixent ingress_target_msgs_per_s = clients×1000
# Filtrer après coup sur point.loadgen_clients dans le JSON, ou relancer en
# éditant temporairement les variants du scénario.
```

## Lecture

1. **Offre** = `effective_offer_msgs_per_s` / `nominal_rate` — jamais `parsed.median_rate` QoS0.
2. **Délivré** = `primary_msgs_per_s` (SUT ou `loadgen_ref_sub.observed_recv_rate`).
3. **Ratio** = `delivery_offer_ratio` (délivré / offre).
4. **`$SYS`** = `sys_counters.dropped_delta` (et sent/received) sur la fenêtre de mesure.
5. **CPU** = `telemetry` containers Mosquitto + processus SUT.

## Verdicts

| Verdict | Critères typiques |
|---|---|
| **VERIFIED broker ceiling** | Sur `broker_ceiling_ingress`, recv plafonne alors que l’offre monte (64k→128k) ; et/ou `dropped_delta` matériel ; bottleneck `broker_limited`. |
| **VERIFIED client ceiling** | Sur `client_ceiling_ingress`, le SUT plafonne **sous** le recv de `broker_ceiling` à la même offre ; drops `$SYS` faibles ; bottleneck `sut_limited`. |
| **offer_limited** | Délivré ≥ ~90 % de l’offre effective — monter `loadgen_clients` avant de conclure. |
| **INCONCLUSIVE** | Loadgen &lt; moitié de l’offre, barrier/worker errors, probe `$SYS` absente, ou signaux contradictoires. |

Hors scope : changer le cpuset broker, remplacer Mosquitto, inclure ces points dans le ranking core.

## Gate NanoMQ (2026-07-14) — NO-GO

Hypothèse testée : un broker multi-thread (NanoMQ 0.22.10) sur le même cpuset
`1,5` lèverait le plafond Mosquitto mono-thread (surtout `blob1m` /
`container_cpu_high`).

Setup gate (fair) :
- Mosquitto compose du repo, port `11883`, cpuset `1,5`
- NanoMQ `emqx/nanomq:0.22.10`, `--network host`, port `21883`,
  `-t 2 -T 2 -n 8` (2 taskq threads, parallel=8)
- emqtt-bench pub + sub, parse **recv** (pas le compteur pub QoS0 ×2)

| Offre / payload | Mosquitto recv med | NanoMQ recv med |
|---|---|---|
| 32k · 256 B QoS0 | ~33k | ~2.6k |
| 64k · 256 B QoS0 | ~29k (plafond) | ~2.6k |
| 128k · 256 B QoS0 | ~25k | ~2.4k |
| blob1m · c=4 | ~127 | ~130 |
| blob1m · c=8 | ~169 | ~133 |

Critère GO du plan (≥ 1.5× Mosquitto sur plafond 256 B **ou** blob1m) :
**non atteint**. NanoMQ n’améliore pas le débit livré sous ce harness (souvent
pire en fan-out 256 B ; blob1m au mieux à égalité, CPU Nano ~2× plus haut).

**Conséquence** : pas de profil broker `nanomq` dans le code. Les trous
ranking `container_cpu_high` / `broker_limited` restent un plafond Mosquitto
structurel ; les distinguer via `broker_ceiling_ingress` /
`client_ceiling_ingress` (ce document), pas via un swap de broker.

Raw : `logs/gate-nanomq-2t-summary.txt`.

## Analyse : pourquoi NanoMQ « perd » ici alors que les benches publics le placent devant

Les chiffres publics ([EMQ Mosquitto vs NanoMQ](https://www.emqx.com/en/blog/open-mqtt-benchmarking-comparison-mosquitto-vs-nanomq)) ne mesurent **pas** la même chose que notre gate.

### Écart de méthodo

| Dimension | Benches publics EMQ | Notre gate / `broker_ceiling_ingress` |
|---|---|---|
| Hardware | AWS c5.4xlarge **16 vCPU** | Host local i7-3770, broker pin **2 CPU** (`1,5`) |
| Outil | XMeter (JMeter) | emqtt-bench |
| Scénario phare NanoMQ | **Fan-out** 5 pubs → 1000 subs (egress 250k) | **Fan-in** N pubs → **1** sub ref |
| Fan-in public | Shared sub + **500** consumers | 1 consumer exact topic |
| Payload | 16 B QoS1 | 256 B / 1 MiB QoS0 |
| Charge « enterprise » | Dizaines de k connexions / topics | ~32–128 clients loadgen |

Sur le basic set public, Mosquitto et NanoMQ sont **similaires**. NanoMQ tire son épingle du jeu quand le multi-cœur parallélise beaucoup de sessions sortantes (fan-out) ou beaucoup de paires pub/sub (p2p 50k topics).

### Diagnostics locaux (2026-07-14, host-net, NanoMQ `-S 65535`)

Fan-in 32 pubs → 1 sub, 256 B QoS0 (topologie gate) :

| Broker | recv med |
|---|---|
| Mosquitto | ~30–32k (suit l’offre) |
| NanoMQ 2 threads (cpuset 1,5) | ~4.5–5.5k |
| NanoMQ 8 threads (unpinned) | ~1.7–5.3k |

Même constat avec un subscriber **Paho** Python (plus lent, mais même ordre) : Mosquitto ~7.7k > NanoMQ wide ~6.5k > NanoMQ 2t ~4.8k. Ce n’est donc pas seulement `emqtt-bench -A once`.

À **faible** fan-in (4 pubs → 1 sub, offre 4k) : les trois brokers livrent ~4k — pas de régression NanoMQ.

Fan-out 8 pubs × 100 subs, 16 B (proche de l’esprit public) :

| Broker | recv agrégé med | vs Mosq |
|---|---|---|
| Mosquitto | ~65k | — |
| NanoMQ 2t | ~85k | **+31 %** |
| NanoMQ 8t | ~56k | −14 % (contention sur ce CPU) |

→ Sur **fan-out**, NanoMQ 2t **bat** Mosquitto, cohérent avec le narratif public. Notre critère GO (1.5× sur le plafond **fan-in 1-sub**) était mal aligné avec là où NanoMQ excelle.

### Ce qui bloque concrètement

1. **Topologie** : le plafond utile du harness (`broker_ceiling_ingress`, beaucoup d’ingress vers une ref sub) stress le chemin **many writers → one session queue**. Mosquitto mono-thread y est efficace ; le modèle actor NanoMQ sérialise / contensionne davantage vers un seul subscriber.
2. **Hardware / pin** : 2 cœurs ne reproduisent pas le gain 16 vCPU des benches EMQ ; plus de threads NanoMQ n’a pas sauvé le fan-in 1-sub et a parfois nui.
3. **Métrique gate** : on jugeait le **recv d’un seul sub**, pas l’egress fan-out multiplié — métrique défavorable à NanoMQ et peu représentative des pubs marketing.
4. **blob1m** : plafonds proches (~130–170 msg/s) — bound mémoire/CPU payload, pas le multi-thread messaging.

### Implications pour le bench client

- Un swap NanoMQ **n’aurait pas** levé les `container_cpu_high` / trous ranking mesurés en many-to-one / gros payload sous Mosquitto pin 2 CPU.
- Si on veut un broker « ceiling » pour pousser les clients plus loin, il faudrait soit un scénario **fan-out / shared-sub** (où NanoMQ aide), soit accepter que le plafond Mosquitto many-to-one est le référentiel réaliste du harness actuel.
- Les benches publics restent crédibles dans **leur** cadre ; ils ne contredisent pas le NO-GO du plan une fois la topologie alignée.
