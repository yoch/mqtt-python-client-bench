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
