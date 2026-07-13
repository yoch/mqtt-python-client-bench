# Conception des scénarios

Ce document décrit **ce que mesure chaque scénario**, comment le banc est câblé (topologie, cadence, métrique primaire), et ce qui est comparable ou non. La source de vérité du catalogue reste `src/mqtt_client_bench/scenarios.py` ; le harness est dans `harness.py`.

## Modèle de mesure (trois protocoles, jamais mélangés)

| Protocole | Question posée | Comment on charge | Métrique primaire |
|---|---|---|---|
| **Capacité** | Quel débit max le client tient ? | Closed-loop : fenêtre `outstanding` bornée, pas de pacing | `completed_success` / s dans `[T0, T1)` |
| **Latence** | Quelle latence à X % de *sa* capacité ? | Open-loop à fractions calibrées (`load_fraction`) | Distribution de latence (PUBACK ou RTT appli) |
| **Intégrité** | Manque / doublon / désordre ? | Débit borné + en-tête séquence | Compteurs d’intégrité (pas un ranking de débit) |

Calibration (`calibrate`) : pour chaque client et **chaque protocole MQTT supporté**, on mesure une capacité publish QoS1 et une capacité RTT, stockées dans `protocol_capacities`. Les scénarios open-loop dérivent `target_rate = capacity[protocol] × load_fraction`. Sans profil calibré compatible (même client / version / protocole), les points à fraction sont refusés.

Profils temporels (`PROFILE_SPECS`) :

| Profil | Mesure / warmup / drain | Runs | Comparable |
|---|---|---|---|
| `standard` | 12 s / 3 s / 6 s | 3 | oui |
| `smoke` | 3 s / 1 s / 2 s | 1 | non (`non_comparable`) |

### Dual protocole (`dual_protocol`)

Un sous-ensemble **core** minimal est expandé en `MQTTv311` **et** `MQTTv5` :

- `pub_qos_sweep_telemetry`, `sub_exact_telemetry`
- `puback_latency_qos1`, `rtt_capacity_qos1`, `application_rtt_qos1`

Les classements / la matrice HTML utilisent des lignes `scenario · protocol` — **jamais** de comparaison cross-protocole. `aiomqtt3` (v5-only) se compare aux autres sur les lignes `MQTTv5` ; `amqtt` saute le v5.

Open-loop (`puback` / `application_rtt`) : fractions **`0.50` et `0.90`** seulement (budget).

## Topologies

| Topologie | Acteurs | Rôle du débit primaire |
|---|---|---|
| `publisher_only` | 1 publisher SUT | Completions publish du SUT |
| `publisher_with_oracle` | 1 publisher SUT + N subscribers (même lib) | Publish (intégrité côté sub) |
| `subscriber_ingress` | 1 subscriber SUT + **emqtt-bench** (N clients) | Messages **délivrés** au subscriber |
| `application_rtt` | initiator SUT + responder (même lib, cpuset `orch`) | Paires requête/réponse complètes |
| `duplex_gateway` | publisher SUT + subscriber SUT + inject emqtt-bench | Débit publisher (commands à 200/s) |
| `connect` | Probe dans l’orchestrateur | Latence / succès de connect |
| `fleet` | N connexions idle | Coût keepalive / RSS / CPU |

## Cadences

| Cadence | Comportement |
|---|---|
| `capacity` | Closed-loop max (pas de `target_rate`) |
| `loaded75` / fractions | Open-loop ; le point porte `load_fraction` → `target_rate` calibré |
| `steady50` | Open-loop à 50 % d’une base (défaut 2000 → **1000 msg/s**) |
| `burst` | Ingress : burst borné (`-L`), puis silence ; recovery via drain |
| `microburst` | Comme burst avec `-L 1000` |
| `periodic10` | Ingress à **10 msg/s** agrégés |

## Offre ingress (emqtt-bench) — point d’attention

Pour `subscriber_ingress` en capacité, le harness vise ~**40 000** msg/s agrégés (`target = 40000`), avec `loadgen_clients` (souvent 32).

L’intervalle emqtt-bench est entier en ms (`-I ≥ 1`), donc :

```text
nominal_rate ≈ clients × 1000 / I
```

Avec 32 clients et `I = 1`, le **nominal enregistré est 32 000**, même si la cible déclarée est 40 000. En pratique emqtt-bench (`-F` inflight) peut **émettre davantage** (~64k observés) ; le débit **délivré** au subscriber peut plafonner plus bas (client *ou* Mosquitto → une seule connexion).

Conséquence : si gmqtt et awscrt collent tous deux à ~30k, ce n’est pas forcément « mêmes perfs », ce peut être un **plafond d’offre / broker**. Pour chercher les vraies limites client, il faut monter `loadgen_clients` **et** vérifier que le broker n’est pas saturé.

---

## Suite `core`

### `pub_payload_sweep_qos0`

- **But** : capacité publisher QoS0 selon la taille de payload.
- **Topologie** : `publisher_only` · **Cadence** : `capacity`.
- **Variants** : `empty0`, `binary64`, `telemetry256`, `event1k`, `record16k`, `block64k`, `blob1m`.
- **Primaire** : completions publish / s.
- **Lecture** : courbe taille → débit ; `blob1m` peut aussi saturer CPU broker / réseau.

### `pub_qos_sweep_telemetry`

- **But** : capacité publisher pour QoS 0 / 1 / 2, payload fixe `telemetry256`.
- **Topologie** : `publisher_only` · **Cadence** : `capacity`.
- **Variants** : `qos_publish ∈ {0,1,2}`.
- **Primaire** : completions / s (QoS0 = remis au transport ; QoS1 = PUBACK ; QoS2 = PUBCOMP).
- **Refus** : clients sans QoS2 correct (`gmqtt`, `awscrt` → `not_implemented:qos2`).

### `pub_qos1_inflight`

- **But** : effet de la fenêtre inflight client sur la capacité QoS1.
- **Topologie** : `publisher_only` · **Cadence** : `capacity`.
- **Variants** : `inflight ∈ {1,20,100}` (+ `max_queued = 10×`, `outstanding = max(n,8)`).
- **Exige** : `max_inflight` / `max_queued` côté adapter (`require_max_*`).
- **Refus** : gmqtt, awscrt, amqtt, etc. sans knobs → `not_implemented:max_inflight`.

### `remaining_length_boundaries`

- **But** : transitions exactes de largeur d’encodage MQTT *Remaining Length* (1 vs 2 octets).
- **Topologie** : `publisher_only` · **Cadence** : `capacity`.
- **Variants** : payloads `rl_126` … `rl_16384` (taille choisie pour viser ces seuils).
- **Lecture** : diagnostic protocole / encodeur, pas ranking produit.

### `sub_exact_telemetry`

- **But** : capacité d’**ingress** : N publishers externes → 1 topic exact → 1 subscriber SUT.
- **Topologie** : `subscriber_ingress` · **Cadence** : `capacity`.
- **Loadgen** : 32 clients emqtt-bench, QoS0, `telemetry256`.
- **Primaire** : messages délivrés au callback subscriber / s.
- **Lecture** : comparer au **`effective_offer_msgs_per_s`** (= `nominal_rate`, ~32k avec `-c 32 -I 1`). Ne pas prendre `parsed.median_rate` QoS0 pour l’offre — emqtt-bench double-compte `pub` (voir [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md)). Si délivré ≈ offre, le point est **offer-limited**.
- **`$SYS`** : `sys_counters` (drops/sent) enregistrés sur la fenêtre de mesure.

### `sub_hierarchy_telemetry`

- **But** : même ingress, mais abonnement wildcard broker (`+` ou `#`) sur topologie `fleet4k_uniform`.
- **Variants** : `subscription=plus` et `subscription=hash`.
- **Primaire** : délivrances / s.
- **Lecture** : coût matching broker + client ; même caveat de plafond d’offre que `sub_exact`.

### `sub_callback_matching`

- **But** : coût du matching **local** `message_callback_add` (Paho).
- **Topologie** : `subscriber_ingress` · abonnement broker `#`, topics `cb/<i>/…` côté loadgen.
- **Variants** : `callback_filters ∈ {1,16,256}`.
- **Refus** : clients sans `native_message_callback_add` (tout sauf paho dans le catalogue stable).

### `duplex_gateway`

- **But** : charge « gateway » : le SUT publie de la télémétrie **et** reçoit des commandes injectées.
- **Topologie** : `duplex_gateway` (pub + sub sur cpuset `sut`).
- **Inject** : emqtt-bench → `bench/<run>/commands` à **200 msg/s** agrégés (2 clients).
- **Variants** : cadence publisher `steady50` ou `burst`.
- **Primaire** : débit publisher (souvent plafonné par la cadence) ; **pas** un ranking de capacité pure.
- **Rapport** : exclu du chart de throughput (scénario rate-capped).

### `burst_recovery`

- **But** : tenue / récupération sous burst d’ingress puis silence.
- **Topologie** : `subscriber_ingress` · **Cadence** : `burst` · sub `#` · fleet.
- **Loadgen** : démarre à `T_MEASURE`, `-L ≈ target × duration`, `I=1`.
- **Primaire** : débit délivré pendant la fenêtre ; drain pour le backlog.
- **Lecture** : même famille de plafond que les autres ingress capacité.

### `e2e_integrity`

- **But** : intégrité de séquence (header `PMQ1`) publisher → subscriber même lib.
- **Topologie** : `publisher_with_oracle` · **Cadence** : `steady50` (~**1000** msg/s).
- **Variants** : QoS 0/1/2 + payload vide QoS0 ; `force_header=True`.
- **Primaire** : débit atteint (toujours ~cap) ; le fond est missing/dup/ooo.
- **Rapport** : exclu du chart de throughput.

### `puback_latency_qos1`

- **But** : latence PUBACK en open-loop à fractions de la **capacité publish** du client.
- **Topologie** : `publisher_only` · fractions `0.50 / 0.90` · tag `dual_protocol`.
- **Exige** : `--load-profile` (ou calibration auto en `compare`).
- **Invalidation** : `open_loop_rate_out_of_tolerance` si le débit réalisé dévie > 2 % de la cible.

### `rtt_capacity_qos1`

- **But** : capacité closed-loop de paires RTT applicatives (même lib des deux côtés).
- **Topologie** : `application_rtt` · **Cadence** : `capacity` · `outstanding=32`.
- **Primaire** : paires complètes / s → baseline pour `application_rtt_qos1`.
- **Note** : amplifie le coût stack (deux publish + deux deliveries par échantillon).

### `application_rtt_qos1`

- **But** : latence RTT applicative open-loop aux fractions de **cette** capacité RTT.
- **Topologie** : `application_rtt` · fractions `0.50 / 0.90` · tag `dual_protocol`.
- **Exige** : `TCP_NODELAY` bout-en-bout (broker + client) ; sinon artefact Nagle ~84 ms/paire.
- **Refus** : `awscrt` → `not_implemented:tcp_nodelay`.

---

## Suite `full`

### `pub_segment_threshold_16k` / `pub_segment_block_64k` / `pub_segment_blob_1m`

- **But** : capacité publisher sur tailles « segmentées » (16 KiB / 64 KiB / 1 MiB), QoS0.
- **Topologie** : `publisher_only` · **Cadence** : `capacity`.
- **Lecture** : diagnostic fragmentation / copies ; un point chacun.

### `payload_stress`

- **But** : stress payload (8 MiB, str encoding, gros QoS1).
- **Variants** : `blob8m` QoS0, `telemetry256_str` QoS0, `block64k`/`blob1m` QoS1.

### `topic_stress`

- **But** : stress topiques (profondeur, longueur, unicode) + matching callbacks extrême.
- **Topologie** : `subscriber_ingress` · 16 clients loadgen.
- **Variants** : `deep32`, `long_topic_{256,1024}`, `unicode`, `callback_filters=4096`, overlapping × 8.

### `sub_multi_subscribe`

- **But** : N abonnements exacts sur un seul client.
- **Variants** : `subscription_count ∈ {16,256}` · `subscription=multi_exact`.

### `fanin_scaling`

- **But** : scaling fan-in publishers → 1 subscriber.
- **Modes** :
  - `constant_aggregate` : débit agrégé cible fixe (~40k), clients 1 / 16 / 128 ;
  - `per_publisher` : ~1000 msg/s **par** client → agrégat = `clients × 1000`.
- **Lecture** : connexion storm vs débit ; utile pour voir si le plafond bouge avec N.

### `fanout_scaling`

- **But** : 1 publisher SUT → N subscribers (même lib).
- **Topologie** : `publisher_with_oracle` · **Variants** : `subscribers ∈ {1,8,32}`.
- **Primaire** : côté publisher (coût fan-out broker + N stacks client).

### `periodic_and_microburst`

- **But** : formes de trafic extrêmes (très lent / micro-burst).
- **Variants** : `periodic10` (10/s), `microburst` (`-L 1000`).

### `mqttv5_properties`

- **But** : coût des propriétés PUBLISH v5 « réalistes » vs v3.1.1 / v5 sans props.
- **Topologie** : `publisher_with_oracle`.
- **Variants** : `MQTTv311/none`, `MQTTv5/none`, `MQTTv5/realistic`.

### `mqttv5_rich`

- **But** : propriétés lourdes ; variantes `topic_alias` / `subscription_identifier` souvent refusées (`not_implemented:*`) jusqu’à implémentation adapter.

### `qos_asymmetric`

- **But** : paires QoS pub/sub asymétriques à débit borné (`steady50`).
- **Variants** : (1,0), (2,1), (0,1).

### `network_matrix`

- **But** : même charge publish sous profils netem `localhost` / `lan` / `wan` / `edge`.
- **Marquage** : tout `network ≠ localhost` → `non_comparable` (diagnostic machine/kernel).

### `tls_steady_state`

- **But** : capacité publish QoS1 sur TLS **déjà établi** (pas handshake de masse).
- **Topologie** : `publisher_only` · `tls=True`.

### `connect_latency_and_churn`

- **But** : latence connect TCP/TLS et orages de connexions.
- **Topologie** : `connect`.
- **Variants** : serial TCP/TLS, `tls_resume`, concurrent 32/256.
- **Refus partiels** : certaines variantes `not_implemented:*` selon adapter.

### `client_fleet_idle`

- **But** : coût d’une flotte idle (keepalive 30 s) : RSS / CPU.
- **Topologie** : `fleet` · tailles 1 / 32 / 256.
- **Refus** : clients `async_bridged` (1 loop/thread par conn) → `fleet_async_bridged`.

### `broker_ceiling_ingress`

- **But** : sonde plafond **Mosquitto** sans SUT Python (emqtt pub + emqtt sub).
- **Suite** : `full` · tags `diagnostic` · `non_comparable`.
- **Variants** : `loadgen_clients ∈ {32,64,128}` → offre effective 32k / 64k / 128k (`I=1`).
- **Primaire** : rate `recv` du sub de référence.
- **Runbook** : [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md).

### `client_ceiling_ingress`

- **But** : même grille d’offre, subscriber SUT — le client casse-t-il avant le broker ?
- **Topologie** : `subscriber_ingress` · même offre que `broker_ceiling_ingress`.
- **Primaire** : delivered SUT vs `effective_offer` + `$SYS`.
- **Runbook** : [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md).

---

## Scénarios `planned` (catalogue seulement)

Non exécutés par les suites ; forcés → `not_implemented:planned_scenario` :

| Nom | Intention |
|---|---|
| `mqttv5_flow_control` | Interaction `Receive Maximum` broker vs inflight client |
| `queue_rejection` | Comptage accepts/rejects sous pression de file |
| `retained_bootstrap` | Snapshot retained massif (très sensible au broker) |
| `session_resume_qos1` | Session persistante + outage court + drain attendu |

---

## Suite `experimental`

Même contrat de mesure que `core`, mais classements séparés pour clients expérimentaux (`zmqtt`, `aiomqtt3`). Voir README.

---

## Comment lire un résultat

1. Regarder `status` / `reasons` (refus capability, broker CPU, open-loop hors tolérance).
2. Regarder `bottleneck` (`sut_limited` / `broker_limited` / `loadgen_limited` / `offer_limited`) — heuristique, pas une vérité absolue.
3. Pour l’ingress : comparer le primaire à `loadgen.effective_offer_msgs_per_s` (ou `nominal_rate`). **Ne pas** traiter `parsed.median_rate` QoS0 comme msgs/s réels (double-comptage emqtt-bench).
4. Ne pas classer `duplex_gateway` / `e2e_integrity` comme des courses de débit : ils sont **volontairement plafonnés**.
5. Latence : ne comparer que des points à la **même fraction** et avec calibration du **même** client.
6. Plafonds broker/client : voir [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md) (`broker_ceiling_ingress`, `client_ceiling_ingress`).

## Fichiers liés

| Fichier | Rôle |
|---|---|
| `src/mqtt_client_bench/scenarios.py` | Déclarations + profiles + expansion |
| `src/mqtt_client_bench/harness.py` | Orchestration, loadgen, validation |
| `src/mqtt_client_bench/loadgen.py` | emqtt-bench, `nominal_rate`, parsing |
| `src/mqtt_client_bench/sys_probe.py` | Probe `$SYS` dropped/sent |
| `src/mqtt_client_bench/workloads.py` | Payloads, topics, header intégrité |
| `src/mqtt_client_bench/roles/` | Workers publisher / subscriber / RTT / responder |
| `docs/CEILING_PROBES.md` | Runbook plafonds broker / client |
| `README.md` | Vue d’ensemble du banc et commandes CLI |
