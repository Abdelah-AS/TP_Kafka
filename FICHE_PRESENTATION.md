# TP3 Kafka — fiche de démonstration

## Préparer les terminaux

**WSL — démarrer Kafka et vérifier que les topics sont disponibles**

```bash
cd /mnt/h/Downloads/web2/tp3-kafka
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Garde ce terminal pour envoyer les POST et interroger Kafka. Dans chaque terminal PowerShell, place-toi dans `H:\Downloads\web2\tp3-kafka`. Démarre les applications une par une et attends `partitions assigned` chez les consommateurs avant les POST. Les topics ont été créés pendant le TP.

Dans **WSL**, colle cette fonction une fois. Les arguments sont, dans l’ordre, l’identifiant de commande, le client, le montant et la date. Le produit est toujours `Clavier` dans cette démo.

```bash
post() {
  printf '{"commande_id":%s,"client_id":%s,"produit":"Clavier","montant":%s,"date":"%s"}' "$1" "$2" "$3" "$4" |
    curl.exe -sS -i -H 'Content-Type: application/json' --data-binary @- http://localhost:8240/commandes
  printf '\n'
}
```

---

## Démo 1 — Une commande, deux groupes, deux calculs

**1. Commandes.** Lance ces trois lignes dans **trois terminaux PowerShell distincts**, une ligne par terminal. Elles démarrent commandes sur 8240, clients sur 8241 et statistiques sur 8242.

```powershell
mvn -o spring-boot:run '-Dspring-boot.run.profiles=commandes'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=clients' '-Dspring-boot.run.arguments=--server.port=8241'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=statistiques' '-Dspring-boot.run.arguments=--server.port=8242'
```

Dans **WSL**, envoie deux commandes de novembre pour le client 2 et une commande d’octobre pour le client 5.

```bash
post 1001 2 50 2026-11-11
post 1002 2 25 2026-11-12
post 1003 5 40 2026-10-15
```

**2. Résultat à montrer.** Les trois POST renvoient `202`. Clients compte deux commandes pour le client 2 et une pour le client 5. Statistiques compte trois claviers, **75 € en novembre** et **40 € en octobre**. Ces chiffres supposent des compteurs vides au départ. Si les services ont déjà traité des événements, montre les augmentations.

**3. Code qui explique le résultat.** Dans [CommandesController.java](src/main/java/com/example/tp3/web/CommandesController.java#L27), `@RequestBody` lit le JSON HTTP. Le contrôleur recrée ensuite le JSON à publier. La clé du message est l’identifiant du client.

```java
String cle = String.valueOf(commande.client_id());
String message = json.writeValueAsString(commande);
kafka.send("commandes-creees", cle, message).join();
```

Dans [ClientsConsommateur.java](src/main/java/com/example/tp3/ClientsConsommateur.java#L24) et [StatistiquesConsommateur.java](src/main/java/com/example/tp3/StatistiquesConsommateur.java#L27), les deux `@KafkaListener` écoutent `commandes-creees` avec des groupes différents. `readValue` reconstruit une `Commande`. Clients ajoute 1 au compteur du client. Statistiques ajoute 1 au compteur du produit, puis additionne le montant dans la case correspondant au mois.

```java
String mois = commande.date().substring(0, 7);
BigDecimal total = chiffreAffairesParMois.getOrDefault(mois, BigDecimal.ZERO);
total = total.add(commande.montant());
chiffreAffairesParMois.put(mois, total);
```

**4. Conclusion à dire.** « Kafka confirme la publication, donc je reçois 202. Les deux groupes lisent ensuite le même événement. Chacun en tire le résultat qui l’intéresse. »

---

## Démo 2 — Arrêter clients et voir Kafka conserver le message

**1. Commandes.** Garde commandes, statistiques et Kafka ouverts. Dans **WSL**, vérifie que `clients-group` a une position enregistrée pour la partition qui reçoit les commandes du client 2. Après la démo 1, attends que le retard soit à zéro.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group clients-group
```

Arrête **seulement clients** avec `Ctrl+C`. Publie une nouvelle commande dans WSL, puis affiche à nouveau le groupe.

```bash
post 1004 2 10 2026-11-13
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group clients-group
```

Redémarre clients dans **PowerShell**, avec le même profil et le même groupe.

```powershell
mvn -o spring-boot:run '-Dspring-boot.run.profiles=clients' '-Dspring-boot.run.arguments=--server.port=8241'
```

Après avoir vu la commande à 10 € dans ses logs, relance `--describe` dans WSL.

**2. Résultat à montrer.** Pendant l’arrêt, le POST renvoie `202` et statistiques reçoit la commande. Pour la partition concernée, `LOG-END-OFFSET` avance et `CURRENT-OFFSET` reste à sa position précédente. `LAG` passe à 1 si une seule commande attend. Quand clients revient, il lit la commande et le retard revient à zéro après enregistrement de la nouvelle position.

**3. Code qui explique le résultat.** Le `.join()` de la démo 1 attend la confirmation du broker. Le listener ci-dessous reprend avec `clients-group` lorsqu’aucun autre groupe n’est fourni au démarrage. Kafka suit la position de ce groupe. La `HashMap` `totaux`, elle, est recréée au démarrage de l’application.

```java
@KafkaListener(topics = "commandes-creees", groupId = "${tp3.groupe-clients:clients-group}")
```

**4. Conclusion à dire.** « La commande publiée pendant l’arrêt est lue au redémarrage. Le groupe reprend sa lecture dans Kafka. Le compteur Java repart de zéro car il était en mémoire. »

*Ce scénario complet reste à vérifier lors de la répétition.*

---

## Démo 3 — Deux instances se partagent un groupe

**1. Commandes.** Arrête clients et statistiques, puis réutilise leurs deux terminaux **PowerShell**. Garde commandes et Kafka ouverts. Lance une ligne par terminal. Les deux instances démarrent avec le même groupe `groupe-A`.

```powershell
mvn -o spring-boot:run '-Dspring-boot.run.profiles=clients' '-Dspring-boot.run.arguments=--server.port=8241 --tp3.groupe-clients=groupe-A'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=clients' '-Dspring-boot.run.arguments=--server.port=8243 --tp3.groupe-clients=groupe-A'
```

Dans **WSL**, affiche les partitions attribuées à chaque membre, puis publie deux commandes du même client.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group groupe-A --members --verbose
post 1010 2 15 2026-11-14
post 1011 2 20 2026-11-14
```

**2. Résultat à montrer.** `--members --verbose` affiche les deux instances et leurs partitions. Les deux commandes du client 2 apparaissent dans le même terminal tant que la répartition reste stable.

**3. Code qui explique le résultat.** Le `groupId` de la démo 2 prend maintenant la valeur `groupe-A` grâce au paramètre `--tp3.groupe-clients`. Le contrôleur utilise `String.valueOf(commande.client_id())` comme clé. Cette clé conduit les messages du même client à la même partition avec la configuration actuelle.

**4. Conclusion à dire.** « Dans un groupe, chaque partition est confiée à une instance à la fois. Je peux retrouver le terminal qui traitera la commande en regardant quelle instance possède sa partition. »

---

## Démo 4 — Cinq philosophes avec superviseur

**1. Commandes.** Arrête les applications Java des démos précédentes et garde Kafka. Lance le **superviseur** dans un terminal PowerShell. Lance ensuite chaque philosophe dans son propre terminal PowerShell, une ligne par terminal.

```powershell
mvn -o spring-boot:run '-Dspring-boot.run.profiles=superviseur' '-Dspring-boot.run.arguments=--server.port=8250'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=philosophe' '-Dspring-boot.run.arguments=--server.port=8260 --philosophe.id=0'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=philosophe' '-Dspring-boot.run.arguments=--server.port=8261 --philosophe.id=1'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=philosophe' '-Dspring-boot.run.arguments=--server.port=8262 --philosophe.id=2'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=philosophe' '-Dspring-boot.run.arguments=--server.port=8263 --philosophe.id=3'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=philosophe' '-Dspring-boot.run.arguments=--server.port=8264 --philosophe.id=4'
```

Quand les six instances écoutent leurs topics, déclenche un repas par philosophe depuis **WSL**.

```bash
for i in 0 1 2 3 4
do
  curl.exe -sS -X POST "http://localhost:826$i/demarrer"
  printf '\n'
done
```

**2. Résultat à montrer.** Les cinq philosophes atteignent `fin`. Dans le terminal du superviseur, `Tout le monde a terminé. Ordre : [...]` doit contenir les cinq identifiants. L’ordre peut varier.

**3. Code qui explique le résultat.** Dans [PhilosopheController.java](src/main/java/com/example/tp3/web/PhilosopheController.java#L32), le POST publie `DEMANDER` dans `philosophes-demandes`. Le philosophe reçoit `AUTORISE` dans son topic de réponses puis envoie `TERMINER` après le repas. Dans [SuperviseurConsommateur.java](src/main/java/com/example/tp3/SuperviseurConsommateur.java#L84), le superviseur vérifie les deux fourchettes avant de les réserver ensemble.

```java
if (fourchettes[id] || fourchettes[droite]) {
    return;
}

fourchettes[id] = true;
fourchettes[droite] = true;
```

**4. Conclusion à dire.** « Le superviseur contrôle les deux fourchettes. Il autorise le premier philosophe en attente lorsqu’il peut lui donner les deux, puis les libère à la réception de TERMINER. »

---

## Démo 5 — Cinq philosophes sans superviseur

**1. Commandes.** Arrête les six applications de la démo 4 après la fin des repas. Garde Kafka. Lance ces cinq lignes dans **cinq terminaux PowerShell**, une par terminal. Chaque instance exécute un philosophe et gère la fourchette du même numéro.

```powershell
mvn -o spring-boot:run '-Dspring-boot.run.profiles=distribue' '-Dspring-boot.run.arguments=--server.port=8270 --philosophe.id=0'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=distribue' '-Dspring-boot.run.arguments=--server.port=8271 --philosophe.id=1'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=distribue' '-Dspring-boot.run.arguments=--server.port=8272 --philosophe.id=2'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=distribue' '-Dspring-boot.run.arguments=--server.port=8273 --philosophe.id=3'
mvn -o spring-boot:run '-Dspring-boot.run.profiles=distribue' '-Dspring-boot.run.arguments=--server.port=8274 --philosophe.id=4'
```

Attends les deux attributions de topics par instance, demandes de fourchette et réponses du philosophe. Déclenche les repas dans **WSL**.

```bash
for i in 0 1 2 3 4
do
  curl.exe -sS -X POST "http://localhost:827$i/demarrer"
  printf '\n'
done
```

**2. Résultat à montrer.** Chaque instance affiche `manger` puis `fin`. Montre aussi une attribution et une libération de fourchette dans les logs. Garde les cinq instances ouvertes jusqu’à la fin de tous les repas, car chacune gère encore une fourchette partagée.

**3. Code qui explique le résultat.** Dans [FourchetteConsommateur.java](src/main/java/com/example/tp3/FourchetteConsommateur.java#L55), le gestionnaire libère la fourchette seulement si la demande vient de son propriétaire. Dans [PhilosopheDistribueController.java](src/main/java/com/example/tp3/web/PhilosopheDistribueController.java#L35), chaque philosophe demande le plus petit numéro, attend l’autorisation, puis demande le plus grand.

```java
this.premiere = Math.min(id, (id + 1) % 5);
this.seconde = Math.max(id, (id + 1) % 5);
```

**4. Conclusion à dire.** « Le philosophe 4 demande 0 puis 4. Tout le monde suit le même ordre croissant, ce qui empêche l’attente circulaire dans cette simulation. Chaque gestionnaire décide qui possède sa fourchette. Kafka transporte les demandes et les réponses. »

---

## Après la démo

Arrête les applications Java avec `Ctrl+C`, puis Kafka avec `docker compose stop` dans WSL. Kafka tourne dans Docker et les services Java sont lancés avec Maven dans cette version. La mise en conteneur des services Java reste à faire. Les compteurs sont en mémoire et chaque message traité est compté, y compris si une commande identique est republiée.
