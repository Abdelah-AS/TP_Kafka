package com.example.tp3.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.tp3.entities.MessagePhilosophe;

import tools.jackson.databind.json.JsonMapper;

@RestController
@Profile("distribue")
public class PhilosopheDistribueController {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json = new JsonMapper();
    private final int id;
    private final int premiere;
    private final int seconde;

    private boolean demandeEnvoyee = false;
    private boolean premiereObtenue = false;
    private boolean repasTermine = false;

    public PhilosopheDistribueController(
            KafkaTemplate<String, String> kafka,
            @Value("${philosophe.id}") int id) {
        this.kafka = kafka;
        this.id = id;

        // On prend toujours le petit numéro avant le grand.
        this.premiere = Math.min(id, (id + 1) % 5);
        this.seconde = Math.max(id, (id + 1) % 5);
    }

    @PostMapping("/demarrer")
    public synchronized String demarrer() {
        if (demandeEnvoyee) {
            return "La demande a déjà été envoyée";
        }

        System.out.println("Philosophe " + id + " : réfléchir");
        System.out.println("Philosophe " + id + " : attendre");

        envoyer(premiere, "PRENDRE");
        demandeEnvoyee = true;

        return "Première fourchette demandée par " + id;
    }

    @KafkaListener(
            topics = "philosophe-${philosophe.id}-reponses",
            groupId = "philosophe-distribue-${philosophe.id}-group",
            properties = "auto.offset.reset=latest")
    public synchronized void recevoir(String message) throws InterruptedException {
        MessagePhilosophe reponse =
                json.readValue(message, MessagePhilosophe.class);

        if (reponse.id() != id
                || !reponse.action().equals("AUTORISE")
                || !demandeEnvoyee
                || repasTermine) {
            return;
        }

        if (reponse.fourchette() == premiere && !premiereObtenue) {
            // On a la première, donc on peut demander la deuxième.
            premiereObtenue = true;
            System.out.println("Philosophe " + id
                    + " a obtenu la fourchette " + premiere);

            envoyer(seconde, "PRENDRE");

        } else if (reponse.fourchette() == seconde && premiereObtenue) {
            // Les deux autorisations sont reçues, on peut manger.
            System.out.println("Philosophe " + id + " : manger");

            try {
                Thread.sleep(1000);
            } finally {
                // Après le repas, on rend les deux fourchettes.
                envoyer(seconde, "LIBERER");
                envoyer(premiere, "LIBERER");
            }

            repasTermine = true;
            System.out.println("Philosophe " + id + " : fin");
        }
    }

    private void envoyer(int fourchette, String action) {
        // On écrit dans le topic de la fourchette concernée.
        MessagePhilosophe message =
                new MessagePhilosophe(action, id, fourchette);

        String texte = json.writeValueAsString(message);

        kafka.send("fourchette-" + fourchette + "-demandes", texte).join();
    }
}