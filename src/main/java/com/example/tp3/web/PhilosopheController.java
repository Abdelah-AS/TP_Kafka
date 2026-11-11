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
@Profile("philosophe")
public class PhilosopheController {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json = new JsonMapper();
    private final int id;

    private boolean demandeEnvoyee = false;
    private boolean repasTermine = false;

    public PhilosopheController(
            KafkaTemplate<String, String> kafka,
            @Value("${philosophe.id}") int id) {
        this.kafka = kafka;
        this.id = id;
    }

    @PostMapping("/demarrer")
    public synchronized String demarrer() {
        // Chaque philosophe fait un seul repas dans notre test.
        if (demandeEnvoyee) {
            return "La demande a déjà été envoyée";
        }

        System.out.println("Philosophe " + id + " : réfléchir");

        MessagePhilosophe demande =
                new MessagePhilosophe("DEMANDER", id, -1);

        System.out.println("Philosophe " + id + " : attendre");

        kafka.send("philosophes-demandes",
                json.writeValueAsString(demande)).join();

        demandeEnvoyee = true;
        return "Demande envoyée pour le philosophe " + id;
    }

    @KafkaListener(
            topics = "philosophe-${philosophe.id}-reponses",
            groupId = "philosophe-${philosophe.id}-group",
            properties = "auto.offset.reset=latest")
    public synchronized void recevoir(String message) throws InterruptedException {
        MessagePhilosophe reponse =
                json.readValue(message, MessagePhilosophe.class);

        // On mange seulement après une autorisation qui nous concerne.
        if (reponse.id() == id
                && reponse.action().equals("AUTORISE")
                && !repasTermine) {

            System.out.println("Philosophe " + id + " : manger");
            Thread.sleep(1000);

            MessagePhilosophe fin =
                    new MessagePhilosophe("TERMINER", id, -1);

            kafka.send("philosophes-demandes",
                    json.writeValueAsString(fin)).join();

            repasTermine = true;
            System.out.println("Philosophe " + id + " : fin");
        }
    }
}