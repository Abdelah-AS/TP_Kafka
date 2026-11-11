package com.example.tp3;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.tp3.entities.MessagePhilosophe;

import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("superviseur")
public class SuperviseurConsommateur {

    private final JsonMapper json = new JsonMapper();
    private final KafkaTemplate<String, String> kafka;

    // Au départ tout est faux, donc toutes les fourchettes sont libres.
    private final boolean[] fourchettes = new boolean[5];
    private final boolean[] mange = new boolean[5];
    private final boolean[] termine = new boolean[5];

    // On garde les demandes dans l'ordre où elles arrivent.
    private final List<Integer> attente = new ArrayList<>();
    private final List<Integer> ordre = new ArrayList<>();

    public SuperviseurConsommateur(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = "philosophes-demandes", groupId = "superviseur-group")
    public synchronized void recevoir(String message) {
        MessagePhilosophe demande =
                json.readValue(message, MessagePhilosophe.class);

        int id = demande.id();

        if (id < 0 || id > 4) {
            System.out.println("Identifiant de philosophe invalide");
            return;
        }

        if (demande.action().equals("DEMANDER")) {
            // On ajoute une seule fois le philosophe dans la file.
            if (!mange[id] && !termine[id] && !attente.contains(id)) {
                attente.add(id);
                System.out.println("Le philosophe " + id + " attend");
            }
        } else if (demande.action().equals("TERMINER") && mange[id]) {
            // Il a fini son repas, donc ses deux fourchettes sont libres.
            fourchettes[id] = false;
            fourchettes[(id + 1) % 5] = false;
            mange[id] = false;
            termine[id] = true;

            System.out.println("Le philosophe " + id + " a terminé");
        }

        servir();

        int nombreTermines = 0;

        for (boolean fin : termine) {
            if (fin) {
                nombreTermines = nombreTermines + 1;
            }
        }

        if (nombreTermines == 5) {
            System.out.println("Tout le monde a terminé. Ordre : " + ordre);
        }
    }

    private void servir() {
        while (!attente.isEmpty()) {
            int id = attente.get(0);
            int droite = (id + 1) % 5;

            // Si une des deux fourchettes est prise, le premier doit attendre.
            if (fourchettes[id] || fourchettes[droite]) {
                return;
            }

            // On réserve les deux avant de donner l'autorisation.
            fourchettes[id] = true;
            fourchettes[droite] = true;
            mange[id] = true;

            attente.remove(0);
            ordre.add(id);

            MessagePhilosophe autorisation =
                    new MessagePhilosophe("AUTORISE", id, -1);

            String reponse = json.writeValueAsString(autorisation);

            // Chaque philosophe reçoit la réponse dans son propre topic.
            kafka.send("philosophe-" + id + "-reponses", reponse).join();

            System.out.println("Le philosophe " + id + " peut manger");
        }
    }
}