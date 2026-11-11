package com.example.tp3;

import com.example.tp3.entities.MessagePhilosophe;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("distribue")
public class FourchetteConsommateur {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json = new JsonMapper();
    private final int numero;

    // -1 veut dire que la fourchette est libre.
    private int proprietaire = -1;

    // On garde les philosophes qui attendent cette fourchette.
    private final List<Integer> attente = new ArrayList<>();

    public FourchetteConsommateur(
            KafkaTemplate<String, String> kafka,
            @Value("${philosophe.id}") int numero) {
        this.kafka = kafka;
        this.numero = numero;
    }

    @KafkaListener(
            topics = "fourchette-${philosophe.id}-demandes",
            groupId = "fourchette-${philosophe.id}-group")
    public synchronized void recevoir(String message) {
        MessagePhilosophe demande =
                json.readValue(message, MessagePhilosophe.class);

        int id = demande.id();

        // Cette fourchette est utilisée par son philosophe et le voisin précédent.
        if (id != numero && id != (numero + 4) % 5) {
            System.out.println("Ce philosophe n'utilise pas la fourchette " + numero);
            return;
        }

        if (demande.action().equals("PRENDRE")) {
            if (proprietaire != id && !attente.contains(id)) {
                attente.add(id);
                System.out.println("Philosophe " + id
                        + " attend la fourchette " + numero);
            }
        } else if (demande.action().equals("LIBERER") && proprietaire == id) {
            // On libère seulement si la demande vient du propriétaire.
            proprietaire = -1;
            System.out.println("Fourchette " + numero + " libérée");
        }

        if (proprietaire == -1 && !attente.isEmpty()) {
            // Le premier de la file devient le propriétaire.
            proprietaire = attente.remove(0);

            MessagePhilosophe autorisation =
                    new MessagePhilosophe("AUTORISE", proprietaire, numero);

            String reponse = json.writeValueAsString(autorisation);

            kafka.send("philosophe-" + proprietaire + "-reponses", reponse).join();

            System.out.println("Fourchette " + numero
                    + " donnée au philosophe " + proprietaire);
        }
    }
}