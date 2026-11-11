package com.example.tp3.web;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.tp3.entities.Commande;

import tools.jackson.databind.json.JsonMapper;

@RestController
@Profile("commandes")
public class CommandesController {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json = new JsonMapper();

    public CommandesController(KafkaTemplate<String, String> kafka) {
        // Spring nous donne l'objet qu'on va utiliser pour envoyer les messages à Kafka.
        this.kafka = kafka;
    }

    @PostMapping("/commandes")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Commande creer(@RequestBody Commande commande) {
        // On prend l'identifiant du client comme clé du message.
        String cle = String.valueOf(commande.client_id());

        // On transforme la commande en JSON pour pouvoir l'envoyer.
        String message = json.writeValueAsString(commande);

        // On envoie le message dans le topic et on attend que Kafka confirme l'envoi.
        kafka.send("commandes-creees", cle, message).join();

        // On renvoie la commande à la personne qui a fait le POST.
        return commande;
    }
}