package com.example.tp3;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.tp3.entities.Commande;

import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("clients")
public class ClientsConsommateur {

    private final JsonMapper json = new JsonMapper();

    // Pour chaque client, on garde le nombre de commandes reçues.
    private final Map<Integer, Integer> totaux = new HashMap<>();

// On garde clients-group par défaut, et on peut choisir un autre groupe au démarrage.
    @KafkaListener(topics = "commandes-creees", groupId = "${tp3.groupe-clients:clients-group}")
    public void recevoir(String message) {
        Commande commande = json.readValue(message, Commande.class);
        int idClient = commande.client_id();

        // Si c'est la première commande de ce client, son compteur part de zéro.
        int nombre = totaux.getOrDefault(idClient, 0);
        nombre = nombre + 1;
        totaux.put(idClient, nombre);

        System.out.println("Client : " + idClient);
        System.out.println("Produit : " + commande.produit());
        System.out.println("Montant : " + commande.montant());
        System.out.println("Nombre de commandes du client " + idClient + " : " + nombre);
    }
}