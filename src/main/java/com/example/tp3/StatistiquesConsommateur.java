package com.example.tp3;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.tp3.entities.Commande;

import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("statistiques")
public class StatistiquesConsommateur {

    private final JsonMapper json = new JsonMapper();

    // On garde le nombre de commandes pour chaque produit.
    private final Map<String, Integer> totauxParProduit = new HashMap<>();

    // On garde le montant total des commandes pour chaque mois.
    private final Map<String, BigDecimal> chiffreAffairesParMois = new HashMap<>();

    @KafkaListener(topics = "commandes-creees", groupId = "statistiques-group")
    public void recevoir(String message) {
        // On transforme le JSON reçu en objet Commande.
        Commande commande = json.readValue(message, Commande.class);

        String produit = commande.produit();

        // On récupère le compteur du produit et on ajoute une commande.
        int nombre = totauxParProduit.getOrDefault(produit, 0);
        nombre = nombre + 1;
        totauxParProduit.put(produit, nombre);

        // Dans une date comme 2026-11-10, on garde juste 2026-11.
        String mois = commande.date().substring(0, 7);

        // Si c'est la première commande du mois, le total part de zéro.
        BigDecimal total = chiffreAffairesParMois.getOrDefault(mois, BigDecimal.ZERO);
        total = total.add(commande.montant());
        chiffreAffairesParMois.put(mois, total);

        System.out.println("Nombre de commandes pour " + produit + " : " + nombre);
        System.out.println("Chiffre d'affaires pour " + mois + " : " + total);
    }
}