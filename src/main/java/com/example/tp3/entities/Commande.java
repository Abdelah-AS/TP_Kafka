package com.example.tp3.entities;

import java.math.BigDecimal;

public record Commande(
        long commande_id,
        int client_id,
        String produit,
        BigDecimal montant,
        String date
) {
}