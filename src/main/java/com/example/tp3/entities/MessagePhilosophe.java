package com.example.tp3.entities;

// Ce message sert aux échanges entre les philosophes et le superviseur.
public record MessagePhilosophe(
        String action,
        int id,
        int fourchette
) {
}