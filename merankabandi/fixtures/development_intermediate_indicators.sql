-- Merankabandi Development and Intermediate Indicators
-- This script loads sections and indicators for the Merankabandi module

-- Clear existing data (optional - uncomment if needed)
-- DELETE FROM merankabandi_indicatorachievement;
-- DELETE FROM merankabandi_indicator;
-- DELETE FROM merankabandi_section;

-- Insert Sections
INSERT INTO merankabandi_section (id, uuid, name, date_created, date_updated, is_deleted, version, user_created_id, user_updated_id)
VALUES 
    (1, 'e1f4d8a0-1234-4567-8901-000000000001', 'Renforcer les capacités de gestion', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (2, 'e1f4d8a0-1234-4567-8901-000000000002', 'Renforcer les filets de sécurité', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (3, 'e1f4d8a0-1234-4567-8901-000000000003', 'Promouvoir l''inclusion productive et l''accès à l''emploi', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (4, 'e1f4d8a0-1234-4567-8901-000000000004', 'Apporter une réponse immédiate et efficace à une crise ou une urgence éligible', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (5, 'e1f4d8a0-1234-4567-8901-000000000005', 'Indicateurs intermédiaires', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1)
ON CONFLICT (id) DO NOTHING;

-- Insert Development Indicators
INSERT INTO merankabandi_indicator (id, uuid, section_id, name, pbc, baseline, target, observation, date_created, date_updated, is_deleted, version, user_created_id, user_updated_id)
VALUES 
    -- Section 1: Renforcer les capacités de gestion
    (1, 'e1f4d8a0-1234-4567-8901-100000000001', 1, 'Ménages des zones ciblées inscrits au Registre social national (nombre)', '', '0.00', '250000.00', 'Ménages appuyés de la s/c 1.1, 1.2, compo 4 et 6', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (2, 'e1f4d8a0-1234-4567-8901-100000000002', 1, 'Ménages des zones ciblées inscrits au Registre social national - réfugiés, ventilés par sexe (Nombre)', '', '0.00', '15000.00', 'Ménages de 2 camps des réfugiés de la Province Ruyigi (Bwagiriza et Nyankanda)', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (3, 'e1f4d8a0-1234-4567-8901-100000000003', 1, 'Ménages des zones ciblées inclus dans le registre social national - communautés d''accueil, ventilés par sexe (nombre)', '', '0.00', '25000.00', '5 633 transferts monétaires aux ménages des communautés hôtes en communes Butezi (966), Bweru (360) et Ryigi (1 241) en Province Ruyigi, Gasorwe (1 800) en Province Munynga et Kiremba (1 266) en Province Ngozi', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (4, 'e1f4d8a0-1234-4567-8901-100000000004', 1, 'Proportion des ménages inscrits dans la base de données des bénéficiaires vivant sous le seuil d''extrême pauvreté (Pourcentage)', '', '0.00', '80.00', '', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    
    -- Section 2: Renforcer les filets de sécurité
    (5, 'e1f4d8a0-1234-4567-8901-100000000005', 2, 'Bénéficiaires des programmes de protection sociale (CRI, nombre)', '', '56090.00', '305000.00', 'Ménages appuyés de la s/c 1.1, 1.2, de la compo 4 et 6 + les bénéficiaires de la vague1', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    
    -- Section 3: Promouvoir l'inclusion productive et l'accès à l'emploi
    (6, 'e1f4d8a0-1234-4567-8901-100000000006', 3, 'Bénéficiaires d''interventions axées sur l''emploi (CRI, nombre)', '', '0.00', '150000.00', '', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    
    -- Section 4: Apporter une réponse immédiate et efficace
    (7, 'e1f4d8a0-1234-4567-8901-100000000007', 4, 'Agriculteurs ayant bénéficié d''actifs ou de services agricoles (CRI, nombre)', '', '0.00', '50000.00', 'Bénéficiaires de la composante 6 (CERC)', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    
    -- Section 5: Indicateurs intermédiaires
    (8, 'e1f4d8a0-1234-4567-8901-100000000008', 5, 'Système de gestion des informations sur la protection sociale mis en place (Oui / Non)', '', 'Non', 'Oui', 'La plateforme technique est disponible', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (9, 'e1f4d8a0-1234-4567-8901-100000000009', 5, 'Plan de communication développé (Oui / Non)', '', 'Non', 'Oui', 'La stratégie de communication est disponible', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (10, 'e1f4d8a0-1234-4567-8901-100000000010', 5, 'Pourcentage des bénéficiaires satisfaits du processus d''inscription au programme', '', '0.00', '80.00', 'Enquête de perception réalisée en mars 2023', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (11, 'e1f4d8a0-1234-4567-8901-100000000011', 5, 'Pourcentage des plaintes résolues', '', '0.00', '90.00', 'Rapport du mécanisme de gestion des plaintes du 1er trimestre 2023', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1),
    (12, 'e1f4d8a0-1234-4567-8901-100000000012', 5, 'Des activités de développement des compétences mises en œuvre (Oui / Non)', '', 'Non', 'Oui', 'Formation en cours sur les métiers dans 5 provinces', '2024-01-01 00:00:00', '2024-01-01 00:00:00', false, 1, 1, 1)
ON CONFLICT (id) DO NOTHING;

-- Update sequences if using PostgreSQL
SELECT setval('merankabandi_section_id_seq', (SELECT MAX(id) FROM merankabandi_section), true);
SELECT setval('merankabandi_indicator_id_seq', (SELECT MAX(id) FROM merankabandi_indicator), true);