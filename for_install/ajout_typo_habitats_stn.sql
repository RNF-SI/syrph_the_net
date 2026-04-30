-- =========================================================
--  Import habitats -> ref_habitats.habref
--  Typologie : cd_table = 'TYPO_STN' (récupération auto du cd_typo)
--  CSV attendu dans le même dossier : ./habitats_stn.csv
--  Exécution : psql -d <base> -f ajout_typo_habitats_stn.sql
-- =========================================================

\set ON_ERROR_STOP on

-- Sécurise l'auto-incrément de cd_typo si la séquence est désynchronisée
SELECT setval(
  pg_get_serial_sequence('ref_habitats.typoref', 'cd_typo'),
  COALESCE((SELECT MAX(cd_typo) FROM ref_habitats.typoref), 0),
  true
);

-- Création de typologie dans typoref si elle n'existe pas encore :
INSERT INTO ref_habitats.typoref (cd_table,lb_nom_typo,nom_jeu_donnees,date_creation,date_mise_jour_metadonnees,auteur_typo,auteur_table,territoire,organisme,langue,presentation,description,origine,ref_biblio,mots_cles,diffusion,type_table)
SELECT 'TYPO_STN','Typologie Syrph The Net','Typologie des habitats spécifique au protocole Syrph The Net','26/08/2025','26/08/2025','Speight M. C. D., Castella E. & Sarthou V. (2015).','Zacharie Moulin (Réserves Naturelles de France)','France métropolitaine','Réserves Naturelles de France','FR','La typologie d’habitats de StN est différente de la typologie CORINE biotopes et de la phytosociologie (Speight, 2017a), les découpages ne correspondant pas à l’utilisation des habitats par les syrphes ou ne couvrant pas totalement la zone géographique de la base de données. La méthode Syrph the Net se base sur une typologie adaptée. Cette dernière est présentée et définie, ainsi que les correspondances vers les typologies CORINE biotopes, EUNIS et phytosociologique sigmatiste, dans le volume StN « Content and Glossary » (Speight et al., 2016) et sa traduction française « Contenu et Glossaire » (Speight et al., 2015). La typologie StN définit deux catégories d’habitats : les « macrohabitats », correspondant aux habitats en tant que communautés végétales comme dans les typologies CORINE biotopes ou phytosociologique ; les habitats supplémentaires, regroupant les éléments ponctuels ou linéaires comme les habitats aquatiques (sources, mares, cours d’eau), les chemins, les clairières. Ces habitats supplémentaires sont liés aux macro-habitats dans le relevé des habitats. Les habitats supplémentaires viennent affiner la description du site permettant de compléter la liste des espèces prédites. Par exemple, une forêt avec des clairières herbeuses abritera théoriquement plus d’espèces qu’une même forêt dépourvue de ces clairières.','Table présentant les informations de base de la typologie spécifique Syrph the Net','Fichier de Cédric Vanappelghem','Speight M. C. D., Castella E. & Sarthou V. (2015). Base de Données StN: Contenu et Glossaire des termes 2015. Syrph the Net, the database of European Syrphidae, Vol.82, 99 pp, Syrph the Net publications, Dublin.','Typologie, habitats, Syrphes, StN','true','TYPO'
WHERE NOT EXISTS ( SELECT 1 FROM ref_habitats.typoref WHERE cd_table = 'TYPO_STN');

-- Import des habitats depuis le CSV

-- Sécurité : s'assurer que TYPO_STN existe (ton INSERT doit être exécuté avant)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM ref_habitats.typoref WHERE cd_table = 'TYPO_STN') THEN
    RAISE EXCEPTION 'cd_table=TYPO_STN introuvable dans ref_habitats.typoref. Insérez-le avant.';
  END IF;
END$$;

BEGIN;

-- ---------- Paramètres dynamiques depuis typoref ----------
WITH params AS (
  SELECT 
    t.cd_typo::int                           AS cd_typo,        -- récupéré automatiquement
    'Valide'::varchar(20)                    AS fg_validite,
    NULL::varchar(500)                       AS lb_auteur,
    'FR'::varchar(5)                         AS france
  FROM ref_habitats.typoref t
  WHERE t.cd_table = 'TYPO_STN'
)
SELECT 1;

-- ---------- Table de staging ----------
DROP TABLE IF EXISTS ref_habitats._hab_import;

CREATE TABLE ref_habitats._hab_import (
  lb_hab_fr text,
  lb_code   text
);

-- ----------- CHARGEMENT CSV (côté client psql) -----------
\copy ref_habitats._hab_import (lb_hab_fr, lb_code) FROM './habitats_stn.csv' WITH CSV HEADER DELIMITER ';' QUOTE '"';

-- ------------ Nettoyage / normalisation ------------
DELETE FROM ref_habitats._hab_import WHERE lb_code IS NULL OR btrim(lb_code) = '';
UPDATE ref_habitats._hab_import SET lb_code = btrim(lb_code), lb_hab_fr = nullif(btrim(lb_hab_fr), '');

-- ------------ Attribution des IDs et calcul des relations ------------
WITH max_exist AS (
  SELECT COALESCE(MAX(cd_hab), 0) AS base FROM ref_habitats.habref
),
imported AS (
  SELECT DISTINCT i.lb_code, i.lb_hab_fr
  FROM ref_habitats._hab_import i
),
ranked AS (
  SELECT i.*, ROW_NUMBER() OVER (ORDER BY i.lb_code) AS rn FROM imported i
),
nodes AS (
  SELECT 
    (m.base + r.rn)::int                           AS cd_hab,
    r.lb_code,
    r.lb_hab_fr,
    length(r.lb_code)::int                         AS niveau,
    ('Niveau ' || length(r.lb_code))::varchar(100) AS lb_niveau,
    CASE WHEN length(r.lb_code) > 1 
         THEN left(r.lb_code, length(r.lb_code)-1) 
         ELSE NULL 
    END                                            AS parent_code
  FROM ranked r
  CROSS JOIN max_exist m
),
roots AS (
  SELECT n.*
  FROM nodes n
  LEFT JOIN nodes p ON p.lb_code = n.parent_code
  WHERE n.parent_code IS NULL OR p.lb_code IS NULL
),
RECURSIVE tree AS (
  SELECT 
    rt.lb_code,
    rt.cd_hab,
    rt.parent_code,
    ('/' || rt.cd_hab::text) AS path_cd_hab
  FROM roots rt

  UNION ALL

  SELECT 
    c.lb_code,
    c.cd_hab,
    c.parent_code,
    (t.path_cd_hab || '/' || c.cd_hab::text) AS path_cd_hab
  FROM tree t
  JOIN nodes c ON c.parent_code = t.lb_code
),
parent_ids AS (
  SELECT c.lb_code, p.cd_hab AS cd_hab_sup
  FROM nodes c
  LEFT JOIN nodes p ON p.lb_code = c.parent_code
),
final_rows AS (
  SELECT 
    n.cd_hab, n.lb_code, n.lb_hab_fr, n.niveau, n.lb_niveau,
    pi.cd_hab_sup, t.path_cd_hab
  FROM nodes n
  LEFT JOIN parent_ids pi ON pi.lb_code = n.lb_code
  LEFT JOIN tree t        ON t.lb_code = n.lb_code
)
INSERT INTO ref_habitats.habref (
  cd_hab, fg_validite, cd_typo, lb_code, lb_hab_fr, lb_hab_fr_complet,
  lb_hab_en, lb_auteur, niveau, lb_niveau, cd_hab_sup, path_cd_hab,
  france, lb_description
)
SELECT
  f.cd_hab,
  p.fg_validite,
  p.cd_typo,                -- << cd_typo récupéré automatiquement
  f.lb_code,
  f.lb_hab_fr,
  NULL,                     -- lb_hab_fr_complet
  NULL,                     -- lb_hab_en
  p.lb_auteur,
  f.niveau,
  f.lb_niveau,
  f.cd_hab_sup,
  f.path_cd_hab,
  p.france,
  NULL                      -- lb_description
FROM final_rows f
CROSS JOIN (SELECT * FROM params LIMIT 1) p
ON CONFLICT (cd_hab) DO NOTHING
;

DROP TABLE IF EXISTS ref_habitats._hab_import;

-- Repeuplement de l'autocomplétion des habitats
DELETE FROM ref_habitats.autocomplete_habitat;
INSERT INTO ref_habitats.autocomplete_habitat
SELECT
  cd_hab,
  h.cd_typo,
  lb_code,
  lb_nom_typo,
  concat(lb_code, ' - ', lb_hab_fr, ' ', lb_hab_fr_complet)
FROM ref_habitats.habref h
JOIN ref_habitats.typoref t ON t.cd_typo = h.cd_typo;

COMMIT;

-- =========================================================
-- Fin du script
-- =========================================================

