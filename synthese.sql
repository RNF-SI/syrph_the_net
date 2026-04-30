-- synthese.sql propre au protocole Syrph The Net.
-- Vue dediee pour alimenter la synthese dans le cadre d'un protocole site-visite-observation.

DROP VIEW IF EXISTS gn_monitoring.v_synthese_syrph_the_net;

CREATE OR REPLACE VIEW gn_monitoring.v_synthese_syrph_the_net
AS WITH source AS (
    SELECT
        id_source
    FROM gn_synthese.t_sources
    WHERE name_source = CONCAT('MONITORING_', UPPER(:module_code))
    LIMIT 1
),
sites AS (
    SELECT
        id_base_site,
        base_site_name,
        base_site_code,
        altitude_min,
        altitude_max,
        geom AS the_geom_4326,
        ST_CENTROID(geom) AS the_geom_point,
        geom_local
    FROM gn_monitoring.t_base_sites
),
visits AS (
    SELECT
        id_base_visit,
        uuid_base_visit,
        id_module,
        id_base_site,
        id_dataset,
        id_digitiser,
        visit_date_min AS date_min,
        COALESCE(visit_date_max, visit_date_min) AS date_max,
        comments
    FROM gn_monitoring.t_base_visits
),
observers AS (
    SELECT
        array_agg(r.id_role) AS ids_observers,
        STRING_AGG(CONCAT(r.nom_role, ' ', r.prenom_role), ' ; ') AS observers,
        cvo.id_base_visit
    FROM gn_monitoring.cor_visit_observer cvo
    JOIN utilisateurs.t_roles r
      ON r.id_role = cvo.id_role
    GROUP BY cvo.id_base_visit
)
SELECT
    -- Champs obligatoires
    o.uuid_observation AS unique_id_sinp,
    obs.ids_observers,
    o.id_observation,
    o.id_observation AS entity_source_pk_value,
    -- Autres champs standard synthese
    v.uuid_base_visit AS unique_id_sinp_grp,
    source.id_source,
    v.id_module,
    v.id_dataset,
    v.id_digitiser,
    ref_nomenclatures.get_id_nomenclature('NAT_OBJ_GEO', 'St') AS id_nomenclature_geo_object_nature,
    ref_nomenclatures.get_id_nomenclature('TYP_GRP', 'POINT') AS id_nomenclature_grp_typ,
    ref_nomenclatures.get_id_nomenclature('TECH_OBS', '100') AS id_nomenclature_obs_technique,
    ref_nomenclatures.get_id_nomenclature('ETA_BIO', '1') AS id_nomenclature_bio_condition,
    CASE
        WHEN tm.uuid_attached_row IS NOT NULL THEN ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '1')
        ELSE ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '2')
    END AS id_nomenclature_exist_proof,
    ref_nomenclatures.get_id_nomenclature('OBJ_DENBR', 'IND') AS id_nomenclature_obj_count,
    ref_nomenclatures.get_id_nomenclature('STATUT_OBS', 'Pr') AS id_nomenclature_observation_status,
    ref_nomenclatures.get_id_nomenclature('STATUT_SOURCE', 'Te') AS id_nomenclature_source_status,
    ref_nomenclatures.get_id_nomenclature('TYP_INF_GEO', '1') AS id_nomenclature_info_geo_type,
    COALESCE(
        NULLIF(json_extract_path(oc.data::json, 'effectif_total')::text, 'null')::integer,
        COALESCE(NULLIF(json_extract_path(oc.data::json, 'effectif_males')::text, 'null')::integer, 0)
        + COALESCE(NULLIF(json_extract_path(oc.data::json, 'effectif_femelles')::text, 'null')::integer, 0)
    ) AS count_min,
    COALESCE(
        NULLIF(json_extract_path(oc.data::json, 'effectif_total')::text, 'null')::integer,
        COALESCE(NULLIF(json_extract_path(oc.data::json, 'effectif_males')::text, 'null')::integer, 0)
        + COALESCE(NULLIF(json_extract_path(oc.data::json, 'effectif_femelles')::text, 'null')::integer, 0)
    ) AS count_max,
    o.cd_nom,
    t.nom_complet AS nom_cite,
    s.altitude_min,
    s.altitude_max,
    s.the_geom_4326,
    s.the_geom_point,
    s.geom_local AS the_geom_local,
    v.date_min,
    v.date_max,
    COALESCE(obs.observers, NULLIF(json_extract_path(vc.data::json, 'observers_txt')::text, 'null')) AS observers,
    v.comments AS comment_context,
    o.comments AS comment_description,
    -- Colonnes techniques utiles pour synthese.import_row_from_table
    v.id_base_site,
    v.id_base_visit,
    -- Informations complementaires conservees
    json_build_object(
        'site_nom', s.base_site_name,
        'site_code', s.base_site_code,
        'effectif_males', json_extract_path(oc.data::json, 'effectif_males'),
        'effectif_femelles', json_extract_path(oc.data::json, 'effectif_femelles'),
        'effectif_total', json_extract_path(oc.data::json, 'effectif_total'),
        'determinateur', json_extract_path(oc.data::json, 'determinateur'),
        'validateur_local', json_extract_path(oc.data::json, 'validateur_local'),
        'code_habitat_principal', json_extract_path(sc.data::json, 'CodeHabitatPrincipal'),
        'code_habitat_secondaire_1', json_extract_path(sc.data::json, 'CodeHabitatSecondaire1'),
        'code_habitat_secondaire_2', json_extract_path(sc.data::json, 'CodeHabitatSecondaire2'),
        'code_habitat_secondaire_3', json_extract_path(sc.data::json, 'CodeHabitatSecondaire3'),
        'code_habitat_secondaire_4', json_extract_path(sc.data::json, 'CodeHabitatSecondaire4'),
        'recolteur_saisi', json_extract_path(vc.data::json, 'observers_txt')
    ) AS additional_data
FROM gn_monitoring.t_observations o
JOIN gn_monitoring.t_observation_complements oc USING (id_observation)
JOIN visits v USING (id_base_visit)
JOIN gn_monitoring.t_visit_complements vc USING (id_base_visit)
JOIN sites s USING (id_base_site)
JOIN gn_monitoring.t_site_complements sc USING (id_base_site)
JOIN gn_commons.t_modules m ON m.id_module = v.id_module
JOIN taxonomie.taxref t USING (cd_nom)
JOIN source ON TRUE
LEFT JOIN observers obs USING (id_base_visit)
LEFT JOIN gn_commons.t_medias tm
  ON tm.id_table_location = gn_commons.get_table_location_id('gn_monitoring', 't_observations')
 AND tm.uuid_attached_row = o.uuid_observation
WHERE m.module_code = :module_code;
