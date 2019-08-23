
---LISTENING AT VERSION_ID LEVEL - DATES WILL NEED TO BE CHANGED PER QUARTER

DROP TABLE IF EXISTS radio1_sandbox.ww_versionid_and_device_type_listening_uk_only;

CREATE TABLE radio1_sandbox.ww_versionid_and_device_type_listening_uk_only
AS
    (
        SELECT DISTINCT
                        audience_id,
                        a.version_id,
                        device_type,
                        broadcast_type,
                        master_brand_id,
                        format_names,
                        brand_id,
                        episode_id,
                        version_type,
                        CASE
                            WHEN brand_id != 'N/A' AND brand_id != '' AND brand_id != 'null' AND brand_id IS NOT NULL
                                THEN brand_id
                            WHEN series_id != 'N/A' AND series_id != '' AND series_id != 'null' AND
                                 series_id IS NOT NULL THEN series_id
                            WHEN episode_id != 'N/A' AND episode_id != '' AND episode_id != 'null' AND
                                 episode_id IS NOT NULL THEN episode_id
                            WHEN clip_id != 'N/A' AND clip_id != '' AND clip_id != 'null' AND clip_id IS NOT NULL
                                THEN clip_id
                            END                  AS tleo_id,
                        CASE
                            WHEN format_names LIKE '%Mixes%' OR
                                 episode_id IN (SELECT DISTINCT episode_id
                                                FROM central_insights.sounds_mixes_episodes_metadata_all) OR
                                 episode_id IN (SELECT DISTINCT episode_id
                                                FROM central_insights.sounds_mixes_metadata_schedule_positions)
                                THEN TRUE
                            ELSE FALSE END       as all_mixes_bool,
                        CASE
                            WHEN master_brand_id = 'bbc_sounds_podcasts' OR
                                 episode_id IN (
                                     SELECT DISTINCT episode_id
                                     FROM prez.scv_vmb
                                     WHERE version_type = 'Podcast version') OR
                                 tleo_id IN (
                                     SELECT DISTINCT tleo_id
                                     FROM central_insights.sounds_podcasts_metadata_schedule_positions
                                 ) OR
                                 format_names LIKE '%Podcast%'
                                THEN TRUE
                            ELSE FALSE END       as all_podcasts_bool,
                        SUM(playback_time_total) AS play_time

        FROM s3_audience.audience_activity_daily_summary a
                 lEFT JOIN prez.scv_vmb b
                           ON a.version_id = b.version_id

        WHERE dt BETWEEN 20190401 AND 20190623
          AND destination = 'PS_SOUNDS'
AND is_signed_in = TRUE
          AND playback_time_total >= 3
          AND geo_country_site_visited = 'United Kingdom'
AND av_content_type = 'Audio'

        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    );



--LISTENING WITH BOTH DEVICE TYPE AND FORMAT TYPE SPLIT
DROP TABLE IF EXISTS radio1_sandbox.ww_format_and_device_type_listening_uk_only;

CREATE TABLE radio1_sandbox.ww_format_and_device_type_listening_uk_only
AS
    (SELECT DISTINCT
         device_type,
         CASE
             WHEN all_podcasts_bool IS TRUE AND broadcast_type = 'Clip' THEN 'Podcast'
             WHEN all_mixes_bool IS TRUE AND broadcast_type = 'Clip' THEN 'Mixes'
             WHEN all_mixes_bool IS FALSE AND all_podcasts_bool IS FALSE AND broadcast_type = 'Clip'
                 THEN 'Catch-up-Radio'
             ELSE 'Live-Radio'
             END                     AS format_type,
         COUNT(DISTINCT audience_id) AS users,
         SUM(play_time)              AS play_time


     FROM radio1_sandbox.ww_versionid_and_device_type_listening_uk_only

     GROUP BY 1, 2
    );



--LISTENING WITH FORMAT TYPE SPLIT ONLY (NEEDED FOR AGGREGATED USER COUNT)
DROP TABLE IF EXISTS radio1_sandbox.ww_format_split_listening_uk_only;
CREATE TABLE radio1_sandbox.ww_format_split_listening_uk_only
AS
  (SELECT DISTINCT
         'all_devices' AS device_type,
         CASE
             WHEN all_podcasts_bool IS TRUE AND broadcast_type = 'Clip' THEN 'Podcast'
             WHEN all_mixes_bool IS TRUE AND broadcast_type = 'Clip' THEN 'Mixes'
             WHEN all_mixes_bool IS FALSE AND all_podcasts_bool IS FALSE AND broadcast_type = 'Clip'
                 THEN 'Catch-up-Radio'
             ELSE 'Live-Radio'
             END                     AS format_type,
         COUNT(DISTINCT audience_id) AS users,
         SUM(play_time)              AS play_time


     FROM radio1_sandbox.ww_versionid_and_device_type_listening_uk_only

     GROUP BY 1,2
    );


--LISTENING WITH DEVICE TYPE SPLIT ONLY (NEEDED FOR AGGREGATED USER COUNT)
DROP TABLE IF EXISTS radio1_sandbox.ww_device_split_listening_uk_only;
CREATE TABLE radio1_sandbox.ww_device_split_listening_uk_only
AS
  (SELECT DISTINCT
         device_type,
        'all_formats' AS format_type,
       /*  CASE
             WHEN all_podcasts_bool IS TRUE AND broadcast_type = 'Clip' THEN 'Podcast'
             WHEN all_mixes_bool IS TRUE AND broadcast_type = 'Clip' THEN 'Mixes'
             WHEN all_mixes_bool IS FALSE AND all_podcasts_bool IS FALSE AND broadcast_type = 'Clip'
                 THEN 'Catch-up-Radio'
             ELSE 'Live-Radio'
             END                     AS format_type,*/
         COUNT(DISTINCT audience_id) AS users,
         SUM(play_time)              AS play_time


     FROM radio1_sandbox.ww_versionid_and_device_type_listening_uk_only

     GROUP BY 1, 2
    );


--TOTAL AGGREGATED USER COUNT N.B TAKE TOTAL LISTENING FROM SUM OF LISTENING FROM ONE OF THE SPLIT TABLES
DROP TABLE IF EXISTS radio1_sandbox.ww_total_listening_uk_only;
CREATE TABLE radio1_sandbox.ww_total_listening_uk_only
AS
  (SELECT DISTINCT
         'all_devices' device_type,
        'all_formats' AS format_type,
         COUNT(DISTINCT audience_id) AS users,
         --SUM(play_time)              AS play_time  --- COMMENTED OUT BECAUSE THIS SUM IS WRONG (NOT SURE WHY)


     FROM radio1_sandbox.ww_versionid_and_device_type_listening_uk_only

     GROUP BY 1, 2
    );




------------- BELOW CODE IS FOR COMPARING TO OTHER DATA WE HAVE

-- COMPARING APP MIGRATION DASHBOARD DATA (SOUNDS APP AND IPR IOS):
SELECT
       app_name, live_aod_split, SUM(stream_playing_time)

FROM radio1_sandbox.sounds_app_migration_4_listening

WHERE day BETWEEN '2019-04-01' AND '2019-06-23'
AND ((LOWER(operating_system) = 'ios'
AND app_name = 'iplayer-radio') OR app_name = 'sounds')

 GROUP BY 1,2;


-- COMPARING SOUNDS MAIN DASHBOARD DATA (SOUNDS APP AND WEB)
SELECT live_aod_split, app_type, SUM(stream_playing_time) FROM radio1_sandbox.sounds_dashboard_5_listening
WHERE week_commencing BETWEEN '2019-04-01' AND '2019-06-23'

GROUP BY 1,2;


-- COMPARING AUDIENCE_CONTENT_ENRICHED DATA (DATA FOR SOUNDS APP AND WEB + iPR iOS) (THE SOUNDS DATA SHOULD BE THE SAME AS ABOVE)
SELECT  bbc_st_lod, all_mixes_bool, all_podcasts_bool, COUNT(DISTINCT hashed_id), SUM(stream_playing_time)

FROM radio1_sandbox.audio_content_enriched
WHERE day BETWEEN '2019-04-01' AND '2019-06-23'

GROUP BY 1,2,3
