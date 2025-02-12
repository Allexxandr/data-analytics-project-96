WITH lead_data AS (
    SELECT
        s.visitor_id AS visitor_id,
        l.created_at AS created_at,
        l.amount AS amount,
        COALESCE(MAX(l.closing_reason), '') AS closing_reason,
        l.status_id AS status_id,
        coalesce(MAX(s.visit_date)) AS visit_date,
        l.lead_id AS lead_id
    FROM leads l
    RIGHT JOIN sessions s ON l.visitor_id = s.visitor_id
    GROUP BY
        s.visitor_id,
        s.visit_date,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.created_at,
        l.status_id
),
final_result AS (
    SELECT
        ld.visitor_id,
        ld.visit_date,
        s.source,
        s.medium,
        s.campaign,
        ld.lead_id,
        ld.created_at,
        ld.amount,
        ld.closing_reason,
        ld.status_id
    FROM lead_data ld
    LEFT JOIN sessions s ON ld.visitor_id = s.visitor_id AND ld.visit_date = s.visit_date
),
max_last_session AS (
    SELECT visitor_id, MAX(visit_date) as max_visit_date
    FROM final_result
    GROUP BY visitor_id
)
SELECT
    fr.visitor_id,
    fr.visit_date,
    fr.source AS utm_source,
    fr.medium AS utm_medium,
    fr.campaign AS utm_campaign,
    fr.lead_id,
    fr.created_at,
    fr.amount,
    fr.closing_reason,
    fr.status_id
FROM final_result fr
JOIN max_last_session mls ON fr.visitor_id = mls.visitor_id AND fr.visit_date = mls.max_visit_date
WHERE 
    LOWER(fr.medium) IN ('cpc', 'cpm', 'cpa', 'cpp', 'youtube', 'tg', 'social') OR
    LOWER(fr.source) IN ('social', 'tg') 
ORDER BY
    COALESCE(amount, 0) DESC,
    visit_date ASC,
    amount NULLS last,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign asc;