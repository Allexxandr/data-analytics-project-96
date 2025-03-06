WITH latest_sessions AS (
    SELECT
        visitor_id,
        MAX(visit_date) AS last_session_date
    FROM sessions
    WHERE
        LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'cpp', 'youtube', 'tg', 'social')
    GROUP BY visitor_id
),

combined_data AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.status_id,
        COALESCE(l.amount, 0) AS amount,
        COALESCE(l.closing_reason, '') AS closing_reason
    FROM sessions AS s
    INNER JOIN latest_sessions AS ls
        ON
            s.visitor_id = ls.visitor_id
            AND s.visit_date = ls.last_session_date
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND ls.last_session_date <= l.created_at
    GROUP BY
        s.visitor_id,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign
)

SELECT *
FROM combined_data
ORDER BY
    amount DESC,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC;