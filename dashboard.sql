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
        s.source,
        s.medium,
        s.campaign,
        CAST(s.visit_date AS DATE) AS visit_day,
        COUNT(DISTINCT s.visitor_id) AS visitors_count,
        COUNT(l.lead_id) AS leads_count,
        COALESCE(SUM(l.amount), 0) AS revenue,
        SUM(CASE WHEN l.status_id = 142 THEN 1 ELSE 0 END) AS purchases_count
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
        CAST(s.visit_date AS DATE),
        s.source,
        s.medium,
        s.campaign
),

final_query AS (
    SELECT
        cd.visit_day,
        cd.source,
        cd.medium,
        cd.campaign,
        SUM(ya_ads.daily_spent) AS daily_spent,
        cd.visitors_count,
        cd.leads_count,
        cd.revenue,
        cd.purchases_count
    FROM combined_data AS cd
    LEFT JOIN ya_ads
        ON
            cd.visit_day = CAST(ya_ads.campaign_date AS DATE)
            AND (cd.source = ya_ads.utm_source)
            AND (cd.medium = ya_ads.utm_medium)
            AND (cd.campaign = ya_ads.utm_campaign)
    GROUP BY
        cd.visit_day,
        cd.source,
        cd.medium,
        cd.campaign,
        cd.visitors_count,
        cd.leads_count,
        cd.revenue,
        cd.purchases_count
    UNION ALL
    SELECT
        cd.visit_day,
        cd.source,
        cd.medium,
        cd.campaign,
        SUM(vk_ads.daily_spent) AS daily_spent,
        cd.visitors_count,
        cd.leads_count,
        cd.revenue,
        cd.purchases_count
    FROM combined_data AS cd
    LEFT JOIN vk_ads
        ON
            cd.visit_day = CAST(vk_ads.campaign_date AS DATE)
            AND (cd.source = vk_ads.utm_source)
            AND (cd.medium = vk_ads.utm_medium)
            AND (cd.campaign = vk_ads.utm_campaign)
    GROUP BY
        cd.visit_day,
        cd.source,
        cd.medium,
        cd.campaign,
        cd.visitors_count,
        cd.leads_count,
        cd.revenue,
        cd.purchases_count
)

SELECT
    visit_day AS visit_date,
    visitors_count,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    leads_count,
    purchases_count,
    revenue,
    MAX(daily_spent) AS total_cost,
    CASE
        WHEN visitors_count = 0 THEN 0 ELSE MAX(daily_spent) / visitors_count
    END AS cpu,
    CASE
        WHEN leads_count = 0 THEN 0 ELSE MAX(daily_spent) / leads_count
    END AS cpl,
    CASE
        WHEN purchases_count = 0 THEN 0 ELSE MAX(daily_spent) / purchases_count
    END AS cpp,
    CASE
        WHEN MAX(daily_spent) = 0 THEN 0 ELSE
            (revenue - MAX(daily_spent)) / MAX(daily_spent)
    END AS cpp
FROM final_query
GROUP BY
    visit_day,
    visitors_count,
    source,
    medium,
    campaign,
    visitors_count,
    leads_count,
    purchases_count,
    revenue
ORDER BY
    revenue DESC,
    visit_date ASC,
    visitors_count DESC,
    source ASC,
    medium ASC,
    campaign ASC;
