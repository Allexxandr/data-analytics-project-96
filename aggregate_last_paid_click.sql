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
    SELECT DISTINCT
        s.source,
        s.medium,
        s.campaign,
        CAST(s.visit_date AS DATE) AS date,
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
    SELECT DISTINCT
        cd.date,
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
            cd.date = CAST(ya_ads.campaign_date AS DATE)
            AND (cd.source = ya_ads.utm_source)
            AND (cd.medium = ya_ads.utm_medium)
            AND (cd.campaign = ya_ads.utm_campaign)
    GROUP BY
        date,
        source,
        medium,
        campaign,
        cd.visitors_count,
        cd.leads_count,
        cd.revenue,
        cd.purchases_count
    UNION ALL
    SELECT DISTINCT
        cd.date,
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
            cd.date = CAST(vk_ads.campaign_date AS DATE)
            AND (cd.source = vk_ads.utm_source)
            AND (cd.medium = vk_ads.utm_medium)
            AND (cd.campaign = vk_ads.utm_campaign)
    GROUP BY
        date,
        source,
        medium,
        campaign,
        cd.visitors_count,
        cd.leads_count,
        cd.revenue,
        cd.purchases_count
)
SELECT
    date AS visit_date,
    visitors_count,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    MAX(daily_spent) AS total_cost,
    leads_count,
    purchases_count,
    revenue
FROM final_query
GROUP BY
    date,
    visitors_count,
    revenue,
    source,
    medium,
    campaign,
    visitors_count,
    leads_count,
    purchases_count,
    revenue
ORDER BY 
    revenue desc,
    visit_date asc,
    visitors_count desc,
    source,
    medium,
    campaign;