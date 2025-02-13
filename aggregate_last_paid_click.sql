WITH lead_data AS (
  SELECT 
    s.visitor_id as s_visitor_id,
    l.lead_id,
    COALESCE(l.created_at, s.visit_date) AS created_at,
    COALESCE(l.amount, 0) AS amount,
    COALESCE(l.closing_reason, '') AS closing_reason,
    COALESCE(l.status_id, NULL) AS status_id,
    MAX(s.visit_date) AS last_session_date
  FROM 
    leads l
  RIGHT JOIN 
    sessions s ON s.visitor_id = l.visitor_id
  GROUP BY 
    s_visitor_id, l.lead_id, l.created_at, l.amount, l.closing_reason, l.status_id, s.visit_date
),
final_result_ AS (
  SELECT 
    ld.s_visitor_id,
    ld.lead_id,
    ld.created_at,
    ld.amount,
    ld.closing_reason,
    ld.status_id,
    s.landing_page,
    s.source,
    s.medium,
    s.campaign,
    s.content,
    ld.last_session_date
  FROM 
    lead_data ld
  LEFT JOIN 
    sessions s ON ld.s_visitor_id = s.visitor_id AND ld.last_session_date = s.visit_date
),
combined_results_ya AS (
  SELECT 
    ya.campaign_date,
    ya.utm_source,
    ya.utm_medium,
    ya.utm_campaign,
    COUNT(*) AS total_count,
    SUM(ya.daily_spent) AS total_spent,
    COUNT(DISTINCT fd.lead_id) AS distinct_lead_count,
    fd.s_visitor_id,
    fd.lead_id,
    fd.created_at,
    fd.amount,
    fd.closing_reason,
    fd.status_id,
    ROW_NUMBER() OVER (PARTITION BY ya.campaign_date, ya.utm_source, ya.utm_medium, ya.utm_campaign) AS rn
  FROM 
    ya_ads ya
  LEFT JOIN 
    final_result_ fd ON CAST(ya.campaign_date AS DATE) = CAST(fd.created_at AS DATE)
  WHERE 
    (ya.utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') 
    OR ya.utm_medium IS NULL)
    AND (ya.utm_source = COALESCE(fd.source, '') 
    OR ya.utm_source IS NULL)
    AND (ya.utm_campaign = COALESCE(fd.campaign, '') 
    OR ya.utm_campaign IS NULL)
  GROUP BY 
    ya.campaign_date,
    ya.utm_source,
    ya.utm_medium,
    ya.utm_campaign,
    fd.s_visitor_id,
    fd.lead_id,
    fd.created_at,
    fd.amount,
    fd.closing_reason,
    fd.status_id
),
combined_results_vk AS (
  SELECT 
    vk.campaign_date,
    vk.utm_source,
    vk.utm_medium,
    vk.utm_campaign,
    COUNT(*) AS total_count,
    SUM(vk.daily_spent) AS total_spent,
    COUNT(DISTINCT fd.lead_id) AS distinct_lead_count,
    fd.s_visitor_id,
    fd.lead_id,
    fd.created_at,
    fd.amount,
    fd.closing_reason,
    fd.status_id,
    ROW_NUMBER() OVER (PARTITION BY vk.campaign_date, vk.utm_source, vk.utm_medium, vk.utm_campaign) AS rn
  FROM 
    vk_ads vk
  LEFT JOIN 
    final_result_ fd ON CAST(vk.campaign_date AS DATE) = CAST(fd.created_at AS DATE)
  WHERE 
    (vk.utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') 
    OR vk.utm_medium IS NULL)
    AND (vk.utm_source = COALESCE(fd.source, '') 
    OR vk.utm_source IS NULL)
    AND (vk.utm_campaign = COALESCE(fd.campaign, '') 
    OR vk.utm_campaign IS NULL)
  GROUP BY 
    vk.campaign_date,
    vk.utm_source,
    vk.utm_medium,
    vk.utm_campaign,
    fd.s_visitor_id,
    fd.lead_id,
    fd.created_at,
    fd.amount,
    fd.closing_reason,
    fd.status_id
),
rn_counts_ya AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT rn) AS distinct_rn_count,
    COUNT(DISTINCT s_visitor_id) AS distinct_visitor_ids
  FROM 
    combined_results_ya
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
rn_counts_vk AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT rn) AS distinct_rn_count,
    COUNT(DISTINCT s_visitor_id) AS distinct_visitor_ids
  FROM 
    combined_results_vk
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
distinct_lead_counts_ya AS (
  SELECT 
    cr.campaign_date,
    cr.utm_source,
    cr.utm_medium,
    cr.utm_campaign,
    COUNT(DISTINCT cr.lead_id) AS distinct_lead_count
  FROM 
    combined_results_ya cr
  GROUP BY 
    cr.campaign_date,
    cr.utm_source,
    cr.utm_medium,
    cr.utm_campaign
),
distinct_lead_counts_vk AS (
  SELECT 
    cr.campaign_date,
    cr.utm_source,
    cr.utm_medium,
    cr.utm_campaign,
    COUNT(DISTINCT cr.lead_id) AS distinct_lead_count
  FROM 
    combined_results_vk cr
  GROUP BY 
    cr.campaign_date,
    cr.utm_source,
    cr.utm_medium,
    cr.utm_campaign
),
status_counts_ya AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(CASE WHEN status_id = 142 THEN 1 END) AS status_142_count
  FROM 
    combined_results_ya
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
status_counts_vk AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(CASE WHEN status_id = 142 THEN 1 END) AS status_142_count
  FROM 
    combined_results_vk
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
distinct_142_counts_ya AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT CASE WHEN status_id = 142 THEN lead_id ELSE NULL END) AS distinct_142_leads
  FROM 
    combined_results_ya
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
distinct_142_counts_vk AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT CASE WHEN status_id = 142 THEN lead_id ELSE NULL END) AS distinct_142_leads
  FROM 
    combined_results_vk
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
purchase_counts_ya AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    MAX(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS max_purchase_value
  FROM 
    combined_results_ya
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
purchase_counts_vk AS (
  SELECT 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign,
    MAX(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS max_purchase_value
  FROM 
    combined_results_vk
  GROUP BY 
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign
),
final_result AS (
  SELECT 
    dr.campaign_date as visit_date,
    dr.utm_source,
    dr.utm_medium,
    dr.utm_campaign,
    rc.distinct_visitor_ids as visitors_count,
    dr.total_spent AS total_cost,
    dlc.distinct_lead_count as leads_count,
    sc.status_142_count purchases_count,
    pc.max_purchase_value as revenue
  FROM 
    combined_results_ya dr
  LEFT JOIN 
    rn_counts_ya rc ON dr.campaign_date = rc.campaign_date AND dr.utm_source = rc.utm_source AND dr.utm_medium = rc.utm_medium AND dr.utm_campaign = rc.utm_campaign
  LEFT JOIN 
    distinct_lead_counts_ya dlc ON dr.campaign_date = dlc.campaign_date AND dr.utm_source = dlc.utm_source AND dr.utm_medium = dlc.utm_medium AND dr.utm_campaign = dlc.utm_campaign
  LEFT JOIN 
    status_counts_ya sc ON dr.campaign_date = sc.campaign_date AND dr.utm_source = sc.utm_source AND dr.utm_medium = sc.utm_medium AND dr.utm_campaign = sc.utm_campaign
  LEFT JOIN 
    distinct_142_counts_ya dc ON dr.campaign_date = dc.campaign_date AND dr.utm_source = dc.utm_source AND dr.utm_medium = dc.utm_medium AND dr.utm_campaign = dc.utm_campaign
  LEFT JOIN 
    purchase_counts_ya pc ON dr.campaign_date = pc.campaign_date AND dr.utm_source = pc.utm_source AND dr.utm_medium = pc.utm_medium AND dr.utm_campaign = pc.utm_campaign
  WHERE 
    dr.rn = 1
  UNION ALL
  SELECT 
    dr.campaign_date as visit_date,
    dr.utm_source,
    dr.utm_medium,
    dr.utm_campaign,
    rc.distinct_visitor_ids as visitors_count,
    dr.total_spent AS total_cost,
    dlc.distinct_lead_count as leads_count,
    sc.status_142_count purchases_count,
    pc.max_purchase_value as revenue
  FROM 
    combined_results_vk dr
  LEFT JOIN 
    rn_counts_vk rc ON dr.campaign_date = rc.campaign_date AND dr.utm_source = rc.utm_source AND dr.utm_medium = rc.utm_medium AND dr.utm_campaign = rc.utm_campaign
  LEFT JOIN 
    distinct_lead_counts_vk dlc ON dr.campaign_date = dlc.campaign_date AND dr.utm_source = dlc.utm_source AND dr.utm_medium = dlc.utm_medium AND dr.utm_campaign = dlc.utm_campaign
  LEFT JOIN 
    status_counts_vk sc ON dr.campaign_date = sc.campaign_date AND dr.utm_source = sc.utm_source AND dr.utm_medium = sc.utm_medium AND dr.utm_campaign = sc.utm_campaign
  LEFT JOIN 
    distinct_142_counts_vk dc ON dr.campaign_date = dc.campaign_date AND dr.utm_source = dc.utm_source AND dr.utm_medium = dc.utm_medium AND dr.utm_campaign = dc.utm_campaign
  LEFT JOIN 
    purchase_counts_vk pc ON dr.campaign_date = pc.campaign_date AND dr.utm_source = pc.utm_source AND dr.utm_medium = pc.utm_medium AND dr.utm_campaign = pc.utm_campaign
  WHERE 
    dr.rn = 1
),
final_result_output AS (
  SELECT *
  FROM final_result
)
SELECT *
FROM final_result_output
ORDER BY 
    COALESCE(revenue, 0) DESC,
    visit_date ASC,
    visitors_count DESC,
	utm_source asc,
    utm_medium ASC,
    utm_campaign asc;