WITH latest_sessions AS (
    SELECT visitor_id,
           MAX(visit_date) AS last_session_date
    FROM sessions
    WHERE LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'cpp', 'youtube', 'tg', 'social')
    GROUP BY visitor_id
)
    SELECT DISTINCT
	s.visitor_id,
	s.visit_date,
	s.source as utm_source,
    s.medium as utm_medium,
    s.campaign as utm_campaign,
    l.lead_id,
    l.created_at,
    COALESCE(l.amount, 0) AS amount,
    COALESCE(l.closing_reason, '') AS closing_reason,
    l.status_id
    FROM sessions s
    INNER JOIN latest_sessions ls
        ON s.visitor_id = ls.visitor_id
        AND s.visit_date = ls.last_session_date
    LEFT JOIN leads l
        ON s.visitor_id = l.visitor_id
        AND l.created_at >= ls.last_session_date
    GROUP by
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
     ORDER by
     	amount desc,
     	s.visit_date asc,
     	s.source,
        s.medium,
        s.campaign;

