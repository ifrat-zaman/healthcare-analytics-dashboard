-- analysis_queries.sql — Six analytical queries for the hospital analytics dashboard.
-- Each query is identified by a sentinel comment: -- [query_N: query_name]

-- [query_1: avg_los_by_service_and_age_group]
SELECT
    service,
    age_group,
    ROUND(AVG(length_of_stay)::NUMERIC, 2) AS avg_los
FROM patients
GROUP BY service, age_group
ORDER BY service, avg_los DESC;

-- [query_2: weekly_occupancy_by_service]
SELECT
    week,
    service,
    occupancy_rate,
    patients_refused,
    event
FROM services_weekly
ORDER BY week, service;

-- [query_3: refusal_analysis]
SELECT
    week,
    service,
    event,
    available_beds,
    patients_request,
    patients_refused,
    refusal_rate
FROM services_weekly
ORDER BY patients_refused DESC
LIMIT 20;

-- [query_4: event_impact_comparison]
SELECT
    event,
    ROUND(AVG(patient_satisfaction)::NUMERIC, 2) AS avg_patient_satisfaction,
    ROUND(AVG(staff_morale)::NUMERIC, 2)          AS avg_staff_morale,
    ROUND(AVG(patients_admitted)::NUMERIC, 2)      AS avg_patients_admitted,
    ROUND(AVG(patients_refused)::NUMERIC, 2)       AS avg_patients_refused,
    ROUND(AVG(occupancy_rate)::NUMERIC, 2)         AS avg_occupancy_rate
FROM services_weekly
GROUP BY event
ORDER BY event;

-- [query_5: staff_attendance_by_role_and_service]
SELECT
    role,
    service,
    ROUND(SUM(present::int)::NUMERIC / COUNT(*) * 100, 2) AS attendance_rate
FROM staff_schedule
GROUP BY role, service
ORDER BY attendance_rate ASC;

-- [query_6: staffing_vs_satisfaction]
SELECT
    sw.week,
    sw.service,
    COALESCE(SUM(ss.present::int), 0) AS staff_present_count,
    sw.patient_satisfaction,
    sw.staff_morale,
    sw.occupancy_rate
FROM services_weekly sw
LEFT JOIN staff_schedule ss
    ON sw.week = ss.week
    AND sw.service = ss.service
GROUP BY sw.week, sw.service, sw.patient_satisfaction, sw.staff_morale, sw.occupancy_rate
ORDER BY sw.week, sw.service;
