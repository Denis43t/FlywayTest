SELECT
    p.id AS project_id,
    p.start_date,
    p.finish_date,
    SUM(w.salary) * (
        EXTRACT(YEAR FROM AGE(p.finish_date, p.start_date)) * 12 +
        EXTRACT(MONTH FROM AGE(p.finish_date, p.start_date))
        ) AS project_cost
FROM
    project p
        JOIN
    project_worker pw ON p.id = pw.project_id
        JOIN
    worker w ON pw.worker_id = w.id
GROUP BY
    p.id, p.start_date, p.finish_date
ORDER BY
    project_cost DESC;
