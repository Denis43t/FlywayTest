WITH project_duration AS (
    SELECT
        id,
        EXTRACT(YEAR FROM AGE(project.finish_date,project.start_date)) * 12 +
        EXTRACT(MONTH FROM  AGE(project.finish_date,project.start_date))
            AS duration_in_months
        FROM project
)

SELECT
    p.id,
    p.start_date,
    p.finish_date,
    pd.duration_in_months
FROM
    project_duration pd
        JOIN
    project p ON pd.id = p.id
WHERE
    pd.duration_in_months = (SELECT MAX(duration_in_months) FROM project_duration);