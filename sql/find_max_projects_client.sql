WITH client_project_count AS (
    SELECT
        client_id,
        COUNT(*) AS project_count
    FROM
        project
    GROUP BY
        client_id
)

SELECT
    c.name,
    cpc.project_count
FROM
    client_project_count cpc
        JOIN
    client c ON cpc.client_id = c.id
WHERE
    cpc.project_count = (SELECT MAX(project_count) FROM client_project_count);

