CREATE SEQUENCE IF NOT EXISTS seq_worker_id
    START WITH 1
    INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS seq_client_id
    START WITH 1
    INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS seq_project_id
    START WITH 1
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS worker(
                                     id BIGINT DEFAULT nextval('seq_worker_id'),
                                     name VARCHAR CHECK(length(name)>1 AND length(name)<1001) NOT NULL,
                                     birthday INTEGER CHECK (birthday>1900),
                                     level VARCHAR CHECK (level='Trainee' OR level='Junior'
                                         OR level='Middle' OR level='Senior' ) NOT NULL,
                                     salary INTEGER CHECK (salary>100 AND salary<100000 ),
                                     CONSTRAINT pk_worker_id PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS client(
                                     id BIGINT DEFAULT nextval('seq_client_id'),
                                     name VARCHAR CHECK(length(name)>1 AND length(name)<1000) NOT NULL,
                                     CONSTRAINT pk_client_id PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS project(
                                      id BIGINT DEFAULT nextval('seq_project_id'),
                                      start_date DATE,
                                      finish_date DATE,
                                      client_id BIGINT,
                                      CONSTRAINT pk_project_id PRIMARY KEY (id),
                                      CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES client(id)
);

CREATE TABLE IF NOT EXISTS project_worker(
                                             project_id BIGINT,
                                             worker_id BIGINT,
                                             CONSTRAINT fk_project_id FOREIGN KEY (project_id) REFERENCES project(id),
                                             CONSTRAINT fk_worker_id FOREIGN KEY (worker_id) REFERENCES worker(id),
                                             CONSTRAINT pk_project_worker PRIMARY KEY (project_id,worker_id)
);