CREATE TABLE completed_jobs (
    job_id uuid PRIMARY KEY,
    source_key text NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shots (
    job_id uuid NOT NULL REFERENCES completed_jobs (job_id) ON DELETE CASCADE,
    shot_index integer NOT NULL CHECK (shot_index >= 0),
    start_us bigint NOT NULL CHECK (start_us >= 0),
    end_us bigint NOT NULL CHECK (end_us > start_us),
    frame_key text NOT NULL,
    PRIMARY KEY (job_id, shot_index)
);
