use std::time::Instant;

pub enum Command {
    Replicate {
        job_id: String,
        path: String,
        created: Instant,
    },
}
