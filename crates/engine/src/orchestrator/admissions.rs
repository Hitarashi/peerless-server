//! Job admission limits and the per-job admission guard.

use super::*;

pub(super) struct Admission {
    pub(super) user_id: i64,
    pub(super) is_admin: bool,
    pub(super) group_id: Option<String>,
}

#[derive(Default)]
pub(super) struct Admissions {
    pub(super) jobs: HashMap<String, Admission>,
}

pub(super) struct AdmissionGuard {
    pub(super) admissions: Arc<Mutex<Admissions>>,
    pub(super) job_id: String,
    pub(super) armed: bool,
}

impl AdmissionGuard {
    pub(super) fn new(admissions: Arc<Mutex<Admissions>>, job_id: String) -> Self {
        Self {
            admissions,
            job_id,
            armed: true,
        }
    }

    pub(super) fn release(&mut self) {
        if self.armed {
            self.admissions
                .lock()
                .expect("admissions poisoned")
                .jobs
                .remove(&self.job_id);
            self.armed = false;
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl RipOrchestrator {
    #[cfg(test)]
    pub(super) fn admit(
        &self,
        job_id: &str,
        options: &RipTaskOptions,
    ) -> Result<(), OrchestratorError> {
        self.admit_in_group(job_id, options, None)
    }
}

impl RipOrchestrator {
    pub(super) fn admit_in_group(
        &self,
        job_id: &str,
        options: &RipTaskOptions,
        group_id: Option<&str>,
    ) -> Result<(), OrchestratorError> {
        let mut admissions = self.admissions.lock().expect("admissions poisoned");

        if !options.is_admin {
            let group_already_admitted = group_id.is_some_and(|group_id| {
                admissions.jobs.values().any(|admission| {
                    !admission.is_admin
                        && admission.user_id == options.user_id
                        && admission.group_id.as_deref() == Some(group_id)
                })
            });
            let mut global_groups = HashSet::new();
            let mut user_groups = HashSet::new();
            for (existing_job_id, admission) in &admissions.jobs {
                if admission.is_admin {
                    continue;
                }
                let group_key = admission
                    .group_id
                    .clone()
                    .unwrap_or_else(|| existing_job_id.clone());
                global_groups.insert((admission.user_id, group_key.clone()));
                if admission.user_id == options.user_id {
                    user_groups.insert(group_key);
                }
            }

            if !group_already_admitted && global_groups.len() >= 16 {
                return Err(OrchestratorError::AdmissionLimit);
            }
            if !group_already_admitted && user_groups.len() >= 4 {
                return Err(OrchestratorError::UserAdmissionLimit);
            }
        }
        admissions.jobs.insert(
            job_id.to_owned(),
            Admission {
                user_id: options.user_id,
                is_admin: options.is_admin,
                group_id: group_id.map(str::to_owned),
            },
        );
        Ok(())
    }
}

impl RipOrchestrator {
    pub fn release_admission(&self, job_id: &str) {
        self.admissions
            .lock()
            .expect("admissions poisoned")
            .jobs
            .remove(job_id);
    }
}
