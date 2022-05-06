use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Hash)]
pub enum BackendType {
    Primary,
    Backup,
    NotDefined,
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash)]
pub struct MarkedValue {
    pub backend_type: BackendType,
    pub backend_id: usize,
    // index for backups and clock for primary
    pub clock: u64,
    pub index: usize,
    pub value: String,
}

// Note since BackupValue will be compared (e.g. for sorting) to have consistent output among all backup,
// make sure to implement full comparison of fields
impl Ord for MarkedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        // BackupValue order is index, value
        match self.backend_id.cmp(&other.backend_id) {
            Ordering::Equal => match self.index.cmp(&other.index) {
                Ordering::Equal => self.value.cmp(&other.value),
                res => res,
            },
            res => res,
        }
    }
}

impl Eq for MarkedValue {}

impl PartialOrd for MarkedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // BackupValue order is index, value, their node should be the same for the entries being compared
        match self.backend_id.partial_cmp(&other.backend_id) {
            Some(Ordering::Equal) => match self.index.partial_cmp(&other.index) {
                Some(Ordering::Equal) => self.value.partial_cmp(&other.value),
                res => res,
            },
            res => res,
        }
    }
}

impl PartialEq for MarkedValue {
    fn eq(&self, other: &Self) -> bool {
        self.backend_id == other.backend_id
            && self.value == other.value
            && self.clock == other.clock
    }
}
