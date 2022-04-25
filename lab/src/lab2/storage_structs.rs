use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use tribbler::trib::Trib;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum FollowingActionType {
    FollowAction,
    UnfollowAction,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct FollowingLogEntry {
    // logical timestamp of when added to the following action list
    // should be unique for each entry per user. I.e. one user should
    // not have 2 following actions with the same clock
    pub clock: u64,
    // Follow or Unfollow action
    pub action: FollowingActionType,
    // Which user is this action done one
    pub whom: String,
}

impl Eq for FollowingLogEntry {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TribListEntry {
    pub trib: Trib,
}

// Note since TribListEntries will be compared (e.g. for sorting) across different users,
// make sure to implement full comparison of fields (e.g. using Tribble Order)
impl Ord for TribListEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Tribble order is clock, time, user, message
        match self.trib.clock.cmp(&other.trib.clock) {
            Ordering::Equal => match self.trib.time.cmp(&other.trib.time) {
                Ordering::Equal => match self.trib.user.cmp(&other.trib.user) {
                    Ordering::Equal => self.trib.message.cmp(&other.trib.message),
                    res => res,
                },
                res => res,
            },
            res => res,
        }
    }
}

impl Eq for TribListEntry {}

impl PartialOrd for TribListEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Tribble order is clock, time, user, message
        match self.trib.clock.partial_cmp(&other.trib.clock) {
            Some(Ordering::Equal) => match self.trib.time.partial_cmp(&other.trib.time) {
                Some(Ordering::Equal) => match self.trib.user.partial_cmp(&other.trib.user) {
                    Some(Ordering::Equal) => self.trib.message.partial_cmp(&other.trib.message),
                    res => res,
                },
                res => res,
            },
            res => res,
        }
    }
}

impl PartialEq for TribListEntry {
    fn eq(&self, other: &Self) -> bool {
        // Technically (user, logical clock) is sufficient for uniqueness in the way my front end
        // is posting tribs in lab 2, but just compare all just in case needed in the future somehow.
        // Order comparisons in a way such that short circuiting prevents longer comparisons
        self.trib.user == other.trib.user
            && self.trib.clock == other.trib.clock
            && self.trib.time == other.trib.time
            && self.trib.message == other.trib.message
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum EarlyRegistrationStatus {
    Valid,
    Invalid,
    // If uncertain, chase down source of truth to determine if can update this entry
    Uncertain,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EarlyRegistrationLogEntry {
    pub username: String,
    pub commit_status: EarlyRegistrationStatus,
}
