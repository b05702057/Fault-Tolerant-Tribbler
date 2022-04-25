use crate::lab2::storage_structs::{
    EarlyRegistrationLogEntry, EarlyRegistrationStatus, FollowingActionType, FollowingLogEntry,
    TribListEntry,
};
use async_trait::async_trait;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

use tribbler::{
    err::{TribResult, TribblerError},
    storage::{BinStorage, KeyValue, Storage},
    trib::{
        is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
    },
};

// Garbage collection constants
const TRIBS_GARBAGE_COLLECT_THRESHOLD: usize = MAX_TRIB_FETCH + 5;
// Use this constant to control the chance of tribs garbage collection (1 in
// TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER chance).
// This is to avoid having all successive posts of a user to constantly do garbage collection
// of mostly the same outdated keys. Prevents inundation of requests to the user's bin.
const TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER: usize = 3;
// Sometimes the user may simply be following many users which leads to long logs, but this may
// not necessarily have many outdated/unnecessary entries. Thus it is better to require a
// condition where the following log is greater than the resolved log by some amount for GC.
const FOLLOWING_LOG_DIFFERENCE_RESOLVED_LOG_GARBAGE_COLLECTION_THRESHOLD: usize = 5;
// Same idea as for TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER above
const FOLLOWING_LOG_GARBAGE_COLLECTION_CHANCE_PARAMETER: usize = 3;

// String constants for special key, key prefixes, and bin name prefixes.
// Full keys/names end in "_#", and prefixes end in "_#_"
// Make sure to check to avoid conflicts (e.g. avoid having one string
// be a prefix of another)
const USER_EXISTS_KEY: &str = "_existing_user_#";
const USER_EXISTS_VALUE: &str = "true";
const EARLY_REGISTRATION_BIN_NAME: &str = "_early_registration_bin_#";
const EARLY_REGISTRATION_LOG_KEY: &str = "_early_registration_log_#";
const FOLLOWING_ACTION_LIST_KEY: &str = "_following_action_list_#";
const TRIBS_LIST_KEY: &str = "_tribs_list_#"; // For posts

/// Notes:
/// - Make sure all special keys have some characters that is disallowed
///   by usernames to avoid confusion. E.g. "_" is a good prefix.
///
/// --------------------------------------------------------------------------------
/// ------------------------------DESIGN DECISIONS----------------------------------
/// --------------------------------------------------------------------------------
///
/// -----------Registration, Checking for user existence, and list_users()--------
///
/// - Below describes the workflow and steps needed to register. Read everything since
///   the reasoning for these steps are not necessarily explained in the beginning.
///
/// - The workflow path for any user request differs whether
///   cached_sorted_list_users is < MIN_LIST_USER or >= MIN_LIST_USER.
///
/// - For sign up, if cached_sorted_list_users < MIN_LIST_USER:
///     - Check the source of truth location to see if user is already signed up.
///       The source of truth or commit point for a registration is done by using
///       the bin whose name is equal to the user we are setting up, and then setting the
///       USER_EXISTS_KEY to "true". If the user is already in there, then return error
///       UsernameTaken. Otherwise continue with below.
///     - Retrieve the registration list (which will only be for the first MIN_LIST_USER
///       users) from predefined location. This location's bin name is
///       EARLY_REGISTRATION_BIN_NAME and list key is EARLY_REGISTRATION_LOG_KEY.
///       The registration status can be evaluated by looking at each field and checking
///       the "commit_status" of the entry. If it is undetermined, then check the source
///       of truth / commit location (read on to see how) and update the field. Then only
///       those with "commit_status" = committed will be considered registered. Make sure
///       to account for duplicate registrations for the same user in the evaluation, since
///       this is possible the (i.e. only consider  duplicate registration as 1 registration
///       count).
///     - Check if that list is already MIN_LIST_USER registered after evaluation. ONLY IF
///       that evaluation is not already MIN_LIST_USER do we do the following:
///           - Create/append a placeholder entry in that list resistration for the
///             first MIN_LIST_USER registrations. This placeholder entry's validity is not
///             yet confirmed (until the registration commit status is determined).
///     - Now we can commit the registration by setting the source of truth. Notice that it
///       is possible for concurrent registration commits for the same user may happen
///       and both return successful for sign_up, but this is allowed by the requirements.
///       Sequential sign up for the same user will be prevented though (by the first step when
///       checking the source of truth).
///     - If we previously created a placeholder in the registration list, then now is the
///       time to set its commit status to true. If this fails or front end shuts down, no issue.
///     - Note the front end could optionally cache the retrieved registration list in
///       cached_sorted_list_users in sign_up(). This is not required to achieve the requirements
///       since list_users() will be responsible for populating this cache as well.
///     - Note the creation of placeholder entry is needed in case we crash before we committed,
///       we can refer to the source of truth for commit status. This is because we have 2 info
///       related to registrations in different places (registration list and source of truth
///       locations). Because the commit action needs to be only 1 write and we want to
///       efficiently lookup if the user exists especially later on, we choose the bin name
///       being user and USER_EXISTS_CACHE_KEY location to be the source of truth (only 1 source
///       of truth possible given requirements).
///     - This registration list is needed because calls to list_user() will defer to this if
///       cached_sorted_list_users is not at MIN_LIST_USER length yet. Otherwise, list_user()
///       has no way to determine registered users (unless by guessing all possible usernames and
///       checking those bins for existence, which would be unrealistic). Hence we have the 2
///       registration info locations.
///     - Since list_user() only neeeds at most MIN_LIST_USER, once registration list evaluates
///       to 20 registered, there is no longer a need to append to it for sign_up. See below:
///
/// - For sign up, if cached_sorted_list_users >= MIN_LIST_USER:
///     - Simply check the source of truth to see if user is already signed up. If yes, return
///       UsernameTaken error. If not directly commit to source of truth location. Again,
///       concurrent registrations for the same user is possible, but allowed by requirements.
///
/// - Thus, to check user existence, only need to go to the source of truth. Simple!
///
/// - List users will have to do something similar to sign_up tho:
///     - If cached_sorted_list_users < MIN_LIST_USER:
///         - Similar procedure to a part seen in sign_up case. Check and evaluate the registration
///           list. Update the cached_sorted_list_users. Return result.
///     - Else (if cached_sorted_list_users >= MIN_LIST_USER):
///         - Just return cache.
///
///
/// -----------------------------Which bin to use for requests?------------------------------
/// - All actions for a user/who (instead of whom) such as adding to a user's posts or
///   follow action list will be done in a bin whose name is the user's username.
/// - This method brings some guarantees. First, all said operations for a user will be
///   on the same bin, and thus same backend. This means the clock() function is always
///   gives advancing values for sequential calls; e.g. we can guarantee properties such as:
///     "If tribble A is posted after tribble B, and they are both posted by the same user,
///     A always shows after B", where
///     "A is posted after B means A calls post() after B’s post() has returned."
///   This is important since sequential clock() on different bins does not guarantee
///   advancing values (since keeper syncing backends frequency is not infinitely fast).
/// - This method also allows us to take advantage of properties to do things such as
///   resolve concurrent following actions taken by a user (see Following/Unfollowing)
///   since all actions of user is in the same bin / backend, whose values will be used
///   as serialization order.
///
///
/// (Read previous section first to see how bin usage gives properties we utilize here)
/// Following/Unfollowing:
/// - A per user log of both following and unfollowing actions (both in the same log per
///   user) is maintained. The storage key to this log is FOLLOWING_ACTION_LIST_KEY.
/// - The order of the entries in the log is the serialization guaranteed by the storage
///   and thus clients should use it as source of truth when resolving any races. Note,
///   the clock number associated is used for unique identifying purposes and not the
///   serialization order. In particular a log (action, logical_time) could have something
///   like:
///     ..., (unfollow C, 12), (follow B, 15), (follow B, 13).
///     Note the logical time need not be sorted and is not used for serialization ordering
///     in the following actions log.
/// - These actions are uniquely identified by their logical clock value, which is unique
///   since it will be generated by clock()
/// - Example of concurrency and how we deal with races:
///   If two A follow B requests occurs concurrently and is appended to A's log, then
///   only the one that is closest to the last action seen action on B (or beginning of
///   log if none exists) in A's log when we first fetched it should return successful to
///   the browser request.
///     - For example: Say A's log (action, logical_time) entries is currently:
///         Log: ..., (unfollow B, 10), (follow C, 12)
///     - Now 2 requests see that we can follow B, leading to execution of 2 concurrent appends:
///         Log: ..., (unfollow B, 10), (follow C, 12), (follow B, 15), (follow B, 13)
///     - Then, only the client that appended (follow B, 15) should return successful, since
///       it is closer to the last action on B i.e. (unfollow B, 10).
///     - Thus, when processing a request, we should fetch the log, save this last
///       action (unfollow B, 10) before appending, and then refetch the log to see the situation.
///     - Note when fetching the lock, we only check for races on the same user and
///       the same type as our action. In this case, the concurrent follow request senders should
///       only check if any follow actions (and ignore unfollow) are in between them and their
///       prior save of last relevant action (i.e. (unfollow B, 10))
///     - This concept applies both to follow and unfollow requests (but check for race actions
///       of same type only). See below for more explanations.
/// - IMPORTANT: We only check for races of the same action type (follow/unfollow) as the
///   action type we are processing in between us and the last seen action.
///     - For example: Say A's log (action, logical_time) entries is currently:
///         Log: ..., (follow B, 10)
///     - Now we append (unfollow B, 11), but this is the resulting og:
///         Log: ..., (follow B, 10), (follow B, 14), (unfollow B, 11)
///     - How could ther be a (follow B 14) in between? Well concurrent appends could lead to
///       this result, and the first time we fetched the log happened to be before the
///       (follow B, 14) reached and written in the log.
///     - Now note this (unfollow B, 11) should still be considered successful. So we cannot
///       consider (follow B, 14) to be a concurrent race that "beat" us to serialization.
///     - Thus when checking for any races, only check on same action. I.e. only if our log was
///       something like this after append, then yes, we lost to a concurrent request:
///         Log: ..., (follow B, 10), (follow B, 14), (unfollow B, 12), (unfollow B, 11)
///         The (unfollow B, 11) is beaten by (unfollow B, 12) since it is the same action as ours
///         and is in between (unfollow B, 11) and the last action noted on B when first reading
///         log i.e. (follow B, 10)
/// - Note again in the examples only the last same action type on the same user B is checked for
///   races, not on any user since we want to allow concurrent following/unfollowing action on
///   different users (e.g. A follow B and A follow C) to succeed.
/// - Side note: Naturally the last saved action before we determine that we can append will be
///   opposite to ours (e.g. we wouldn't unfollow someone we haven't followed and vice versa).
///   This property is not required for the correctness of the race checking we described.
/// - Garbage collection is described in the follow() and
///   garbage_collect_following_log_if_needed() functions
///
///
/// ----------------------------------Tribs, Posting, and Home----------------------------------
/// - For each user, their tribs are stored in their own bin as a list with storage key being
///   TRIBS_LIST_KEY. They are uniquely identified (within each user's list) by logical time.
/// - The order in which to display tribs is the Tribble Order (see write up), which prioritizes
///   logical timestamp unlike the case for the follow/unfollow actions log.
/// - This is because concurrent posts are fine in any order, as long as that order stays
///   consistent for user. Thus, this makes it easy to just sort all posts based on logical clock,
///   especially when used in home() and sorting with other users' (potential on other backends)
///   posts as well (assuming keeper keeps backends roughly in sync). This consistent sorting of
///   tribs, prioritizing logical timestamps, allow us to satisfy both requirement 2 and
///   requirement 3 for home() in the writeup:
///         Requirement 2:    (satisisfied thanks to rough synchronization by keeper)
///         - If tribble A is posted at least 10 seconds after tribble B, even if they are posted
///           by different users, A always shows after B.
///         Requirement 3:    (satisfied thanks to consistent/deterministic sorting order)
///         - If tribble A is posted after a user client sees tribble B, A always shows after B.
/// - Note the requirement that sequential (non-concurrent) posts by the same user is still
///   achieved since all posts for a user go to the trib list in their own bin implying it goes
///   through the same backend, and thus the logical timestamp retrieved ensures absolute
///   uniqueness in logical clock() values. Thus if user posted tribB sequentially after tribA
///   then tribA will always have lower/earlier logical timestamp. This satisfies requirement 1
///   for home() in the writeup:
///         Requirement 1:
///         - If tribble A is posted after tribble B, and they are both posted by the same user,
///           A always shows after B. A is posted after B means A calls post() after B’s post()
///           has returned.
/// - Although as described requirement 2 is satisfied from just this sorting property, we can
///   improve the experience of the user by calling x <= clock(max_clock_seen_by_user) on the bin
///   and use the result x as our logical time to have things be closer to actual ordering of
///   events across DIFFERENT users posts in the real world. This is because there may be duplicate
///   logical time of posts across different users' posts that we don't have within only one user's
///   posts list.
///   Note we need to call clock() anyway to get a unique (within the the user's trib list) logical
///   time for our trib so this does not cost performance. max_clock_seen_by_user is the the clock
///   field passed to us when user calls post().
///
/// - Garbage collection is considered outdated tribs is done once TRIBS_GARBAGE_COLLECT_THRESHOLD
///   in length is surpassed upon a post. Note "considered" is because even after passing this
///   threshold (which must be >= MAX_TRIB_FETCH, ideally somewhat > MAX_TRIB_FETCH to avoid
///   the threshold being passed every post after MAX_TRIB_FETCH), there is only a 1 in
///   TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER chance that the garbage collection request is carried
///   out. This is to avoid having all successive posts of a user to constantly do garbage collection
///   of mostly the same outdated keys and prevents inundation of repeated requests to the user's bin.
pub struct TribFront {
    pub bin_storage: Box<dyn BinStorage>,

    /// Cached users seen through list_users. Since users cannot
    /// be deleted once created, this caching makes sense. Spreads the
    /// load across front ends. Note this cache information will
    /// be populated as needed and not the sourced of truth (most recent
    /// state of all users registered) and thus the front end is still safe
    /// to be killed any time. Hold max of 20.
    cached_sorted_list_users: RwLock<Vec<String>>,
}

// Helper functions that may be reused
impl TribFront {
    pub fn new(bin_storage: Box<dyn BinStorage>) -> TribFront {
        TribFront {
            bin_storage: bin_storage,
            cached_sorted_list_users: RwLock::new(vec![]),
        }
    }

    // Short function to make code more readable.
    // Checks source of truth / commit point for registration and return bool answer.
    async fn user_exists(bin_of_user: &Box<dyn Storage>) -> TribResult<bool> {
        Ok(bin_of_user.get(USER_EXISTS_KEY).await? == Some(USER_EXISTS_VALUE.to_string()))
    }

    // See the Registration design notes for TribFront
    // Fetches and resolves the state of the early registration log
    // Also sort, dedupe, and truncate to MIN_LIST_USER length at most.
    // Truncation is needed since there may be concurrent appends to early registration list when it is
    // at MIN_LIST_USER - 1 in resolved length.
    // IMPORTANT: assumes the passed in bin is the correct registration bin.
    async fn get_and_resolve_early_registration_log_and_sort_and_truncated(
        &self,
        early_reg_bin: &Box<dyn Storage>,
    ) -> TribResult<Vec<String>> {
        let early_reg_log_serialized: Vec<String> =
            early_reg_bin.list_get(EARLY_REGISTRATION_LOG_KEY).await?.0;
        let res: Result<Vec<EarlyRegistrationLogEntry>, _> = early_reg_log_serialized
            .iter()
            .map(|entry_serialized| serde_json::from_str(entry_serialized))
            .collect();
        let early_reg_log_entries: Vec<EarlyRegistrationLogEntry> = res?;

        let mut evaluated_registered_usernames = vec![];
        for reg_entry in early_reg_log_entries.iter() {
            match reg_entry.commit_status {
                EarlyRegistrationStatus::Valid => {
                    evaluated_registered_usernames.push(reg_entry.username.clone())
                }
                EarlyRegistrationStatus::Invalid => (),
                EarlyRegistrationStatus::Uncertain => {
                    let bin_of_uncertain_user = self.bin_storage.bin(&reg_entry.username).await?;
                    if bin_of_uncertain_user.get(USER_EXISTS_KEY).await?
                        == Some(USER_EXISTS_VALUE.to_string())
                    {
                        // If commited, then add to resolved registered list. Then asyncly send update of reg status to reg list
                        evaluated_registered_usernames.push(reg_entry.username.clone());

                        // Asynchronously update validity in registration list. No need to wait or care if errors
                        match self.bin_storage.bin(EARLY_REGISTRATION_BIN_NAME).await {
                            Ok(new_early_reg_bin) => Self::update_registration_entry_validity(
                                new_early_reg_bin,
                                &reg_entry.username,
                                EarlyRegistrationStatus::Valid,
                            ),
                            Err(_) => (), // Ignore error
                        }
                    }
                }
            }

            // NOTE DO NOT BREAK EARLY HERE BASED ON evaluated_registered_usernames.lne() SINCE THERE MAY BE DUPLICATES
        }

        // Sort, deduplicate, and truncate to MIN_LIST_USER at most. Dedup() assumes sorted vec (see rust docs)
        // Truncation is needed since there may be concurrent appends to early registration list when it is
        // at MIN_LIST_USER - 1 in resolved length.
        evaluated_registered_usernames.sort();
        evaluated_registered_usernames.dedup();
        evaluated_registered_usernames.truncate(MIN_LIST_USER);
        Ok(evaluated_registered_usernames)
    }

    // Update cached users by fetching and resolving the registration log if the registered users are
    // greater than those in cache
    async fn update_cached_users_if_needed(&self) -> TribResult<()> {
        let early_reg_bin = self.bin_storage.bin(EARLY_REGISTRATION_BIN_NAME).await?;
        let sorted_fetched_registered_usernames: Vec<String> = self
            .get_and_resolve_early_registration_log_and_sort_and_truncated(&early_reg_bin)
            .await?;

        let mut cached_sorted_list_users = self.cached_sorted_list_users.write().await;

        // Only update if greater in length, since cached_sorted_list_users may have been
        // recently updated to be even longer already.
        if sorted_fetched_registered_usernames.len() > cached_sorted_list_users.len() {
            *cached_sorted_list_users = sorted_fetched_registered_usernames;
        }

        Ok(())
    }

    // Spawns async task to do this.
    // Note does not return anything. Don't care if successful or not since if EarlyRegistrationStatus
    // is still Uncertain then someone else can check and update it later
    fn update_registration_entry_validity(
        early_reg_bin: Box<dyn Storage>,
        user: &str,
        new_reg_status: EarlyRegistrationStatus,
    ) {
        // Noone will check the result of this spawn
        let user_str = user.to_string();
        tokio::spawn(async move {
            let updated_reg_entry = EarlyRegistrationLogEntry {
                username: user_str,
                commit_status: new_reg_status,
            };
            // Don't care about errors
            let updated_reg_entry_serialized = match serde_json::to_string(&updated_reg_entry) {
                Ok(val) => val,
                Err(_) => return,
            };
            let _ = early_reg_bin
                .list_append(&KeyValue {
                    key: EARLY_REGISTRATION_LOG_KEY.to_string(),
                    value: updated_reg_entry_serialized,
                })
                .await;
        });
    }

    // Retrieve following log from bin and return a deserialized vector of log entries
    async fn get_following_log(
        bin_of_user: &Box<dyn Storage>,
    ) -> TribResult<Vec<FollowingLogEntry>> {
        let following_log_list = bin_of_user.list_get(FOLLOWING_ACTION_LIST_KEY).await?;
        let following_log_vec = following_log_list.0;

        let res: Result<Vec<FollowingLogEntry>, _> = following_log_vec
            .iter()
            .map(|following_action| serde_json::from_str(following_action))
            .collect();
        let following_action_vec = res?;
        Ok(following_action_vec)
    }

    // Returns the log of actions but only keeping the last most entry of action on each
    // whom, maintaining relative ordering of those entries.
    fn resolve_current_following_state(
        following_log: &[FollowingLogEntry],
    ) -> Vec<FollowingLogEntry> {
        // Algorithm: Traverse following_log in reverse and maintain a result vector. Only push the first
        // entry for action on each unique user. Reverse the stored results before returning.

        // Populate in reverse order to log and then reverse vector before returning.
        let mut results: Vec<FollowingLogEntry> = vec![];

        // Holds user where we have seen an entry on them already
        let mut seen = HashSet::new();
        for following_log_entry in following_log.iter().rev() {
            if !seen.contains(&following_log_entry.whom) {
                seen.insert(following_log_entry.whom.clone());
                results.push(following_log_entry.clone());
            }
        }

        // Important: reverse back to match relative order in following_log.
        // Maybe currently doesn't matter since action on user will be unique
        // here but just in case need same relative log ordering in the future.
        results.reverse();
        results
    }

    // Fetch following action log, resolve state, and extract current followees (users that
    // current user follows) and return their usernames.
    async fn get_resolved_followees_only(
        bin_of_user: &Box<dyn Storage>,
    ) -> TribResult<Vec<String>> {
        let following_log: Vec<FollowingLogEntry> = Self::get_following_log(bin_of_user).await?;
        // Last follow or unfollow actions only
        let resolved_following_entries: Vec<FollowingLogEntry> =
            Self::resolve_current_following_state(&following_log);

        // Filter out unfollow action and only extract list of whom for follows.
        let followees: Vec<String> = resolved_following_entries
            .into_iter()
            .filter_map(|entry| {
                // Filter map will filter out None values. Hence only keep
                // following action to extract followees
                if entry.action == FollowingActionType::FollowAction {
                    return Some(entry.whom);
                } else {
                    return None;
                }
            })
            .collect();

        Ok(followees)
    }

    // Create following (both follow and unfollow) entry action and serialize to string
    fn serialize_following_action(
        clock: u64,
        action: FollowingActionType,
        whom: &str,
    ) -> TribResult<String> {
        let following_log_entry = FollowingLogEntry {
            clock: clock,
            action: action,
            whom: whom.to_string(),
        };
        let following_log_entry_ser = serde_json::to_string(&following_log_entry)?;
        Ok(following_log_entry_ser)
    }

    // Fetch at most the MAX_TRIB_FETCH trib entries from user
    async fn get_most_recent_trib_entries_from_user_bin(
        bin_of_user: Box<dyn Storage>,
    ) -> TribResult<Vec<TribListEntry>> {
        // Simply use Tribble Order (see lab 2) for sorting before returning response.
        // Read Tribs and Posting section in design notes for TribFront

        // Fetch the tribs list of the user
        let res_list = bin_of_user.list_get(TRIBS_LIST_KEY).await?;
        let res_vec: Vec<String> = res_list.0; // vec of TribListEntry serialized strings

        // Deserialize list and sort in TribListEntry format first (since we didnt define Ord for Trib)
        let deserialized_list_result: Result<Vec<TribListEntry>, _> = res_vec
            .iter()
            .map(|serialized_entry| serde_json::from_str(serialized_entry))
            .collect();
        let mut trib_entries = deserialized_list_result?;
        trib_entries.sort();

        // Collect from drain_idx_start to the end of trib_entries
        // Ensure the range [drain_idx_start, trib_entries.len() - 1] inclusive cover at most MAX_TRIB_FETCH entries
        let mut drain_idx_start: i32 = trib_entries.len() as i32 - MAX_TRIB_FETCH as i32;
        if drain_idx_start < 0 {
            drain_idx_start = 0;
        }
        let most_recent_trib_entries: Vec<TribListEntry> =
            trib_entries.drain(drain_idx_start as usize..).collect();

        Ok(most_recent_trib_entries)
    }

    // No caller should wait for the results here. Should wrap this in
    // a tokio spawn for example to let the task run in the background.
    // Check if user's tribs length has reached a threshold for garbage
    // collection and if so this function itself will spawn async tasks
    // to do key removal of the no longer necessary tribs.
    // Note there is only a 1 in TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER chance of
    // moving forward to avoid excesscie duplicate garbage collection attempts from
    // successive posts by the same user.
    async fn garbage_collect_tribs_if_needed(bin_of_user: Box<dyn Storage>) -> TribResult<()> {
        // Fetch the tribs list of the user
        let res_list = bin_of_user.list_get(TRIBS_LIST_KEY).await?;
        let res_vec: Vec<String> = res_list.0; // vec of TribListEntry serialized strings

        // Only garbage collect if garbage collect length threshold is exceeded
        if res_vec.len() < TRIBS_GARBAGE_COLLECT_THRESHOLD {
            return Ok(());
        }

        // Only have a 1 in TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER chance of actually doing garbage collection.
        // This prevents quick successive user posts from flooding requests to the same bin to remove
        // mostly the same outdated entries.
        // Also if res_vec.len() is >= 8 + TRIBS_GARBAGE_COLLECT_THRESHOLD, then let the chance be 100%, since
        // we consider that getting way too long.
        if res_vec.len() < 8 + TRIBS_GARBAGE_COLLECT_THRESHOLD {
            let mut rng = rand::thread_rng();
            if rng.gen_range(1..=TRIBS_GARBAGE_COLLECTION_CHANCE_PARAMETER) > 1 {
                return Ok(()); // return since didnt get selected to move forward with GC.
            }
        }

        // Deserialize list and sort in TribListEntry format first (since we didnt define Ord for Trib)
        let deserialized_list_result: Result<Vec<TribListEntry>, _> = res_vec
            .iter()
            .map(|serialized_entry| serde_json::from_str(serialized_entry))
            .collect();
        let mut trib_entries = deserialized_list_result?;

        // Important: Sort based on tribble order
        trib_entries.sort();

        // Scale this with how excessive the length is.
        // If trib_entries is getting too long though, choosing absolute trib_entries.len() - TRIBS_GARBAGE_COLLECT_THRESHOLD
        // will help bring it down to exact TRIBS_GARBAGE_COLLECT_THRESHOLD after GC. Ideally we want to be in the first
        // slowly scaling case (trib_entries.len() - MAX_TRIB_FETCH) / 2) being the max which means it would bring the list
        // length down below TRIBS_GARBAGE_COLLECT_THRESHOLD
        let num_entries_to_garbage_collect = std::cmp::max(
            (trib_entries.len() - MAX_TRIB_FETCH) / 2,
            trib_entries.len() - TRIBS_GARBAGE_COLLECT_THRESHOLD,
        );

        // Keep only up to NUM_ENTRIES_TO_GARBAGE_COLLECT_AT_A_TIME outdated entries to do garbage collection
        // NUM_ENTRIES_TO_GARBAGE_COLLECT_AT_A_TIME is used to avoid sending too many requests at once.
        trib_entries.truncate(num_entries_to_garbage_collect);

        // To clone and move into tasks
        let bin_of_user_arc = Arc::new(bin_of_user);

        // Spawn async tasks to remove outdated entries. Don't wait for result
        for entry_to_gc in trib_entries {
            let bin_of_user_arc_clone = Arc::clone(&bin_of_user_arc);
            tokio::spawn(async move {
                bin_of_user_arc_clone
                    .list_remove(&KeyValue {
                        key: TRIBS_LIST_KEY.to_string(),
                        value: serde_json::to_string(&entry_to_gc)?,
                    })
                    .await
            });
        }

        Ok(())
    }

    // Should not need to wait for this function to return. Caller should wrap in a tokio::spawn for example
    // This can be done in the background. This function itself will spawn more tasks that it does wait for
    // completion synchronously.
    // Checks if the following_log is excessive by a certain threshold (has some amount of unnecessary
    // entries greater than a threshold) before deciding on GC.
    // Flip a FOLLOWING_LOG_GARBAGE_COLLECTION_CHANCE_PARAMETER-sided coin to decide to move forward with GC.
    // Assumes resolved_log entries are the ones that must not be GC. Every entry in following_log and not in
    // resolved_log is fair game for GC.
    async fn garbage_collect_following_log_if_needed(
        bin_of_user: Box<dyn Storage>,
        following_log: Vec<FollowingLogEntry>,
        resolved_log: Vec<FollowingLogEntry>,
    ) -> TribResult<()> {
        // Only GC if difference is greater than some threshold.
        if following_log.len() - resolved_log.len()
            < FOLLOWING_LOG_DIFFERENCE_RESOLVED_LOG_GARBAGE_COLLECTION_THRESHOLD
        {
            return Ok(());
        }

        // Only have a 1 in FOLLOWING_LOG_GARBAGE_COLLECTION_CHANCE_PARAMETER chance of moving forward
        // with GC, unless the difference is too large then 100%.
        if following_log.len() - resolved_log.len()
            < 2 * FOLLOWING_LOG_DIFFERENCE_RESOLVED_LOG_GARBAGE_COLLECTION_THRESHOLD
        {
            let mut rng = rand::thread_rng();
            if rng.gen_range(1..=FOLLOWING_LOG_GARBAGE_COLLECTION_CHANCE_PARAMETER) > 1 {
                return Ok(()); // return since didnt get selected to move forward with GC.
            }
        }

        // Keep a set of the resolved entries logical timestamps, which should uniquely identify and map 1-1 to a
        // resolved FollowingLogEntry. Then it will be more efficent to check which entries in following log are not
        // in resolved log
        let resolved_log_set: HashSet<u64> =
            resolved_log.into_iter().map(|entry| entry.clock).collect();

        // To clone and move into tasks
        let bin_of_user_arc = Arc::new(bin_of_user);

        for entry in following_log {
            // Again, clock uniquely identifies an entry. If an entry in following log is not
            // in the resolved log, it is outdated.
            if !resolved_log_set.contains(&entry.clock) {
                let bin_of_user_arc_clone = Arc::clone(&bin_of_user_arc);
                tokio::spawn(async move {
                    bin_of_user_arc_clone
                        .list_remove(&KeyValue {
                            key: FOLLOWING_ACTION_LIST_KEY.to_string(),
                            value: serde_json::to_string(&entry)?,
                        })
                        .await
                });
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Server for TribFront {
    /// Creates a user.
    /// Returns error when the username is invalid;
    /// returns error when the user already exists.
    /// Concurrent sign ups on the same user might both succeed with no error.
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }

        let bin_of_user = self.bin_storage.bin(user).await?;

        // Now check if username taken from source of truth
        if Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }

        // Now check the cached list size to determine workflow

        let cached_sorted_list_users = self.cached_sorted_list_users.read().await;

        // Workflow is different for < MIN_LIST_USER and >= MIN_LIST_USER
        if (*cached_sorted_list_users).len() < MIN_LIST_USER {
            // Don't hold lock.
            drop(cached_sorted_list_users);

            let early_reg_bin = self.bin_storage.bin(EARLY_REGISTRATION_BIN_NAME).await?;
            let early_sorted_registered_usernames: Vec<String> = self
                .get_and_resolve_early_registration_log_and_sort_and_truncated(&early_reg_bin)
                .await?;

            // If the actually registered username is already MIN_LIST_USER, only need to check source of truth
            if early_sorted_registered_usernames.len() >= MIN_LIST_USER {
                // Try to update cache since likely our cache is still < MIN_LIST_USER since we are in
                // the bigger if condition (unless someone recently update int).
                let _ = self.update_cached_users_if_needed().await; // It's ok if cache is not updated.
                                                                    // Can directly commit registration
                let sign_up_success = bin_of_user
                    .set(&KeyValue {
                        key: USER_EXISTS_KEY.to_string(),
                        value: USER_EXISTS_VALUE.to_string(),
                    })
                    .await?;
                // Check for err (see storage set interface comment, if res = false, then also error)
                if !sign_up_success {
                    return Err("Error upon set USER_EXISTS_KEY to commit user registration".into());
                }
                // Once we're here, must return success
                // Success!
                return Ok(());
            }

            // Otherwise, if the registered list is not MIN_LIST_USER, we also need to append to it
            // when processing a sign up

            let new_reg_entry = EarlyRegistrationLogEntry {
                username: user.to_string(),
                commit_status: EarlyRegistrationStatus::Uncertain,
            };

            let new_reg_entry_serialized = serde_json::to_string(&new_reg_entry)?;
            let append_reg_entry_success = early_reg_bin
                .list_append(&KeyValue {
                    key: EARLY_REGISTRATION_LOG_KEY.to_string(),
                    value: new_reg_entry_serialized,
                })
                .await?;
            // According to Storage trait signature description, a false returned could also indicate error.
            // I.e. TribResult<bool> type is returned, so in addition to the possible Err() result, the Ok(false)
            // could also indicate error setting value
            if !append_reg_entry_success {
                // If failed, mark entry as invalid
                // don't care about this result since not gonna keep trying to mark invalid
                Self::update_registration_entry_validity(
                    early_reg_bin,
                    user,
                    EarlyRegistrationStatus::Invalid,
                );
                return Err("Error upon list_append to add user to early registration log".into());
            }

            // Commit point now. Source of truth is at bin of user at the USER_EXISTS_KEY location.
            let sign_up_success = bin_of_user
                .set(&KeyValue {
                    key: USER_EXISTS_KEY.to_string(),
                    value: USER_EXISTS_VALUE.to_string(),
                })
                .await?;
            // Check for err (see storage set interface comment, if res = false, then also error)
            if !sign_up_success {
                // If failed, mark entry as invalid
                // don't care about this result since not gonna keep trying to mark invalid
                Self::update_registration_entry_validity(
                    early_reg_bin,
                    user,
                    EarlyRegistrationStatus::Invalid,
                );
                return Err("Error upon set USER_EXISTS_KEY to commit user registration".into());
            }
            // IMPORTANT From this point forwards, must NOT return error. Not returning because crash is ok since
            // network partitions can happen anway but returning error after committed regisration is unacceptable.

            // It's fine if not successful, since upon seeing EarlyRegistrationStatus::Uncertain, others will still
            // have to refer to source of truth location
            Self::update_registration_entry_validity(
                early_reg_bin,
                user,
                EarlyRegistrationStatus::Valid,
            );

            // Update cache for next time. Do NOT propagate error.
            let _ = self.update_cached_users_if_needed().await;

            return Ok(()); // MUST return success here
        } else {
            // Don't hold lock.
            drop(cached_sorted_list_users);

            // Commit point now. Source of truth is at bin of user.
            let sign_up_success = bin_of_user
                .set(&KeyValue {
                    key: USER_EXISTS_KEY.to_string(),
                    value: USER_EXISTS_VALUE.to_string(),
                })
                .await?;
            // Check for err (see storage set interface comment, if res = false, then also error)
            if !sign_up_success {
                return Err("Error upon set USER_EXISTS_KEY to commit user registration".into());
            }

            return Ok(());
        }
    }

    /// List 20 registered users.  When there are less than 20 users that
    /// signed up the service, all of them needs to be listed.  When there
    /// are more than 20 users that signed up the service, an arbitrary set
    /// of at lest 20 of them needs to be listed.
    /// The result should be sorted in alphabetical order.
    async fn list_users(&self) -> TribResult<Vec<String>> {
        // Check self.cached_sorted_list_users first.
        let mut cached_sorted_list_users = self.cached_sorted_list_users.write().await;

        // If already have MIN_LIST_USER, can return just the cached list. Technically this cached list
        // should never be > MIN_LIST_USER.
        if (*cached_sorted_list_users).len() >= MIN_LIST_USER {
            return Ok((*cached_sorted_list_users).clone());
        }

        // Note: cached_sorted_list_users lock is purposefully held while fetching from
        // registration bins so that other concurrent requests to the same frontend
        // pending for the lock will benefit from the populated user list cache instead of
        // also having to fetch from registration bins themselves.

        let early_reg_bin = self.bin_storage.bin(EARLY_REGISTRATION_BIN_NAME).await?;
        let sorted_users = self
            .get_and_resolve_early_registration_log_and_sort_and_truncated(&early_reg_bin)
            .await?;

        *cached_sorted_list_users = sorted_users.clone();

        Ok(sorted_users)
    }

    /// Post a tribble.  The clock is the maximum clock value this user has
    /// seen so far by reading tribbles or clock sync.
    /// Returns error when who does not exist;
    /// returns error when post is too long.
    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        // Bin of "who"
        let bin_of_user = self.bin_storage.bin(who).await?;

        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        // Check length limit
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }

        // Call x <= clock(clock) to get a logical time >= clock and use for the trib post
        // This tries to get the ordering of the posts across DIFFERENT users as close as possible
        // to real world ordering (since there may be duplicate logical time of posts across
        // different users' posts that we don't have within each user's posts list)
        // Read Tribs and Posting section in design notes for TribFront.
        let new_clock = bin_of_user.clock(clock).await?;

        let post_entry = TribListEntry {
            trib: Trib {
                clock: new_clock,
                time: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
                user: who.to_string(),
                message: post.to_string(),
            },
        };
        // Serialize the post
        let post_serialized = serde_json::to_string(&post_entry)?;
        // Insert the trib post to the list
        let post_success = bin_of_user
            .list_append(&KeyValue {
                key: TRIBS_LIST_KEY.to_string(),
                value: post_serialized,
            })
            .await?;
        // According to Storage trait signature description, a false returned could also indicate error.
        // I.e. TribResult<bool> type is returned, so in addition to the possible Err() result, the Ok(false)
        // could also indicate error setting value
        if !post_success {
            return Err("Error upon list_append to tribs log".into());
        }

        // From here onwards MUST RETURN SUCCESS

        // If needed, Garbage collect posts. Spawn async task to do so.
        // Note we are not actually waiting for tokio spawn handle to return and don't care
        // about results.
        tokio::spawn(async move {
            let _ = Self::garbage_collect_tribs_if_needed(bin_of_user).await;
        });

        Ok(())
    }

    /// List the tribs that a particular user posted.
    /// Returns error when user has not signed up.
    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        // Bin of user
        let bin_of_user = self.bin_storage.bin(user).await?;

        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        // Simply use Tribble Order (see lab 2) for sorting before returning response.
        // Read Tribs and Posting section in design notes for TribFront

        let most_recent_trib_entries =
            Self::get_most_recent_trib_entries_from_user_bin(bin_of_user).await?;

        // Convert to return format Vec<Arc<Trib>>>
        let trib_list_to_return: Vec<Arc<Trib>> = most_recent_trib_entries
            .into_iter()
            .map(|entry| Arc::new(entry.trib))
            .collect();

        Ok(trib_list_to_return)
    }

    /// Follow someone's timeline.
    /// Returns error when who == whom;
    /// returns error when who is already following whom;
    /// returns error when who is trying to following
    /// more than trib.MaxFollowing users.
    /// returns error when who or whom has not signed up.
    /// Concurrent follows might both succeed without error.
    /// The count of following users might exceed trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generated by concurrent Follow()
    /// calls.
    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        // This function is especially long mainly because of logic to handle concurrent following
        // actions on the same user (e.g. 2 concurrent A follow B, A follow B).

        // Note later on we don't call self.is_following() but check it a different way
        // for efficiency purposes, since we are able to extract that information the following
        // actions log etc. we need anyway in this function

        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }

        // Bin of "who"
        let bin_of_user = self.bin_storage.bin(who).await?;

        // Check both who and whom are signed up
        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        // Bin of "whom"
        let bin_of_whom = self.bin_storage.bin(whom).await?;
        if !Self::user_exists(&bin_of_whom).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let following_log: Vec<FollowingLogEntry> = Self::get_following_log(&bin_of_user).await?;

        // Vector of followees after resolving list of follow actions
        // It is resolved according to ORDER in list and NOT the logical clock
        // order. This is because the order in list is serialized by the storage
        // and is the serialization all clients will thus take as source of truth.
        // See Following/Unfollowing under Notes for TribFront for more info.
        // resolved_following_entries should contain at most 1 log entry per user, which is
        // the latest entry for that user.
        let resolved_following_entries: Vec<FollowingLogEntry> =
            Self::resolve_current_following_state(&following_log);

        // Possible garbage collection in background. Only CONSIDER executing if following_log is some amount
        // longer than resolved_following_entries, which guarantees the difference in entries are number of unnecessary
        // entries in log. This prevents unneccesarily frequent garbage collection.
        // Simply request removes of some/all entries that are in following_log but not in resolved_following_entries.
        let bin_of_user_for_following_gc = self.bin_storage.bin(who).await?;
        let following_log_clone = following_log.clone();
        let resolved_following_entries_clone = resolved_following_entries.clone();
        tokio::spawn(async move {
            let _ = Self::garbage_collect_following_log_if_needed(
                bin_of_user_for_following_gc,
                following_log_clone,
                resolved_following_entries_clone,
            )
            .await;
        });

        // Keep track of the last most entry holding an action on "whom".
        // Could be None if doesn't exist in log (hence Option<>).
        // This is needed since later on we need to get log again and make sure the item
        // we set for whom is not directly adjacent to this exact entry to consider it successful.
        // This is for the requirement that only one follow on the same whom will return successful.
        // - E.g. If two A follow B requests occurs concurrently and is appended to A's log, then
        //   only the one that is closest to the last follow/unfollow action on B in A's log should
        //   return successful to the browser request.
        // See Following/Unfollowing under Notes for TribFront for more info.
        // Note if entry of an action on whom exists guaranteed to be last entry in following_log since
        // the resolved_following_entries can contain at most one entry of action on a particular whom
        let last_entry_for_whom_prior_to_append: Option<FollowingLogEntry> =
            match resolved_following_entries
                .iter()
                .position(|entry| entry.whom == whom)
            {
                Some(idx) => Some(resolved_following_entries[idx].clone()),
                None => None,
            };

        // A vector holding following actions that only contains follow (and not unfollow) actions.
        // Essentially the list of current entries for current followees.
        let resolved_following_entries_follow_only: Vec<FollowingLogEntry> =
            resolved_following_entries
                .iter()
                .filter(|&entry| entry.action == FollowingActionType::FollowAction)
                .cloned()
                .collect();

        // Check to see if already following
        if resolved_following_entries_follow_only
            .iter()
            .any(|entry| entry.whom == whom)
        {
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        // Note, it's ok to have concurrent follow to exceed MAX_FOLLOWING so we
        // are checking before the list_append. Racing a follow on the same whom
        // will still be acceptable if we only return success if we are the first
        // of those to be accepted, even if it is concurrent with follows on other
        // users which will bring the total to exceed MAX_FOLLOWING.
        // This is why checking size limit here is ok
        if resolved_following_entries_follow_only.len() >= MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }

        // After all error checking above, can now attempt to add follow action to log.
        // Get clock first and then call list_append to add following action.
        // Get logical timestamp
        let clock = bin_of_user.clock(0).await?;

        // Get serialized following action log entry
        let follow_action_entry_ser =
            Self::serialize_following_action(clock, FollowingActionType::FollowAction, whom)?;

        // Insert the action to the following log.
        let res = bin_of_user
            .list_append(&KeyValue {
                key: FOLLOWING_ACTION_LIST_KEY.to_string(),
                value: follow_action_entry_ser,
            })
            .await?;
        // According to Storage trait signature description, a false returned could also indicate error.
        // I.e. TribResult<bool> type is returned, so in addition to the possible Err() result, the Ok(false)
        // could also indicate error setting value
        if !res {
            return Err("Error upon list_append to follow entry".into());
        }

        // Now get log again to check for race conditions for following same whom
        let following_log_after_append: Vec<FollowingLogEntry> =
            Self::get_following_log(&bin_of_user).await?;

        // Now check following actions log again to make sure new entry set is the closest SAME action as
        // ours to last action on whom. Read Following/Unfollowing notes of TribFront to see why we only
        // check for races of SAME action.
        // If not return AlreadyFollowing since a concurrent follow beat us to the serialization order.
        // To do this, first find our recently added entry in the log.
        // Then look backwards and see if there is any races of the SAME action as ours on
        // whom between ours and the last action saved.
        // Note following_log_after_append is not resolved so this going in reverse process is important
        // since our saved last action on whom (prior to our append) in following_log_after_append could be much
        // greater than in the resolved log.

        // Note on entry is uniquely identified by clock
        let idx_of_our_entry: usize =
            match following_log_after_append
                .iter()
                .position(|entry| entry.clock == clock)
            {
                Some(idx) => idx,
                None => return Err(
                    "Missing our entry despite already passed receiving success fromm list_append!"
                        .into(),
                ),
            };

        // Go in reverse from our append entry index until either we reach the last saved action (in which
        // case we are good) or some SAME action as ours on whom found first (in which case we are beaten
        // by a concurrent request).
        // Important to start search at idx_of_our_entry - 1 to avoid checking our own entry.
        // Use i32 since need to go to reach -1 to break from while loop.
        let mut search_idx: i32 = idx_of_our_entry as i32 - 1i32;
        while search_idx >= 0 {
            let search_entry: &FollowingLogEntry = &following_log_after_append[search_idx as usize];

            // Only see if we reached last save entry for break if last_entry_for_whom_prior_to_append
            // is not None (i.e. such an entry exists whe we first fetched)
            if let Some(last_saved_relevant_entry) = &last_entry_for_whom_prior_to_append {
                // If reached here, no races in between us and last saved entry.
                if search_entry == last_saved_relevant_entry {
                    return Ok(());
                }
            }

            // Note only check for the SAME action in races to ignore new opposite action appends.
            // In this case it is the FollowingActionType::FollowAction action.
            // Read Following/Unfollowing notes examples for TribFront for more info.
            // Of course, also only care about actions on same whom.
            if search_entry.action == FollowingActionType::FollowAction && search_entry.whom == whom
            {
                // Some concurrent follow beat us to the serialization point.
                return Err(Box::new(TribblerError::AlreadyFollowing(
                    who.to_string(),
                    whom.to_string(),
                )));
            }
            search_idx -= 1;
        }

        // - If reached here, either last_entry_for_whom_prior_to_append is None, or we did not see it again
        //   when we refetched the log. In both cases, we return success. Reasoning:
        // - The first case is obvious that it is fine to declare the operation successful.
        // - The latter case could be caused by garbage removal, only keeping the latest action. Example:
        //    - Say we want to unfollow B (same concept applies to follow, but use unfollow as ex here).
        //    - We first fetch log and see:
        //         ..., (following B, 10)
        //    - We save (following B, 10) as last relevant actionl. However, the log could have been
        //      concurrently appended with a non-racing action to ours and become:
        //         ..., (following B, 10), (following B, 12)
        //    - Then when we append our action, say (unfollow B, 14) we get:
        //         ..., (following B, 10), (following B, 12), (unfollow B, 14)
        //    - At the same time garbage collector comes and removes (following B, 10) (and possibly
        //      (following B, 12) as well).
        //    - Then when we refetch the log, the saved (following B, 10) is missing.
        //    - Thus, as long as there is no racing condition in between us and the beginning of the log
        //      our (unfollow B, 14) as is the case here, our unfollow should still be allowed to succeed.
        //    - The same concept applies to the opposite case when we try to follow.
        Ok(())
    }

    /// Unfollow someone's timeline.
    /// Returns error when who == whom.
    /// returns error when who is not following whom;
    /// returns error when who or whom has not signed up.
    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        // Function very similar to follow() logic but with opposite actions (follow / unfollow).
        // A difference is unfollow doesnt have to check max length.

        // Note later on we don't call self.is_following() but check it a different way
        // for efficiency purposes, since we are able to extract that information the following
        // actions log etc. we need anyway in this function

        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }

        // Bin of "who"
        let bin_of_user = self.bin_storage.bin(who).await?;

        // Check both who and whom are signed up
        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        // Bin of "whom"
        let bin_of_whom = self.bin_storage.bin(whom).await?;
        if !Self::user_exists(&bin_of_whom).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let following_log: Vec<FollowingLogEntry> = Self::get_following_log(&bin_of_user).await?;

        // Vector of followees after resolving list of follow actions
        // It is resolved according to ORDER in list and NOT the logical clock
        // order. This is because the order in list is serialized by the storage
        // and is the serialization all clients will thus take as source of truth.
        // See Following/Unfollowing under Notes for TribFront for more info.
        // resolved_following_entries should contain at most 1 log entry per user, which is
        // the latest entry for that user.
        let resolved_following_entries: Vec<FollowingLogEntry> =
            Self::resolve_current_following_state(&following_log);

        // Keep track of the last most entry holding an action on "whom".
        // Could be None if doesn't exist in log (hence Option<>).
        // This is needed since later on we need to get log again and make sure the item
        // we set for whom is not directly adjacent to this exact entry to consider it successful.
        // This is for the requirement that only one follow on the same whom will return successful.
        // - E.g. If two A follow B requests occurs concurrently and is appended to A's log, then
        //   only the one that is closest to the last follow/unfollow action on B in A's log should
        //   return successful to the browser request.
        // See Following/Unfollowing under Notes for TribFront for more info.
        // Note if entry of an action on whom exists guaranteed to be last entry in following_log since
        // the resolved_following_entries can contain at most one entry of action on a particular whom
        let last_entry_for_whom_prior_to_append: Option<FollowingLogEntry> =
            match resolved_following_entries
                .iter()
                .position(|entry| entry.whom == whom)
            {
                Some(idx) => Some(resolved_following_entries[idx].clone()),
                None => None,
            };

        // Note here we do the exact same (not mirrored version) of the logic in follow() and extract
        // the follow only list and not unfollow only. This is because although existence of unfollow
        // in resolved list (e.g. last action) tells us that we are not following a user, a non-existence
        // of unfollows doesn't tell us conclusively if we are following a user or not (e.g. example we
        // start with a log with no unfollows then we can only tell that we are not following a user
        // if we don't see a FOLLOW entry).
        // A vector holding following actions that only contains follow (and not unfollow) actions.
        // Essentially the list of current entries for current followees.
        let resolved_following_entries_follow_only: Vec<FollowingLogEntry> =
            resolved_following_entries
                .iter()
                .filter(|&entry| entry.action == FollowingActionType::FollowAction)
                .cloned()
                .collect();

        // If not already following, can't unfollow.
        // Already following if not existing in follow only resulting list
        if !resolved_following_entries_follow_only
            .iter()
            .any(|entry| entry.whom == whom)
        {
            return Err(Box::new(TribblerError::NotFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        // After all error checking above, can now attempt to add follow action to log.
        // Get clock first and then call list_append to add following action.
        // Get logical timestamp
        let clock = bin_of_user.clock(0).await?;

        // Get serialized following action log entry
        let unfollow_action_entry_ser =
            Self::serialize_following_action(clock, FollowingActionType::UnfollowAction, whom)?;

        // Insert the action to the following log.
        let res = bin_of_user
            .list_append(&KeyValue {
                key: FOLLOWING_ACTION_LIST_KEY.to_string(),
                value: unfollow_action_entry_ser,
            })
            .await?;
        // According to Storage trait signature description, a false returned could also indicate error.
        // I.e. TribResult<bool> type is returned, so in addition to the possible Err() result, the Ok(false)
        // could also indicate error setting value
        if !res {
            return Err("Error upon list_append to unfollow entry".into());
        }

        // Now get log again to check for race conditions for following same whom
        let following_log_after_append: Vec<FollowingLogEntry> =
            Self::get_following_log(&bin_of_user).await?;

        // Now check following actions log again to make sure new entry set is the closest SAME action as
        // ours to last action on whom. Read Following/Unfollowing notes of TribFront to see why we only
        // check for races of SAME action.
        // If not return NotFollowing since a concurrent unfollow beat us to the serialization order.
        // To do this, first find our recently added entry in the log.
        // Then look backwards and see if there is any races of the SAME action as ours on
        // whom between ours and the last action saved.
        // Note following_log_after_append is not resolved so this going in reverse process is important
        // since our saved last action on whom (prior to our append) in following_log_after_append could be much
        // greater than in the resolved log.

        // Note on entry is uniquely identified by clock
        let idx_of_our_entry: usize =
            match following_log_after_append
                .iter()
                .position(|entry| entry.clock == clock)
            {
                Some(idx) => idx,
                None => return Err(
                    "Missing our entry despite already passed receiving success fromm list_append!"
                        .into(),
                ),
            };

        // Go in reverse from our append entry index until either we reach the last saved action (in which
        // case we are good) or some SAME action as ours on whom found first (in which case we are beaten
        // by a concurrent request).
        // Important to start search at idx_of_our_entry - 1 to avoid checking our own entry.
        // Use i32 since need to go to reach -1 to break from while loop.
        let mut search_idx: i32 = idx_of_our_entry as i32 - 1i32;
        while search_idx >= 0 {
            let search_entry: &FollowingLogEntry = &following_log_after_append[search_idx as usize];

            // Only see if we reached last save entry for break if last_entry_for_whom_prior_to_append
            // is not None (i.e. such an entry exists whe we first fetched)
            if let Some(last_saved_relevant_entry) = &last_entry_for_whom_prior_to_append {
                // If reached here, no races in between us and last saved entry.
                if search_entry == last_saved_relevant_entry {
                    return Ok(());
                }
            }

            // Note only check for the SAME action in races to ignore new opposite action appends.
            // In this case it is the FollowingActionType::UnfollowAction action.
            // Read Following/Unfollowing notes examples for TribFront for more info.
            // Of course, also only care about actions on same whom.
            if search_entry.action == FollowingActionType::UnfollowAction
                && search_entry.whom == whom
            {
                // Some concurrent unfollow beat us to the serialization point.
                return Err(Box::new(TribblerError::NotFollowing(
                    who.to_string(),
                    whom.to_string(),
                )));
            }
            search_idx -= 1;
        }

        // - If reached here, either last_entry_for_whom_prior_to_append is None, or we did not see it again
        //   when we refetched the log. In both cases, we return success. Reasoning:
        // - The first case is obvious that it is fine to declare the operation successful.
        // - The latter case could be caused by garbage removal, only keeping the latest action. Example:
        //    - Say we want to unfollow B (same concept applies to follow, but use unfollow as ex here).
        //    - We first fetch log and see:
        //         ..., (following B, 10)
        //    - We save (following B, 10) as last relevant actionl. However, the log could have been
        //      concurrently appended with a non-racing action to ours and become:
        //         ..., (following B, 10), (following B, 12)
        //    - Then when we append our action, say (unfollow B, 14) we get:
        //         ..., (following B, 10), (following B, 12), (unfollow B, 14)
        //    - At the same time garbage collector comes and removes (following B, 10) (and possibly
        //      (following B, 12) as well).
        //    - Then when we refetch the log, the saved (following B, 10) is missing.
        //    - Thus, as long as there is no racing condition in between us and the beginning of the log
        //      our (unfollow B, 14) as is the case here, our unfollow should still be allowed to succeed.
        //    - The same concept applies to the opposite case when we try to follow.
        Ok(())
    }

    /// Returns true when who following whom.
    /// Returns error when who == whom.
    /// Returns error when who or whom has not signed up.
    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }

        // Bin of "who"
        let bin_of_user = self.bin_storage.bin(who).await?;

        // Check both who and whom are signed up
        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        // Bin of "whom"
        let bin_of_whom = self.bin_storage.bin(whom).await?;
        if !Self::user_exists(&bin_of_whom).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        // Retrieves resolved following log (follow action only) and then see
        // if whom is in there
        let followees: Vec<String> = self.following(who).await?;

        if followees.iter().any(|followee| followee == whom) {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    /// Returns the list of following users.
    /// Returns error when who has not signed up.
    /// The list have users more than trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generate d by concurrent Follow()
    /// calls.
    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        // Bin of "who"
        let bin_of_user = self.bin_storage.bin(who).await?;

        // Check if user is signed up
        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        // Filter out unfollow action and only extract list of whom for follows.
        let followees: Vec<String> = Self::get_resolved_followees_only(&bin_of_user).await?;

        Ok(followees)
    }

    /// List the tribs of someone's following users (including himself).
    /// Returns error when user has not signed up.
    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        // Bin of user
        let bin_of_user = self.bin_storage.bin(user).await?;

        // Check if user is signed up
        if !Self::user_exists(&bin_of_user).await? {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        let followees = Self::get_resolved_followees_only(&bin_of_user).await?;

        // Important, start with the user's own bin, since they should also see their own tribs.
        let mut bin_client_vec = vec![bin_of_user];

        for followee in followees.iter() {
            // There is not really an async operation in this call (bin() calls new_client
            // which doesnt do any async operation)
            bin_client_vec.push(self.bin_storage.bin(followee).await?);
        }

        // Request user's own and each followee's tribs in parallel
        let tasks: Vec<_> = bin_client_vec
            .into_iter()
            .map(|bin_client| {
                // Note deliberately NOT adding ";" to the async function as well as the
                // spawn statements since they are used as expression return results
                tokio::spawn(async move {
                    Self::get_most_recent_trib_entries_from_user_bin(bin_client).await
                })
            })
            .collect();

        // Note keeping as TribLilstEntries so it can be sorted (since we can't implement Ord for Trib)
        let mut all_trib_entries: Vec<TribListEntry> = vec![];
        // Note chaining of "??" is needed. One is for the tokio's spawned task error
        // capturing (a Result<>) and the other is the inner await function call which
        // is also another Result
        for task in tasks {
            let mut res = task.await??;
            all_trib_entries.append(&mut res);
        }

        // Sort first (currently type TribListEntry), since we can't sort type Trib.
        all_trib_entries.sort();

        // Extract only the most recent entries, and up to MAX_TRIB_FETCH of them
        // Collect from drain_idx_start to the end of all_tribs
        // Ensure the range [drain_idx_start, all_tribs.len() - 1] inclusive cover at most MAX_TRIB_FETCH entries
        let mut drain_idx_start: i32 = all_trib_entries.len() as i32 - MAX_TRIB_FETCH as i32;
        if drain_idx_start < 0 {
            drain_idx_start = 0;
        }
        let most_recent_trib_entries: Vec<TribListEntry> =
            all_trib_entries.drain(drain_idx_start as usize..).collect();

        // Convert to return format Vec<Arc<Trib>>>
        let trib_list_to_return: Vec<Arc<Trib>> = most_recent_trib_entries
            .into_iter()
            .map(|entry| Arc::new(entry.trib))
            .collect();

        Ok(trib_list_to_return)
    }
}
