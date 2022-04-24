//! The tribbler package contains a number of tools and functions which can
//! be utilized when implementing the Tribbler service. Any public functions
//! here can be utilized when completing assignments.
//!
//! No modifications should be made to _any_ code within this package unless
//! explicitly approved by the TA or instructor. If you think there is an error
//! or a particular entity needs to be modified, please consult with a TA or
//! instructor first.
#![doc(
    html_logo_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Creative-Tail-Animal-penguin.svg/480px-Creative-Tail-Animal-penguin.svg.png"
)]
#![doc(
    html_favicon_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Creative-Tail-Animal-penguin.svg/128px-Creative-Tail-Animal-penguin.svg.png?20160314145218"
)]
pub mod addr;
pub mod colon;
pub mod config;
pub mod err;
pub mod ref_impl;
/// protobuf-generated RPC stubs and message structs
pub mod rpc;
pub mod storage;
pub mod trib;
