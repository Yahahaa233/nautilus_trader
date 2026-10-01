// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Client trait definitions for data and execution clients.
//!
//! Provides the core trait interfaces that define how clients interact with
//! data providers and execution venues.

mod data;
mod execution;

use std::fmt::{Debug, Display};

/// A live adapter's owned, bounded freeze. Inventories are generated from its
/// actual request/session state, never supplied by a checkpoint caller.
pub trait RunningAdapterCheckpoint: Debug {
    /// Replay state and the exact restricted adapter profile under this freeze.
    fn inventory(&self) -> &serde_json::Value;
    /// Revalidates the same freeze before and after durable persistence.
    ///
    /// # Errors
    /// Refuses changes, rejected admission, and unhealthy session boundaries.
    fn verify(&self) -> anyhow::Result<()>;
    /// Reopens callback admission only after successful verification.
    ///
    /// # Errors
    /// Refuses an invalid freeze; dropping unfinished guards fails closed.
    fn finish(self: Box<Self>) -> anyhow::Result<()>;
}

pub use data::DataClient;
pub use execution::{
    DEFAULT_POSITION_RECONCILIATION_TOLERANCE, ExecutionClient, generate_mass_status,
};

#[inline(always)]
fn log_not_implemented<T: Debug>(cmd: &T) {
    log::warn!("{cmd:?} - handler not implemented");
}

#[inline(always)]
pub fn log_command_error<C: Debug, E: Display>(cmd: &C, e: &E) {
    log::error!("Error on {cmd:?}: {e}");
}
