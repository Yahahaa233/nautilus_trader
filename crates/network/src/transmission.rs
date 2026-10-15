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

//! Native propagation of a process-private transmission generation.
use nautilus_core::transmission::TransmissionPermit;
use std::future::Future;

tokio::task_local! {
    static CURRENT: Option<TransmissionPermit>;
}

/// Capture the current command's immutable generation for another native queue.
#[must_use]
pub fn current() -> Option<TransmissionPermit> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

/// Run a native adapter operation with its captured transmission generation.
pub async fn scope<F: Future>(permit: Option<TransmissionPermit>, future: F) -> F::Output {
    CURRENT.scope(permit, future).await
}

/// Check the current command immediately before entering a native transport.
#[must_use]
pub fn can_transmit() -> bool {
    current().is_none_or(|permit| permit.can_transmit())
}
