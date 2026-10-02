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

//! Exercises actual manager state and cross-process elapsed-time semantics.
use super::*;
#[test]
fn checkpoint_manager_restore_keeps_actual_fifo_retries_shapes_and_ages_offline() {
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(
        nautilus_common::live::clock::LiveClock::new(None),
    ));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = ExecutionManagerConfig::default();
    let mut source = ExecutionManager::new(clock.clone(), cache.clone(), config.clone()).unwrap();
    let first = ClientOrderId::from("SOURCE-FIRST");
    let second = ClientOrderId::from("SOURCE-SECOND");
    source.register_inflight(first);
    source.register_inflight(second);
    source
        .order_inflight_checks
        .get_mut(&first)
        .unwrap()
        .retry_count = 2;
    source
        .order_inflight_checks
        .get_mut(&first)
        .unwrap()
        .last_query_at = Some(dst::time::Instant::now());
    source.order_recon_retries.insert(first, 3);
    source.order_coverage_unresolved.insert(second);
    let account = AccountId::from("OKX-001");
    let instrument = InstrumentId::from("BTC-USDT-SWAP.OKX");
    let fill = (account, instrument, TradeId::from("SOURCE-FILL"));
    source.fills_processed.mark(fill);
    source.fills_recent.mark(fill);
    source.position_activity.mark((instrument, account));
    source
        .position_activity_revisions
        .insert((instrument, account), 7);
    source.position_recon.insert(
        (instrument, account),
        PositionReconciliationState {
            report_shape: PositionReportShape::MultiLeg,
            retries: 2,
        },
    );
    source
        .position_recon_tolerances
        .insert(account, Decimal::new(1, 5));
    let at = dst::time::Instant::now();
    let snapshot = source.checkpoint_inventory(at).unwrap();
    let mut restored = ExecutionManager::new(clock.clone(), cache.clone(), config.clone()).unwrap();
    restored
        .restore_checkpoint_inventory(&snapshot, 1_000_000_000)
        .unwrap();
    assert_eq!(
        restored
            .order_inflight_checks
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(restored.order_inflight_checks[&first].retry_count, 2);
    assert_eq!(restored.order_recon_retries[&first], 3);
    assert!(restored.order_coverage_unresolved.contains(&second));
    assert_eq!(
        restored.position_activity_revisions[&(instrument, account)],
        7
    );
    assert_eq!(
        restored.position_recon[&(instrument, account)].report_shape,
        PositionReportShape::MultiLeg
    );
    assert!(restored.fills_processed.contains_key(&fill));
    assert!(
        !restored
            .fills_recent
            .within(&fill, Duration::from_millis(900)),
        "offline elapsed time cannot refresh recency"
    );
    assert!(restored.fills_recent.within(&fill, Duration::from_secs(2)));
    assert!(restored.restore_checkpoint_inventory(&snapshot, 0).is_err());
    let mut broken = snapshot.clone();
    let duplicate = broken["fills_processed"][0].clone();
    broken["fills_processed"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    let mut fresh = ExecutionManager::new(clock.clone(), cache.clone(), config.clone()).unwrap();
    assert!(fresh.restore_checkpoint_inventory(&broken, 0).is_err());
    assert!(!fresh.fills_processed.contains_key(&fill));
    let mut wrong = snapshot.clone();
    wrong["configuration"]["filter_unclaimed_external"] = serde_json::json!(true);
    assert!(fresh.restore_checkpoint_inventory(&wrong, 0).is_err());
    source.order_query_pending.insert(first);
    assert!(
        source
            .checkpoint_inventory(dst::time::Instant::now())
            .is_err(),
        "live query cannot be archived as complete"
    );
}
