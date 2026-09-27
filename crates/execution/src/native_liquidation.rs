//! Read-only provenance for orders created by a live matching-engine liquidation.
//! Strategy code cannot insert proofs. Engine reset/drop invalidates all its proofs.
use nautilus_common::cache::Cache;
use nautilus_core::UUID4;
use nautilus_model::{
    events::OrderInitialized,
    identifiers::{AccountId, ClientOrderId, PositionId},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
};

#[derive(Clone, Debug)]
pub struct NativeLiquidationProof {
    engine: Weak<()>,
    cache: Weak<RefCell<Cache>>,
    engine_epoch: UUID4,
    account_id: AccountId,
    position_id: PositionId,
    order: OrderInitialized,
}
thread_local! {
    static PROOFS: RefCell<HashMap<ClientOrderId,NativeLiquidationProof>> = RefCell::new(HashMap::new());
}
impl NativeLiquidationProof {
    pub fn engine_epoch(&self) -> UUID4 {
        self.engine_epoch
    }
    pub fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub fn position_id(&self) -> PositionId {
        self.position_id
    }
    pub fn order(&self) -> &OrderInitialized {
        &self.order
    }
    pub fn matches_cache(&self, cache: &Cache) -> bool {
        self.engine.upgrade().is_some()
            && self.cache.upgrade().is_some_and(|source| {
                source
                    .try_borrow()
                    .is_ok_and(|source| std::ptr::eq(&*source, cache))
            })
    }
}
pub fn proof(order_id: ClientOrderId) -> Option<NativeLiquidationProof> {
    PROOFS.with(|proofs| {
        proofs
            .borrow()
            .get(&order_id)
            .filter(|p| p.engine.upgrade().is_some())
            .cloned()
    })
}
pub(crate) fn register(
    engine: &Rc<()>,
    epoch: UUID4,
    cache: &Rc<RefCell<Cache>>,
    account: AccountId,
    position: PositionId,
    order: OrderInitialized,
) {
    PROOFS.with(|proofs| {
        let mut proofs = proofs.borrow_mut();
        proofs.retain(|_, p| p.engine.upgrade().is_some());
        proofs.insert(
            order.client_order_id,
            NativeLiquidationProof {
                engine: Rc::downgrade(engine),
                cache: Rc::downgrade(cache),
                engine_epoch: epoch,
                account_id: account,
                position_id: position,
                order,
            },
        );
    });
}
