use super::*;

use nautilus_common::actor::registry::get_actor_unchecked;

thread_local! {
    static CALLS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn fixture() -> Rc<RefCell<Trader>> {
    CALLS.with(|calls| calls.borrow_mut().clear());
    let factory = ClockFactory::test_default();
    let cache = Rc::new(RefCell::new(Cache::new(None, None)));
    let portfolio = Rc::new(RefCell::new(Portfolio::new(
        factory.clock(),
        cache.clone(),
        None,
    )));
    Rc::new(RefCell::new(Trader::new(
        TraderId::from("TRADER-001"),
        UUID4::new(),
        Environment::Backtest,
        factory,
        cache,
        portfolio,
    )))
}
#[allow(clippy::unnecessary_wraps)]
fn save(id: Ustr) -> anyhow::Result<PersistedComponentState> {
    CALLS.with(|calls| calls.borrow_mut().push(id.to_string()));
    Ok(IndexMap::from([(
        "state".into(),
        id.as_str().as_bytes().to_vec(),
    )]))
}
#[allow(clippy::unnecessary_wraps)]
fn empty(id: Ustr) -> anyhow::Result<PersistedComponentState> {
    CALLS.with(|calls| calls.borrow_mut().push(id.to_string()));
    Ok(IndexMap::new())
}
fn fail(id: Ustr) -> anyhow::Result<PersistedComponentState> {
    CALLS.with(|calls| calls.borrow_mut().push(id.to_string()));
    anyhow::bail!("injected save failure for {id}")
}
#[allow(clippy::unnecessary_wraps)]
fn load(_: Ustr, _: PersistedComponentState) -> anyhow::Result<()> {
    Ok(())
}
fn register(
    trader: &Rc<RefCell<Trader>>,
    save_actor: ComponentStateSaveFn,
    save_strategy: ComponentStateSaveFn,
) {
    let mut trader = trader.borrow_mut();
    let actor = ActorId::from("COLLECT-ACTOR");
    let strategy = StrategyId::from("COLLECT-001");
    trader.actor_ids.push(actor);
    trader.strategy_ids.push(strategy);
    trader.actor_state_callbacks.insert(
        actor,
        ComponentStateCallbacks {
            load,
            save: save_actor,
        },
    );
    trader.strategy_state_callbacks.insert(
        strategy,
        ComponentStateCallbacks {
            load,
            save: save_strategy,
        },
    );
}

#[test]
fn collect_component_state_without_database_keeps_registration_order_and_empty_state() {
    let trader = fixture();
    register(&trader, save, empty);
    assert!(!trader.borrow().cache.borrow().has_backing());
    let state = Trader::collect_component_state(&trader).unwrap();
    assert_eq!(state.actors.len(), 1);
    assert_eq!(state.strategies.len(), 1);
    assert_eq!(
        state.actors[&ActorId::from("COLLECT-ACTOR")]["state"],
        b"COLLECT-ACTOR"
    );
    assert!(state.strategies[&StrategyId::from("COLLECT-001")].is_empty());
    CALLS.with(|calls| assert_eq!(*calls.borrow(), vec!["COLLECT-ACTOR", "COLLECT-001"]));
    let bytes = serde_json::to_vec(&state).unwrap();
    assert_eq!(
        serde_json::from_slice::<CollectedComponentState>(&bytes).unwrap(),
        state
    );
}

#[test]
fn collect_component_state_attempts_all_callbacks_but_never_returns_partial_state() {
    let trader = fixture();
    register(&trader, fail, fail);
    let error = Trader::collect_component_state(&trader)
        .unwrap_err()
        .to_string();
    assert!(error.contains("actor COLLECT-ACTOR callback"));
    assert!(error.contains("strategy COLLECT-001 callback"));
    CALLS.with(|calls| assert_eq!(*calls.borrow(), vec!["COLLECT-ACTOR", "COLLECT-001"]));
}

#[test]
fn collect_component_state_rejects_missing_registration_before_callbacks() {
    let trader = fixture();
    register(&trader, save, save);
    trader.borrow_mut().strategy_state_callbacks.clear();
    assert!(Trader::collect_component_state(&trader).is_err());
    CALLS.with(|calls| assert!(calls.borrow().is_empty()));
}

#[test]
fn collect_component_state_rejects_active_trader_and_accepts_empty_registry() {
    let trader = fixture();
    let guard = trader.borrow_mut();
    assert!(Trader::collect_component_state(&trader).is_err());
    drop(guard);
    let state = Trader::collect_component_state(&trader).unwrap();
    assert!(state.actors.is_empty() && state.strategies.is_empty());
}

#[derive(Debug)]
struct SavingStrategy {
    core: nautilus_trading::strategy::StrategyCore,
}
impl DataActor for SavingStrategy {
    fn on_save(&self) -> anyhow::Result<IndexMap<String, Vec<u8>>> {
        Ok(IndexMap::from([(
            "native".into(),
            b"saved without backing".to_vec(),
        )]))
    }
}
nautilus_trading::nautilus_strategy!(SavingStrategy);

#[test]
fn collect_component_state_invokes_real_registered_strategy_on_save() {
    let trader = fixture();
    let strategy = SavingStrategy {
        core: nautilus_trading::strategy::StrategyCore::new(
            nautilus_trading::strategy::StrategyConfig {
                strategy_id: Some(StrategyId::from("SAVING-001")),
                ..Default::default()
            },
        ),
    };
    trader.borrow_mut().add_strategy(strategy).unwrap();
    let state = Trader::collect_component_state(&trader).unwrap();
    assert_eq!(
        state.strategies[&StrategyId::from("SAVING-001")]["native"],
        b"saved without backing"
    );
    assert!(state.actors.is_empty());
    trader.borrow_mut().clear_strategies().unwrap();
}

#[derive(Debug)]
struct RestoringStrategy {
    core: nautilus_trading::strategy::StrategyCore,
    loaded: Option<IndexMap<String, Vec<u8>>>,
}

impl DataActor for RestoringStrategy {
    fn on_load(&mut self, state: PersistedComponentState) -> anyhow::Result<()> {
        self.loaded = Some(state);
        Ok(())
    }
}

nautilus_trading::nautilus_strategy!(RestoringStrategy);

#[test]
fn restore_component_state_loads_payload_for_exact_registered_strategy() {
    let trader = fixture();
    let strategy_id = StrategyId::from("RESTORE-001");
    trader
        .borrow_mut()
        .add_strategy(RestoringStrategy {
            core: nautilus_trading::strategy::StrategyCore::new(
                nautilus_trading::strategy::StrategyConfig {
                    strategy_id: Some(strategy_id),
                    ..Default::default()
                },
            ),
            loaded: None,
        })
        .unwrap();

    let payload = IndexMap::from([("checkpoint".into(), b"payload".to_vec())]);
    let state = CollectedComponentState {
        actors: IndexMap::new(),
        strategies: IndexMap::from([(strategy_id, payload.clone())]),
    };
    Trader::restore_component_state(&trader, &state).unwrap();

    let registered = get_actor_unchecked::<RestoringStrategy>(&strategy_id.inner());
    assert_eq!(registered.loaded.as_ref(), Some(&payload));
    drop(registered);
    trader.borrow_mut().clear_strategies().unwrap();
}

#[test]
fn restore_component_state_rejects_identity_mismatch_before_callbacks() {
    let trader = fixture();
    let strategy_id = StrategyId::from("RESTORE-002");
    trader
        .borrow_mut()
        .add_strategy(RestoringStrategy {
            core: nautilus_trading::strategy::StrategyCore::new(
                nautilus_trading::strategy::StrategyConfig {
                    strategy_id: Some(strategy_id),
                    ..Default::default()
                },
            ),
            loaded: None,
        })
        .unwrap();

    let state = CollectedComponentState {
        actors: IndexMap::new(),
        strategies: IndexMap::from([(StrategyId::from("RESTORE-WRONG"), IndexMap::new())]),
    };
    let error = Trader::restore_component_state(&trader, &state)
        .unwrap_err()
        .to_string();
    assert!(error.contains("identity/order"));

    let registered = get_actor_unchecked::<RestoringStrategy>(&strategy_id.inner());
    assert!(registered.loaded.is_none());
    drop(registered);
    trader.borrow_mut().clear_strategies().unwrap();
}

#[test]
fn restore_component_state_rejects_active_trader() {
    let trader = fixture();
    let state = CollectedComponentState {
        actors: IndexMap::new(),
        strategies: IndexMap::new(),
    };
    let guard = trader.borrow_mut();
    assert!(Trader::restore_component_state(&trader, &state).is_err());
    drop(guard);
}
