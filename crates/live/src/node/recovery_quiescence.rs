//! Explicit no-timer subset for paused recovery capture.
use anyhow::{Context, Result, ensure};
use nautilus_common::clock::Clock;
use nautilus_system::trader::Trader;
use std::{any::TypeId, cell::RefCell, rc::Rc};

pub(super) fn with_empty_registered_timers<T>(
    kernel_clock: &Rc<RefCell<dyn Clock>>,
    trader: &Rc<RefCell<Trader>>,
    capture: impl FnOnce() -> Result<T>,
) -> Result<T> {
    // Retaining this borrow also prevents changing the registered clock map.
    let trader = trader
        .try_borrow()
        .context("trader busy during timer inventory")?;
    let components = trader.registered_component_clocks()?;
    let mut clocks = vec![("kernel".to_owned(), kernel_clock.clone())];
    clocks.extend(
        components
            .into_iter()
            .map(|(id, clock)| (format!("component:{id}"), clock)),
    );
    let held = clocks
        .iter()
        .map(|(id, clock)| {
            Ok((
                id,
                clock
                    .try_borrow()
                    .with_context(|| format!("clock busy: {id}"))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let verify = || -> Result<()> {
        for (id, clock) in &held {
            let kind = (**clock).type_id();
            ensure!(
                kind == TypeId::of::<nautilus_common::clock::TestClock>()
                    || kind == TypeId::of::<nautilus_common::live::clock::LiveClock>(),
                "unsupported clock implementation: {id}"
            );
            let names = clock.timer_names();
            let count = clock.timer_count();
            ensure!(count == names.len(), "timer inventory disagrees: {id}");
            ensure!(
                count == 0,
                "paused capture has unsupported active timers: {id}: {names:?}"
            );
        }
        Ok(())
    };
    verify()?;
    let result = capture();
    verify()?;
    // Keep the trader inventory borrow alive until all capture work has ended.
    ensure!(
        trader.component_count() + 1 == clocks.len(),
        "component clocks changed during capture"
    );
    result
}
