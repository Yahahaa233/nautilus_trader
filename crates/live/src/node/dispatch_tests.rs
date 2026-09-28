#[test]
fn node_dispatch_runner_inputs_use_observer_and_preserve_incomplete_coverage() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchRecord, DispatchSource};
    let mut node = LiveNode::builder(TraderId::from("DISPATCH-001"), Environment::Sandbox)
        .unwrap()
        .with_reconciliation(false)
        .build()
        .unwrap();
    let records = Rc::new(RefCell::new(Vec::new()));
    let output = records.clone();
    let protocol = DispatchObserver::new("fixture".into(), move |record| {
        output.borrow_mut().push(record.clone());
        Ok(())
    })
    .unwrap();
    let observer = NodeDispatchObserver::new(protocol.clone(), |source, phase, _| {
        Ok(DispatchInput {
            source,
            phase: phase.into(),
            payload: serde_json::json!({"fixture":true}),
            batch_index: None,
        })
    });
    node.set_dispatch_observer(observer.clone()).unwrap();
    assert!(node.set_dispatch_observer(observer.clone()).is_err());
    node.process_runner_event(PendingRunnerEvent::TimeEvent(stub_time_event_handler()));
    node.process_runner_event(PendingRunnerEvent::DataEvent(stub_data_event()));
    node.process_runner_event(PendingRunnerEvent::DataCommand(stub_data_command()));
    node.process_runner_event(PendingRunnerEvent::SystemCommand(stub_system_command()));
    node.process_runner_event(PendingRunnerEvent::SystemEvent(SystemEvent::SocketState(
        SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            "fixture".into(),
            SocketState::Disconnected,
        ),
    )));
    node.process_runner_event(PendingRunnerEvent::ExecEvent(stub_exec_event()));
    node.process_runner_event(PendingRunnerEvent::ExecCommand(
        stub_trading_command_message(),
    ));
    node.process_reconciliation_events(&[]);
    let sources: Vec<_> = records
        .borrow()
        .iter()
        .filter_map(|record| match record {
            DispatchRecord::Begin { input, .. } => Some(input.source),
            _ => None,
        })
        .collect();
    assert_eq!(
        sources,
        vec![
            DispatchSource::Time,
            DispatchSource::DataEvent,
            DispatchSource::DataCommand,
            DispatchSource::SystemCommand,
            DispatchSource::SystemEvent,
            DispatchSource::ExecutionEvent,
            DispatchSource::ExecutionCommand,
            DispatchSource::Reconciliation
        ]
    );
    assert_eq!(protocol.completed_root().unwrap(), 8);
    assert_eq!(observer.coverage().unwrap()["coverage_complete"], false);
    node.note_dispatch_gap("startup_fixture");
    assert!(records.borrow().iter().any(|record| matches!(record,
        DispatchRecord::Uncovered { reason, .. } if reason == "startup_fixture")));
}

#[test]
fn node_dispatch_sink_failure_prevents_time_callback() {
    use crate::dispatch::{DispatchInput, DispatchObserver};
    use nautilus_common::timer::{TimeEvent, TimeEventCallback};
    let mut node = LiveNode::builder(TraderId::from("DISPATCH-FAIL-001"), Environment::Sandbox)
        .unwrap()
        .with_reconciliation(false)
        .build()
        .unwrap();
    let protocol =
        DispatchObserver::new("fixture".into(), |_| anyhow::bail!("durable begin failed")).unwrap();
    node.set_dispatch_observer(NodeDispatchObserver::new(
        protocol.clone(),
        |source, phase, _| {
            Ok(DispatchInput {
                source,
                phase: phase.into(),
                payload: serde_json::Value::Null,
                batch_index: None,
            })
        },
    ))
    .unwrap();
    let called = Rc::new(Cell::new(false));
    let callback_called = called.clone();
    let message = TimeEventMessage::new(
        TimeEvent::new(
            "blocked".into(),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(1),
        ),
        TimeEventCallback::RustLocal(Rc::new(move |_: TimeEvent| callback_called.set(true))),
    );
    assert!(!node.process_time_event(message));
    assert!(!called.get());
    assert_eq!(protocol.completed_root().unwrap(), 0);
    assert!(node.handle.should_stop());
}

#[test]
fn recovery_dispatch_runs_callback_inside_an_idle_observed_boundary() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchRecord, DispatchSource};

    let mut node = LiveNode::builder(
        TraderId::from("DISPATCH-RECOVERY-001"),
        Environment::Sandbox,
    )
    .unwrap()
    .with_reconciliation(false)
    .build()
    .unwrap();
    let records = Rc::new(RefCell::new(Vec::new()));
    let output = records.clone();
    let protocol = DispatchObserver::new("recovery".into(), move |record| {
        output.borrow_mut().push(record.clone());
        Ok(())
    })
    .unwrap();
    node.set_dispatch_observer(NodeDispatchObserver::new(
        protocol.clone(),
        |source, phase, _| {
            Ok(DispatchInput {
                source,
                phase: phase.into(),
                payload: serde_json::json!({"recovery": true}),
                batch_index: None,
            })
        },
    ))
    .unwrap();

    let applied = Rc::new(Cell::new(false));
    let applied_callback = applied.clone();
    let result = node
        .with_recovery_dispatch(
            DispatchInput {
                source: DispatchSource::Replay,
                phase: "recovery".into(),
                payload: serde_json::json!({"input_id": "queued-1"}),
                batch_index: None,
            },
            move |node| {
                applied_callback.set(node.state() == NodeState::Idle);
                Ok::<_, anyhow::Error>("queued-1")
            },
        )
        .unwrap();

    assert_eq!(result, "queued-1");
    assert!(applied.get());
    assert_eq!(protocol.completed_root().unwrap(), 1);
    assert!(records.borrow().iter().any(|record| matches!(
        record,
        DispatchRecord::Begin { input, .. } if input.source == DispatchSource::Replay
    )));
    assert!(records.borrow().iter().any(|record| matches!(
        record,
        DispatchRecord::Complete {
            outermost: true,
            ..
        }
    )));
    assert!(!node.handle.should_stop());
}

#[test]
fn recovery_dispatch_callback_failure_poison_stops_the_node() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchSource};

    let mut node = LiveNode::builder(
        TraderId::from("DISPATCH-RECOVERY-FAIL-001"),
        Environment::Sandbox,
    )
    .unwrap()
    .with_reconciliation(false)
    .build()
    .unwrap();
    let protocol = DispatchObserver::new("recovery-failure".into(), |_| Ok(())).unwrap();
    node.set_dispatch_observer(NodeDispatchObserver::new(protocol, |source, phase, _| {
        Ok(DispatchInput {
            source,
            phase: phase.into(),
            payload: serde_json::Value::Null,
            batch_index: None,
        })
    }))
    .unwrap();

    let error = node
        .with_recovery_dispatch::<(), _>(
            DispatchInput {
                source: DispatchSource::Replay,
                phase: "recovery".into(),
                payload: serde_json::Value::Null,
                batch_index: None,
            },
            |_| anyhow::bail!("codec rejected recovery input"),
        )
        .unwrap_err();
    assert!(format!("{error:#}").contains("codec rejected recovery input"));
    assert!(node.handle.should_stop());
    assert!(node.dispatch_completion_proof().is_err());
}

#[test]
fn recovery_dispatch_queue_handoff_is_fifo_and_observed_per_input() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchRecord, DispatchSource};

    let mut node = LiveNode::builder(TraderId::from("DISPATCH-QUEUE-001"), Environment::Sandbox)
        .unwrap()
        .with_reconciliation(false)
        .build()
        .unwrap();
    let records = Rc::new(RefCell::new(Vec::new()));
    let output = records.clone();
    let protocol = DispatchObserver::new("recovery-queue".into(), move |record| {
        output.borrow_mut().push(record.clone());
        Ok(())
    })
    .unwrap();
    node.set_dispatch_observer(NodeDispatchObserver::new(
        protocol.clone(),
        |source, phase, _| {
            Ok(DispatchInput {
                source,
                phase: phase.into(),
                payload: serde_json::Value::Null,
                batch_index: None,
            })
        },
    ))
    .unwrap();

    for id in ["one", "two", "three"] {
        node.enqueue_recovery_dispatch(DispatchInput {
            source: DispatchSource::Replay,
            phase: "recovery".into(),
            payload: serde_json::json!({"id": id}),
            batch_index: None,
        })
        .unwrap();
    }
    assert_eq!(node.recovery_dispatch_queue_len(), 3);

    let mut applied = Vec::new();
    let drained = node
        .drain_recovery_dispatch(|node, input| {
            assert_eq!(node.state(), NodeState::Idle);
            applied.push(input.payload["id"].as_str().unwrap().to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(drained, 3);
    assert_eq!(applied, ["one", "two", "three"]);
    assert_eq!(node.recovery_dispatch_queue_len(), 0);
    assert_eq!(protocol.completed_root().unwrap(), 3);
    assert_eq!(
        records
            .borrow()
            .iter()
            .filter(|record| matches!(record, DispatchRecord::Begin { .. }))
            .count(),
        3
    );
}

#[test]
fn recovery_dispatch_queue_failure_preserves_unprocessed_suffix_and_stops() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchSource};

    let mut node = LiveNode::builder(
        TraderId::from("DISPATCH-QUEUE-FAIL-001"),
        Environment::Sandbox,
    )
    .unwrap()
    .with_reconciliation(false)
    .build()
    .unwrap();
    let protocol = DispatchObserver::new("recovery-queue-failure".into(), |_| Ok(())).unwrap();
    node.set_dispatch_observer(NodeDispatchObserver::new(protocol, |source, phase, _| {
        Ok(DispatchInput {
            source,
            phase: phase.into(),
            payload: serde_json::Value::Null,
            batch_index: None,
        })
    }))
    .unwrap();
    for id in ["one", "two"] {
        node.enqueue_recovery_dispatch(DispatchInput {
            source: DispatchSource::Replay,
            phase: "recovery".into(),
            payload: serde_json::json!({"id": id}),
            batch_index: None,
        })
        .unwrap();
    }

    let error = node
        .drain_recovery_dispatch(|_, input| anyhow::bail!("reject {}", input.payload["id"]))
        .unwrap_err();
    assert!(format!("{error:#}").contains("reject \"one\""));
    assert!(node.handle.should_stop());
    assert_eq!(node.recovery_dispatch_queue_len(), 2);
}

#[test]
fn final_drain_records_each_discarded_system_input() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchRecord, DispatchSource};

    let mut node = LiveNode::builder(TraderId::from("DISPATCH-DRAIN-001"), Environment::Sandbox)
        .unwrap()
        .with_reconciliation(false)
        .build()
        .unwrap();
    let records = Rc::new(RefCell::new(Vec::new()));
    let output = records.clone();
    let protocol = DispatchObserver::new("fixture".into(), move |record| {
        output.borrow_mut().push(record.clone());
        Ok(())
    })
    .unwrap();
    node.set_dispatch_observer(NodeDispatchObserver::new(protocol, |source, phase, _| {
        Ok(DispatchInput {
            source,
            phase: phase.into(),
            payload: serde_json::json!({"fixture":true}),
            batch_index: None,
        })
    }))
    .unwrap();

    let (_time_tx, time_rx) = tokio::sync::mpsc::unbounded_channel::<TimeEventMessage>();
    let (system_evt_tx, system_evt_rx) = tokio::sync::mpsc::unbounded_channel::<SystemEvent>();
    let (system_cmd_tx, system_cmd_rx) = tokio::sync::mpsc::unbounded_channel::<SystemCommand>();
    let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    let (_exec_cmd_tx, exec_cmd_rx) =
        tokio::sync::mpsc::unbounded_channel::<TradingCommandMessage>();
    let (_data_evt_tx, data_evt_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let (_data_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::unbounded_channel::<DataCommand>();

    system_evt_tx
        .send(SystemEvent::SocketState(SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            "fixture".into(),
            SocketState::Disconnected,
        )))
        .unwrap();
    system_cmd_tx.send(stub_system_command()).unwrap();

    let mut time_rx = crate::runner::SnapshotReceiver::from(time_rx);
    let mut system_evt_rx = crate::runner::SnapshotReceiver::from(system_evt_rx);
    let mut system_cmd_rx = crate::runner::SnapshotReceiver::from(system_cmd_rx);
    let mut exec_evt_rx = crate::runner::SnapshotReceiver::from(exec_evt_rx);
    let mut exec_cmd_rx = crate::runner::SnapshotReceiver::from(exec_cmd_rx);
    let mut data_evt_rx = crate::runner::SnapshotReceiver::from(data_evt_rx);
    let mut data_cmd_rx = crate::runner::SnapshotReceiver::from(data_cmd_rx);
    node.drain_channels(
        &mut time_rx,
        &mut system_evt_rx,
        &mut system_cmd_rx,
        &mut exec_evt_rx,
        &mut exec_cmd_rx,
        &mut data_evt_rx,
        &mut data_cmd_rx,
    );

    let discarded_sources: Vec<_> = records
        .borrow()
        .iter()
        .filter_map(|record| match record {
            DispatchRecord::Discarded { input, .. } => Some(input.source),
            _ => None,
        })
        .collect();
    assert_eq!(
        discarded_sources,
        vec![DispatchSource::SystemEvent, DispatchSource::SystemCommand]
    );
}

#[test]
fn paused_queue_capture_holds_gate_through_callback_and_rejects_failure() {
    use crate::dispatch::DispatchObserver;
    use crate::runner_recovery::{RunnerRecoveryCodecRegistry, RunnerRecoveryWatermark};
    for mode in 0..5 {
        let mut node = LiveNode::builder(TraderId::from("CAPTURE-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        node.kernel
            .risk_engine
            .borrow_mut()
            .set_trading_state(nautilus_model::enums::TradingState::Halted);
        let protocol = DispatchObserver::new("capture-run".into(), |_| Ok(())).unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(protocol.clone(), |_, _, _| {
            anyhow::bail!("no live encode")
        }))
        .unwrap();
        let registry = RunnerRecoveryCodecRegistry::new([]).seal().unwrap();
        node.replay_recovery_events(
            &RunnerRecoveryWatermark {
                recovery_id: "capture-run".into(),
                checkpoint_sequence: 1,
                dispatch_watermark: 1,
            },
            &[],
            &registry,
            |_, _| Ok(()),
        )
        .unwrap();
        let ingress = node.runner.as_ref().unwrap().ingress_gate();
        let handle = node.handle();
        let persisted = std::cell::Cell::new(false);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            node.with_paused_recovery_queue_checkpoint(
                &registry,
                |snapshot| {
                    assert!(snapshot.entries().is_empty());
                    assert!(ingress.verify_open().is_err());
                    match mode {
                        1 => anyhow::bail!("injected collection failure"),
                        2 => panic!("injected collection panic"),
                        3 => handle.stop(),
                        4 => {
                            let token = protocol.begin(crate::dispatch::DispatchInput {
                                source: crate::dispatch::DispatchSource::Replay,
                                phase: "unexpected-during-capture".into(),
                                payload: serde_json::json!({"changed": true}),
                                batch_index: None,
                            })?;
                            protocol.complete(&token)?;
                        }
                        _ => {}
                    }
                    Ok(17)
                },
                |value| {
                    persisted.set(true);
                    Ok(value)
                },
            )
        }));
        assert_eq!(persisted.get(), mode == 0);
        if mode == 0 {
            assert_eq!(result.unwrap().unwrap(), 17);
            ingress.verify_open().unwrap();
            assert!(!node.handle.should_stop());
        } else {
            assert!(result.is_err() || result.unwrap().is_err());
            assert!(node.handle.should_stop());
            assert!(ingress.verify_open().is_err());
        }
        assert!(node.recovery_requires_release);
        node.kernel.dispose();
    }
}

#[tokio::test]
async fn paused_timer_inventory_checks_registered_actor_and_retains_borrows() {
    use nautilus_common::timer::TimeEventCallback;
    let mut node = LiveNode::builder(TraderId::from("CLOCK-INVENTORY-001"), Environment::Sandbox)
        .unwrap()
        .with_reconciliation(false)
        .build()
        .unwrap();
    node.add_actor(StartupSocketActor::new(Rc::new(RefCell::new(Vec::new()))))
        .unwrap();
    let clocks = node
        .kernel
        .trader
        .borrow()
        .registered_component_clocks()
        .unwrap();
    assert_eq!(clocks.len(), 1);
    let actor_clock = clocks[0].1.clone();
    recovery_quiescence::with_empty_registered_timers(
        &node.kernel.clock,
        &node.kernel.trader,
        || {
            assert!(actor_clock.try_borrow_mut().is_err());
            assert!(node.kernel.clock.try_borrow_mut().is_err());
            assert!(node.kernel.trader.try_borrow_mut().is_err());
            Ok(())
        },
    )
    .unwrap();
    let deadline = nautilus_core::UnixNanos::from(
        actor_clock.borrow().timestamp_ns().as_u64() + 60_000_000_000,
    );
    actor_clock
        .borrow_mut()
        .set_time_alert_ns(
            "pending-actor",
            deadline,
            Some(TimeEventCallback::RustLocal(Rc::new(|_| {}))),
            None,
        )
        .unwrap();
    assert_eq!(node.kernel.clock.borrow().timer_count(), 0);
    assert!(
        recovery_quiescence::with_empty_registered_timers::<()>(
            &node.kernel.clock,
            &node.kernel.trader,
            || panic!("must not capture active actor timer")
        )
        .is_err()
    );
    assert_eq!(actor_clock.borrow().timer_names(), vec!["pending-actor"]);
    actor_clock.borrow_mut().cancel_timers();
    let held = actor_clock.borrow_mut();
    assert!(
        recovery_quiescence::with_empty_registered_timers(
            &node.kernel.clock,
            &node.kernel.trader,
            || Ok(())
        )
        .is_err()
    );
    drop(held);
    node.kernel
        .trader
        .borrow_mut()
        .create_component_clock(nautilus_model::identifiers::ComponentId::from("ORPHAN"));
    assert!(
        recovery_quiescence::with_empty_registered_timers(
            &node.kernel.clock,
            &node.kernel.trader,
            || Ok(())
        )
        .is_err()
    );
}

#[test]
fn empty_bootstrap_receipt_requires_real_handoffs_and_never_invents_dispatch() {
    use crate::dispatch::DispatchObserver;
    use crate::runner_recovery::RunnerRecoveryCodecRegistry;
    for mode in 0..3 {
        let mut node =
            LiveNode::builder(TraderId::from("EMPTY-BOOTSTRAP-001"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .build()
                .unwrap();
        node.kernel
            .risk_engine
            .borrow_mut()
            .set_trading_state(nautilus_model::enums::TradingState::Halted);
        let observer = DispatchObserver::new("empty-bootstrap".into(), |_| Ok(())).unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(observer.clone(), |_, _, _| {
            anyhow::bail!("no dispatch")
        }))
        .unwrap();
        let source = UUID4::new();
        let digest = "a".repeat(64);
        assert!(
            node.complete_empty_bootstrap_recovery("original", 3, &digest, source)
                .is_err()
        );
        node.restore_native_cache(Cache::default()).unwrap();
        assert!(
            node.complete_empty_bootstrap_recovery("original", 3, &digest, source)
                .is_err()
        );
        node.restore_component_state(&CollectedComponentState {
            actors: Default::default(),
            strategies: Default::default(),
        })
        .unwrap();
        if mode == 2 {
            nautilus_common::live::runner::get_system_command_sender()
                .send(stub_system_command())
                .unwrap();
        }
        let receipt = node.complete_empty_bootstrap_recovery("original", 3, &digest, source);
        if mode == 2 {
            assert!(receipt.is_err());
            assert_eq!(
                node.runner
                    .as_ref()
                    .unwrap()
                    .pending_queue_counts()
                    .values()
                    .sum::<usize>(),
                1
            );
            assert!(node.handle.should_stop());
            continue;
        }
        assert_eq!(
            serde_json::to_value(receipt.unwrap()).unwrap()["checkpoint_sequence"],
            3
        );
        assert!(node.recovery_native_frontier.is_none());
        assert_eq!(observer.completed_root().unwrap(), 0);
        assert!(
            node.complete_empty_bootstrap_recovery("original", 3, &digest, source)
                .is_err()
        );
        let persisted = std::cell::Cell::new(false);
        let registry = RunnerRecoveryCodecRegistry::new([]).seal().unwrap();
        let result = node.with_paused_recovery_inventory_checkpoint(
            &registry,
            |snapshot, inventory| {
                assert!(snapshot.entries().is_empty());
                let inventory = serde_json::to_value(inventory)?;
                assert_eq!(inventory["runner_counts"].as_object().unwrap().len(), 7);
                assert!(
                    inventory["runner_counts"]
                        .as_object()
                        .unwrap()
                        .values()
                        .all(|v| *v == 0)
                );
                assert_eq!(inventory["timer_counts"]["kernel"], 0);
                assert_eq!(inventory["message_bus_mode"], "local_only");
                if mode == 1 {
                    assert!(
                        std::panic::catch_unwind(|| nautilus_common::msgbus::publish_any(
                            "attempted".into(),
                            &()
                        ))
                        .is_err()
                    );
                }
                Ok(())
            },
            |()| {
                persisted.set(true);
                Ok(())
            },
        );
        assert_eq!(result.is_ok(), mode == 0);
        assert_eq!(persisted.get(), mode == 0);
        assert_eq!(observer.completed_root().unwrap(), 0);
    }
}
