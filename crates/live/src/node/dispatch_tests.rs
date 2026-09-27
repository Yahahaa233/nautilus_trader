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

    let mut node = LiveNode::builder(TraderId::from("DISPATCH-RECOVERY-001"), Environment::Sandbox)
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
    node.set_dispatch_observer(NodeDispatchObserver::new(protocol.clone(), |source, phase, _| {
        Ok(DispatchInput {
            source,
            phase: phase.into(),
            payload: serde_json::json!({"recovery": true}),
            batch_index: None,
        })
    }))
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
        DispatchRecord::Complete { outermost: true, .. }
    )));
    assert!(!node.handle.should_stop());
}

#[test]
fn recovery_dispatch_callback_failure_poison_stops_the_node() {
    use crate::dispatch::{DispatchInput, DispatchObserver, DispatchSource};

    let mut node = LiveNode::builder(TraderId::from("DISPATCH-RECOVERY-FAIL-001"), Environment::Sandbox)
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
    node.set_dispatch_observer(NodeDispatchObserver::new(protocol.clone(), |source, phase, _| {
        Ok(DispatchInput {
            source,
            phase: phase.into(),
            payload: serde_json::Value::Null,
            batch_index: None,
        })
    }))
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

    let mut node = LiveNode::builder(TraderId::from("DISPATCH-QUEUE-FAIL-001"), Environment::Sandbox)
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
        .drain_recovery_dispatch(|_, input| {
            anyhow::bail!("reject {}", input.payload["id"])
        })
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

    let (_time_tx, mut time_rx) = tokio::sync::mpsc::unbounded_channel::<TimeEventMessage>();
    let (system_evt_tx, mut system_evt_rx) = tokio::sync::mpsc::unbounded_channel::<SystemEvent>();
    let (system_cmd_tx, mut system_cmd_rx) =
        tokio::sync::mpsc::unbounded_channel::<SystemCommand>();
    let (_exec_evt_tx, mut exec_evt_rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    let (_exec_cmd_tx, mut exec_cmd_rx) =
        tokio::sync::mpsc::unbounded_channel::<TradingCommandMessage>();
    let (_data_evt_tx, mut data_evt_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let (_data_cmd_tx, mut data_cmd_rx) = tokio::sync::mpsc::unbounded_channel::<DataCommand>();

    system_evt_tx
        .send(SystemEvent::SocketState(SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            "fixture".into(),
            SocketState::Disconnected,
        )))
        .unwrap();
    system_cmd_tx.send(stub_system_command()).unwrap();

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
