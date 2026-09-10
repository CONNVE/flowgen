//! Covers a DAG shape where a script task fans out to two `iterate`
//! branches — one implicit successor, one reached via `depends_on`
//! re-joining the same parent — and verifies both branches observe the
//! same `event.data` the shared parent produced, tick over tick, with no
//! stale carry-over from a prior tick.
//!
//! Shape:
//!   trigger (generate, 3 ticks)
//!     -> source (script, emits [] on tick 1, non-empty on ticks 2 and 3)
//!     -> gate (script, records what it received, keyed by tick number)
//!          -> branch_a (implicit successor: iterate over event.data)
//!          -> branch_b_gate (script, depends_on: [gate]) -> branch_b (iterate)
//!
//! No external dependency (in-memory cache), not `#[ignore]`d.

use flowgen::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};
use flowgen::flow::FlowBuilder;
use flowgen_core::cache::memory::MemoryCache;
use flowgen_core::cache::Cache;
use std::sync::Arc;
use std::time::Duration;

fn script_task(name: &str, code: &str, depends_on: Option<Vec<&str>>) -> TaskType {
    TaskType::script(flowgen_core::task::script::config::Processor {
        name: name.to_string(),
        code: flowgen_core::resource::Source::Inline(code.to_string()),
        depends_on: depends_on.map(|v| v.into_iter().map(String::from).collect()),
        ..Default::default()
    })
}

fn iterate_task(name: &str) -> TaskType {
    TaskType::iterate(flowgen_core::task::iterate::config::Processor {
        name: name.to_string(),
        iterate_key: None,
        depends_on: None,
        retry: None,
    })
}

async fn read_cache_value_by_suffix(
    cache: &Arc<dyn Cache>,
    all_keys: &[String],
    suffix: &str,
) -> Option<String> {
    let key = all_keys.iter().find(|k| k.ends_with(suffix))?;
    match cache.get(key).await {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn both_branches_of_a_reentrant_fan_out_observe_the_same_tick_data() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let source = script_task(
        "source",
        r#"
        let n = ctx.cache.get("source.tick");
        let tick = if n == () { 1 } else { parse_int(n) + 1 };
        ctx.cache.put("source.tick", tick.to_string(), 300);
        event.data = if tick == 1 { [] } else { [tick] };
        ctx.meta.tick = tick;
        event
        "#,
        None,
    );

    let gate = script_task(
        "gate",
        r#"
        ctx.cache.put("gate.tick." + ctx.meta.tick.to_string() + ".data_len", event.data.len().to_string(), 300);
        ctx.meta.payload = event.data;
        event
        "#,
        None,
    );

    let branch_a = iterate_task("branch_a");

    let branch_b_gate = script_task(
        "branch_b_gate",
        r#"
        ctx.cache.put("branch_b_gate.tick." + ctx.meta.tick.to_string() + ".data_len", ctx.meta.payload.len().to_string(), 300);
        event.data = ctx.meta.payload;
        event
        "#,
        Some(vec!["gate"]),
    );

    let branch_b = iterate_task("branch_b");

    let flow_config = match FlowConfig::from_path(
        FlowConfigRaw {
            flow: Flow {
                name: Some("reentrant_fan_out_repro".to_string()),
                labels: None,
                tasks: vec![
                    TaskType::generate(flowgen_core::task::generate::config::Subscriber {
                        name: "trigger".to_string(),
                        interval: Some(Duration::from_millis(200)),
                        count: Some(3),
                        ..Default::default()
                    }),
                    source,
                    gate,
                    branch_a,
                    branch_b_gate,
                    branch_b,
                ],
                require_leader_election: None,
                parallel_instances: 1,
            },
        },
        "reentrant_fan_out_repro".to_string(),
        None,
    ) {
        Ok(config) => Arc::new(config),
        Err(e) => panic!("FlowConfig::from_path returned Err: {e:?}"),
    };

    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new());
    let client_registry = Arc::new(flowgen_core::client_registry::ClientRegistry::new());

    let mut flow = match FlowBuilder::new()
        .config(flow_config)
        .cache(Arc::clone(&cache))
        .system_cache(Arc::clone(&cache))
        .client_registry(client_registry)
        .build()
    {
        Ok(flow) => flow,
        Err(e) => panic!("FlowBuilder::build returned Err: {e:?}"),
    };

    match flow.init().await {
        Ok(()) => {}
        Err(e) => panic!("flow.init() returned Err: {e:?}"),
    }

    let blocking_handles = match flow.start_tasks().await {
        Ok(handles) => handles,
        Err(e) => panic!("flow.start_tasks() returned Err: {e:?}"),
    };
    assert_eq!(
        blocking_handles.len(),
        0,
        "this flow has no webhook/blocking tasks, so start_tasks() must return zero \
         blocking handles; got {}",
        blocking_handles.len()
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    let all_keys = match cache.list_keys("").await {
        Ok(keys) => keys,
        Err(e) => panic!("cache.list_keys() returned Err: {e:?}"),
    };

    for tick in 1..=3 {
        let gate_suffix = format!("gate.tick.{tick}.data_len");
        let branch_b_gate_suffix = format!("branch_b_gate.tick.{tick}.data_len");

        let gate_len = read_cache_value_by_suffix(&cache, &all_keys, &gate_suffix).await;
        let branch_b_gate_len =
            read_cache_value_by_suffix(&cache, &all_keys, &branch_b_gate_suffix).await;

        assert_eq!(
            gate_len, branch_b_gate_len,
            "tick {tick}: gate saw event.data.len() = {gate_len:?} but branch_b_gate \
             saw ctx.meta.payload.len() = {branch_b_gate_len:?}; branch_b_gate only \
             ever receives what gate forwarded via ctx.meta.payload, so these must match"
        );

        let expected_len = if tick == 1 { "0" } else { "1" };
        assert_eq!(
            gate_len.as_deref(),
            Some(expected_len),
            "tick {tick}: expected gate to see event.data of length {expected_len} \
             (tick 1 is the empty-array case from source, ticks 2-3 carry one element); \
             got {gate_len:?} (all cache keys: {all_keys:?})"
        );
    }
}
