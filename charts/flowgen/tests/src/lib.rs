//! Helm chart tests for the flowgen chart.
//!
//! These tests run `helm template` against the local chart and assert on
//! the rendered Kubernetes manifests. They require `helm` to be installed
//! and available on `PATH`.

#![cfg(test)]

use serde::Deserialize;
use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Deserialize)]
struct Manifest {
    kind: String,
    metadata: Option<serde_yaml::Value>,
    spec: Option<serde_yaml::Value>,
}

fn render(values: &str) -> Vec<Manifest> {
    let chart_dir = env!("CARGO_MANIFEST_DIR");
    let chart_path = std::path::Path::new(chart_dir)
        .parent()
        .expect("tests dir has a parent");

    let mut child = Command::new("helm")
        .args(["template", "flowgen"])
        .arg(chart_path)
        .args(["--values", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("helm should be installed and executable");

    child
        .stdin
        .as_mut()
        .expect("helm stdin piped")
        .write_all(values.as_bytes())
        .expect("write values to helm stdin");

    let output = child.wait_with_output().expect("helm template should run");

    assert!(
        output.status.success(),
        "helm template failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("helm output is utf-8");
    let mut manifests = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(&stdout) {
        let value = serde_yaml::Value::deserialize(doc).expect("rendered doc is valid YAML");
        if value.is_null() {
            continue;
        }
        let manifest: Manifest =
            serde_yaml::from_value(value).expect("rendered manifest matches schema");
        manifests.push(manifest);
    }
    manifests
}

fn find_deployment(manifests: &[Manifest]) -> Manifest {
    manifests
        .iter()
        .find(|m| m.kind == "Deployment")
        .expect("chart should render a Deployment")
        .clone()
}

// podTemplate.merge / podTemplate.patch land in spec.template.spec, not spec.
fn pod_spec(deployment: &Manifest) -> &serde_yaml::Value {
    deployment
        .spec
        .as_ref()
        .expect("Deployment should have a spec")
        .get("template")
        .expect("Deployment spec should have a template")
        .get("spec")
        .expect("pod template should have a spec")
}

#[test]
fn default_deployment_renders_without_host_network() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);

    assert!(
        pod.get("hostNetwork").is_none(),
        "default pod spec should not contain hostNetwork, got: {pod:?}"
    );
}

#[test]
fn merge_injects_host_network_and_dns_policy() {
    let manifests = render(
        r#"flowgen:
  podTemplate:
    merge:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
"#,
    );
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);

    let host_network = pod
        .get("hostNetwork")
        .and_then(|v| v.as_bool())
        .expect("hostNetwork should be a boolean");
    assert!(host_network, "hostNetwork should be true");

    let dns_policy = pod
        .get("dnsPolicy")
        .and_then(|v| v.as_str())
        .expect("dnsPolicy should be a string");
    assert_eq!(dns_policy, "ClusterFirstWithHostNet");
}

fn service_named(manifests: &[Manifest], suffix: &str) -> Option<Manifest> {
    manifests
        .iter()
        .find(|m| {
            m.kind == "Service"
                && m.metadata
                    .as_ref()
                    .and_then(|md| md.get("name"))
                    .and_then(|n| n.as_str())
                    .is_some_and(|n| n.ends_with(suffix))
        })
        .cloned()
}

fn service_port(svc: &Manifest) -> u64 {
    svc.spec
        .as_ref()
        .and_then(|s| s.get("ports"))
        .and_then(|p| p.as_sequence())
        .and_then(|seq| seq.first())
        .and_then(|p| p.get("port"))
        .and_then(|v| v.as_u64())
        .expect("service port should be numeric")
}

#[test]
fn ai_gateway_service_renders_when_configured() {
    let manifests = render(
        r#"flowgen:
  ai_gateway:
    type: ClusterIP
    port: 3002
"#,
    );
    let svc = service_named(&manifests, "-ai-gateway").expect("ai-gateway Service should render");
    assert_eq!(service_port(&svc), 3002);
}

#[test]
fn ai_gateway_service_omitted_by_default() {
    let manifests = render("");
    assert!(service_named(&manifests, "-ai-gateway").is_none());
}

#[test]
fn web_service_renders_when_configured() {
    let manifests = render(
        r#"flowgen:
  web:
    type: ClusterIP
    port: 8080
"#,
    );
    let svc = service_named(&manifests, "-web").expect("web Service should render");
    assert_eq!(service_port(&svc), 8080);
}

#[test]
fn web_service_omitted_by_default() {
    let manifests = render("");
    assert!(service_named(&manifests, "-web").is_none());
}

#[test]
fn patch_adds_host_network() {
    let manifests = render(
        r#"flowgen:
  podTemplate:
    patch:
      - op: add
        path: /hostNetwork
        value: true
"#,
    );
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);

    let host_network = pod
        .get("hostNetwork")
        .and_then(|v| v.as_bool())
        .expect("hostNetwork should be a boolean");
    assert!(host_network, "hostNetwork should be true after patch");
}

fn container(pod: &serde_yaml::Value) -> &serde_yaml::Value {
    pod.get("containers")
        .and_then(|v| v.as_sequence())
        .and_then(|s| s.first())
        .expect("pod spec should have at least one container")
}

/// Regression: `http_server` is declared `Option<HttpServerOptions>` in
/// the app config, so setting it to null in values.yaml must render a
/// valid chart without a `nil pointer evaluating interface {}.port` from
/// helm. Both the containerPort declaration and the Service manifest have
/// to be guarded on the section being present.
#[test]
fn http_server_disabled_renders_without_port_or_service() {
    let manifests = render(
        r#"flowgen:
  http_server: null
"#,
    );

    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));
    let ports = container
        .get("ports")
        .and_then(|v| v.as_sequence())
        .expect("container should still declare ports (health at minimum)");
    assert!(
        !ports
            .iter()
            .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("http")),
        "container should not declare an `http` port when http_server is null, got: {ports:?}"
    );

    assert!(
        service_named(&manifests, "-http-server").is_none(),
        "http-server Service should not render when http_server is null"
    );
}

#[test]
fn probes_point_to_dedicated_health_port() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));

    let health_port = container
        .get("ports")
        .and_then(|v| v.as_sequence())
        .expect("container should declare ports")
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("health"))
        .expect("container should expose a `health` port");
    assert_eq!(
        health_port.get("containerPort").and_then(|v| v.as_u64()),
        Some(8081),
        "health port should default to 8081"
    );

    let readiness = container
        .get("readinessProbe")
        .and_then(|p| p.get("httpGet"))
        .expect("container should have a readinessProbe httpGet");
    assert_eq!(
        readiness.get("path").and_then(|v| v.as_str()),
        Some("/readyz")
    );
    assert_eq!(
        readiness.get("port").and_then(|v| v.as_str()),
        Some("health")
    );

    let liveness = container
        .get("livenessProbe")
        .and_then(|p| p.get("httpGet"))
        .expect("container should have a livenessProbe httpGet");
    assert_eq!(
        liveness.get("path").and_then(|v| v.as_str()),
        Some("/livez")
    );
    assert_eq!(
        liveness.get("port").and_then(|v| v.as_str()),
        Some("health")
    );
}

/// Every probe sets `timeoutSeconds`, and a startupProbe exists.
///
/// Leaving `timeoutSeconds` unset is the bug worth catching: Kubernetes
/// defaults it to 1s, so a worker merely busy enough to delay the handler
/// gets restarted, losing in-flight work and shifting load onto replicas
/// that then time out too. What the values should be is a deployment
/// decision — this only asserts they were made deliberately.
#[test]
fn probes_set_their_timeout_explicitly() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));

    for probe in ["startupProbe", "readinessProbe", "livenessProbe"] {
        let value = container
            .get(probe)
            .unwrap_or_else(|| panic!("container should have a {probe}"));
        assert!(
            value
                .get("timeoutSeconds")
                .and_then(|v| v.as_u64())
                .is_some(),
            "{probe} must set timeoutSeconds; unset means Kubernetes' 1s default"
        );
    }
}
