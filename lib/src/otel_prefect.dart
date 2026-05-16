// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_prefect_suppression.dart';

const _tracerName = 'dartastic_otel_libs_prefect';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

class PrefectResult {
  const PrefectResult({
    this.accountId,
    this.workspaceId,
    this.flowId,
    this.flowName,
    this.deploymentId,
    this.flowRunId,
    this.flowRunName,
    this.taskRunId,
    this.flowRunState,
    this.taskRunState,
    this.workPoolName,
    this.workPoolType,
    this.workQueueName,
    this.runCount,
    this.tags,
    this.statusCode,
  });

  /// Prefect Cloud account id (null for OSS server).
  final String? accountId;

  /// Prefect Cloud workspace (tenant scope).
  final String? workspaceId;

  final String? flowId;
  final String? flowName;

  /// Deployment = a packaged + scheduled instance of a flow.
  final String? deploymentId;

  final String? flowRunId;
  final String? flowRunName;
  final String? taskRunId;

  /// `SCHEDULED` / `PENDING` / `RUNNING` / `COMPLETED` / `FAILED` /
  /// `CANCELLED` / `CRASHED` / `PAUSED` / `CANCELLING` / `LATE`.
  final String? flowRunState;

  /// Same state types as flow runs; tracked separately for task-level
  /// observability.
  final String? taskRunState;

  /// Work pool routes runs to infrastructure (Process / Docker /
  /// Kubernetes / ECS / Cloud Run / etc.).
  final String? workPoolName;

  /// `process` / `docker` / `kubernetes` / `ecs` / `cloud-run` /
  /// `vertex-ai` / `azure-container-instance`.
  final String? workPoolType;

  /// Work queue within the pool.
  final String? workQueueName;

  /// Number of times this flow_run has been kicked off (retries).
  final int? runCount;

  /// Comma-separated user tags.
  final String? tags;

  final int? statusCode;
}

class PrefectCallResult<T> {
  const PrefectCallResult({required this.value, this.result});
  final T value;
  final PrefectResult? result;
}

/// Wraps a Prefect API call in a CLIENT span. [operation] is one of
/// `flows.list`, `flow_runs.create`, `flow_runs.read`,
/// `flow_runs.delete`, `flow_runs.set_state`,
/// `deployments.create_flow_run`, `task_runs.list`,
/// `work_pools.list`, `work_pools.create`, `blocks.create`,
/// `automations.create`, `events.emit`.
Future<T> tracedPrefect<T>({
  required String operation,
  required Future<PrefectCallResult<T>> Function() invoke,
}) async {
  if (prefectInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'rpc.system': 'prefect',
    'rpc.method': operation,
    'prefect.operation': operation,
  };

  final span = _tracer().startSpan(
    'prefect $operation',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.result != null) {
      final extras = <Attribute<Object>>[];
      if (r.result!.accountId != null) {
        extras.add(OTel.attributeString(
          'prefect.account.id',
          r.result!.accountId!,
        ));
      }
      if (r.result!.workspaceId != null) {
        extras.add(OTel.attributeString(
          'prefect.workspace.id',
          r.result!.workspaceId!,
        ));
      }
      if (r.result!.flowId != null) {
        extras.add(OTel.attributeString(
          'prefect.flow.id',
          r.result!.flowId!,
        ));
      }
      if (r.result!.flowName != null) {
        extras.add(OTel.attributeString(
          'prefect.flow.name',
          r.result!.flowName!,
        ));
      }
      if (r.result!.deploymentId != null) {
        extras.add(OTel.attributeString(
          'prefect.deployment.id',
          r.result!.deploymentId!,
        ));
      }
      if (r.result!.flowRunId != null) {
        extras.add(OTel.attributeString(
          'prefect.flow_run.id',
          r.result!.flowRunId!,
        ));
      }
      if (r.result!.flowRunName != null) {
        extras.add(OTel.attributeString(
          'prefect.flow_run.name',
          r.result!.flowRunName!,
        ));
      }
      if (r.result!.taskRunId != null) {
        extras.add(OTel.attributeString(
          'prefect.task_run.id',
          r.result!.taskRunId!,
        ));
      }
      if (r.result!.flowRunState != null) {
        extras.add(OTel.attributeString(
          'prefect.flow_run.state',
          r.result!.flowRunState!,
        ));
        if (r.result!.flowRunState == 'FAILED' ||
            r.result!.flowRunState == 'CRASHED') {
          span.setStatus(
            SpanStatusCode.Error,
            'Prefect flow_run ${r.result!.flowRunState}',
          );
        }
      }
      if (r.result!.taskRunState != null) {
        extras.add(OTel.attributeString(
          'prefect.task_run.state',
          r.result!.taskRunState!,
        ));
        if (r.result!.taskRunState == 'FAILED' ||
            r.result!.taskRunState == 'CRASHED') {
          span.setStatus(
            SpanStatusCode.Error,
            'Prefect task_run ${r.result!.taskRunState}',
          );
        }
      }
      if (r.result!.workPoolName != null) {
        extras.add(OTel.attributeString(
          'prefect.work_pool.name',
          r.result!.workPoolName!,
        ));
      }
      if (r.result!.workPoolType != null) {
        extras.add(OTel.attributeString(
          'prefect.work_pool.type',
          r.result!.workPoolType!,
        ));
      }
      if (r.result!.workQueueName != null) {
        extras.add(OTel.attributeString(
          'prefect.work_queue.name',
          r.result!.workQueueName!,
        ));
      }
      if (r.result!.runCount != null) {
        extras.add(OTel.attributeInt(
          'prefect.run.count',
          r.result!.runCount!,
        ));
      }
      if (r.result!.tags != null) {
        extras.add(OTel.attributeString('prefect.tags', r.result!.tags!));
      }
      if (r.result!.statusCode != null) {
        extras.add(OTel.attributeInt(
          'http.response.status_code',
          r.result!.statusCode!,
        ));
        if (r.result!.statusCode! >= 400) {
          span.setStatus(
            SpanStatusCode.Error,
            'Prefect HTTP ${r.result!.statusCode}',
          );
        }
      }
      if (extras.isNotEmpty) span.addAttributes(OTel.attributes(extras));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
