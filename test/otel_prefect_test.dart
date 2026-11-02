// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_prefect/otel_prefect.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_prefect-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('deployments.create_flow_run records work pool + tags', () async {
    await tracedPrefect<String>(
      operation: 'deployments.create_flow_run',
      invoke: () async => const PrefectCallResult(
        value: 'ok',
        result: PrefectResult(
          accountId: 'acc_1',
          workspaceId: 'ws_1',
          flowName: 'etl_warehouse',
          deploymentId: 'dep_1',
          flowRunId: 'fr_1',
          flowRunState: 'SCHEDULED',
          workPoolName: 'k8s-prod',
          workPoolType: 'kubernetes',
          workQueueName: 'default',
          runCount: 1,
          tags: 'nightly,warehouse',
          statusCode: 201,
        ),
      ),
    );
    final a = attrsOf(
      spans.findSpanByName('prefect deployments.create_flow_run')!,
    );
    expect(a['prefect.flow.name'], 'etl_warehouse');
    expect(a['prefect.flow_run.state'], 'SCHEDULED');
    expect(a['prefect.work_pool.type'], 'kubernetes');
    expect(a['prefect.tags'], 'nightly,warehouse');
  });

  test('CRASHED flow_run sets span error', () async {
    await tracedPrefect<String>(
      operation: 'flow_runs.read',
      invoke: () async => const PrefectCallResult(
        value: 'ok',
        result: PrefectResult(
          flowRunId: 'fr_2',
          flowRunState: 'CRASHED',
          runCount: 1,
        ),
      ),
    );
    expect(
      spans.findSpanByName('prefect flow_runs.read')!.status,
      SpanStatusCode.Error,
    );
  });

  test('LATE does NOT set span error', () async {
    await tracedPrefect<String>(
      operation: 'flow_runs.read',
      invoke: () async => const PrefectCallResult(
        value: 'ok',
        result: PrefectResult(flowRunState: 'LATE'),
      ),
    );
    expect(
      spans.findSpanByName('prefect flow_runs.read')!.status,
      isNot(SpanStatusCode.Error),
    );
  });

  test('suppression skips spans', () async {
    await runWithoutPrefectInstrumentationAsync(() async {
      await tracedPrefect<String>(
        operation: 'flows.list',
        invoke: () async => const PrefectCallResult(value: 'v'),
      );
    });
    expect(spans.findSpansStartingWith('prefect '), isEmpty);
  });
}
