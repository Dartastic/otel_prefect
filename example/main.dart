// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_prefect/otel_prefect.dart';

/// Traces Prefect calls with OpenTelemetry.
///
/// `otel_prefect` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Prefect call in
/// [tracedPrefect]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'prefect-example');

  // Wraps a Prefect API call in a CLIENT span.
  final prefectId = await tracedPrefect<String>(
    operation: 'flow_runs.create',
    invoke: () async {
      // Make the Prefect call here, then report what it returned.
      const id = 'prefect_8f3a2c';
      return const PrefectCallResult(
          value: id,
          result: PrefectResult(
              accountId: 'account_8f3a2c',
              workspaceId: 'workspace_8f3a2c',
              flowId: 'flow_8f3a2c',
              flowName: 'example',
              deploymentId: 'deployment_8f3a2c',
              flowRunId: 'flowrun_8f3a2c',
              flowRunName: 'example',
              taskRunId: 'taskrun_8f3a2c',
              flowRunState: 'SCHEDULED',
              taskRunState: 'active',
              workPoolName: 'example',
              workPoolType: 'process',
              workQueueName: 'orders',
              runCount: 3,
              tags: 'example',
              statusCode: 201));
    },
  );
  OTelLog.info('tracedPrefect flow_runs.create returned $prefectId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutPrefectInstrumentationAsync.
  await runWithoutPrefectInstrumentationAsync(
    () => tracedPrefect<String>(
      operation: 'flow_runs.create',
      invoke: () async {
        const id = 'prefect_8f3a2c';
        return const PrefectCallResult(
            value: id,
            result: PrefectResult(
                accountId: 'account_8f3a2c',
                workspaceId: 'workspace_8f3a2c',
                flowId: 'flow_8f3a2c',
                flowName: 'example',
                deploymentId: 'deployment_8f3a2c',
                flowRunId: 'flowrun_8f3a2c',
                flowRunName: 'example',
                taskRunId: 'taskrun_8f3a2c',
                flowRunState: 'SCHEDULED',
                taskRunState: 'active',
                workPoolName: 'example',
                workPoolType: 'process',
                workQueueName: 'orders',
                runCount: 3,
                tags: 'example',
                statusCode: 201));
      },
    ),
  );

  await OTel.shutdown();
}
