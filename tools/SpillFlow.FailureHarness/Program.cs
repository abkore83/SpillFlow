using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using SpillFlow;

const int RecordCount = 10_000;
const string OperationId = "hard-crash";

if (args.ElementAtOrDefault(0) == "create-fixture")
    return await CreateRecoveryFixtureAsync(
        args.ElementAtOrDefault(1) ?? throw new ArgumentException("A fixture output directory is required."),
        args.ElementAtOrDefault(2) ?? "orderby");

if (args.ElementAtOrDefault(0) == "worker")
    return await RunWorkerAsync(args.Skip(1).ToArray());

return await RunHarnessAsync(args);

static async Task<int> CreateRecoveryFixtureAsync(string outputDirectory, string operationName)
{
    if (operationName is not ("orderby" or "distinct" or "exceptby" or "aggregate" or "groupjoin"))
        throw new ArgumentException($"Unknown recoverable operation '{operationName}'.");
    outputDirectory = Path.GetFullPath(outputDirectory);
    if (Directory.Exists(outputDirectory) && Directory.EnumerateFileSystemEntries(outputDirectory).Any())
        throw new InvalidOperationException($"Fixture directory '{outputDirectory}' is not empty.");
    Directory.CreateDirectory(Path.GetDirectoryName(outputDirectory)!);
    var stagingRoot = Path.Combine(Path.GetDirectoryName(outputDirectory)!, $"fixture-stage-{Guid.NewGuid():N}");
    Directory.CreateDirectory(stagingRoot);
    string sessionDirectory;
    await using (var session = new SpillSession(Options(stagingRoot)))
    {
        sessionDirectory = session.WorkingDirectory;
        var delivered = 0;
        try
        {
            var output = operationName == "orderby"
                ? CompatibilityOrderByOutput(session)
                : RecoverableOutput(session, operationName, null);
            await foreach (var _ in output)
            {
                if (++delivered == 15)
                    throw new FixtureInterruptionException();
            }
        }
        catch (FixtureInterruptionException) { }
        session.PreserveForRecovery();
    }

    if (Directory.Exists(outputDirectory)) Directory.Delete(outputDirectory);
    Directory.Move(sessionDirectory, outputDirectory);
    if (Directory.Exists(stagingRoot)) Directory.Delete(stagingRoot, recursive: true);
    Console.WriteLine($"Created {operationName} recovery fixture: {outputDirectory}");
    return 0;
}

static async IAsyncEnumerable<long> CompatibilityOrderByOutput(SpillSession session)
{
    await foreach (var value in session.OrderByRecoverableSpill(
                       (offset, token) => DescendingSource(offset, 1_000, token),
                       "compat-orderby",
                       value => value,
                       outputCheckpointInterval: 10))
        yield return value;
}

static async Task<int> RunHarnessAsync(string[] harnessArgs)
{
    var requestedOutput = ReadOption(harnessArgs, "--output");
    var root = Path.GetFullPath(requestedOutput ?? Path.Combine(
        "artifacts", "failure-harness", DateTime.UtcNow.ToString("yyyyMMdd-HHmmss")));
    Directory.CreateDirectory(root);
    var results = new List<ScenarioResult>();
    var started = DateTime.UtcNow;

    var crashMatrix = new Dictionary<string, string[]>
    {
        ["orderby"] = ["reading", "merging", "output"],
        ["distinct"] = ["reading", "partitioning", "output"],
        ["exceptby"] = ["reading", "readingsecondary", "partitioning", "output"],
        ["aggregate"] = ["reading", "partitioning", "output"],
        ["groupjoin"] = ["reading", "readingsecondary", "partitioning", "output"]
    };
    foreach (var entry in crashMatrix)
    {
        foreach (var phase in entry.Value)
        {
            var operation = entry.Key;
            var name = $"crash-{operation}-{phase}";
            results.Add(await ExecuteScenarioAsync(name, async scenarioRoot =>
            {
                var prepared = await PrepareKilledSessionAsync(scenarioRoot, operation, phase);
                var recovered = await RunChildAsync(
                    $"recover-{operation}", prepared.SessionDirectory, prepared.RecoveredPath);
                Require(recovered.ExitCode == 0,
                    $"Recovery failed with exit code {recovered.ExitCode}: {recovered.Error}{recovered.Output}");
                var measurement = ParseLastJson<RecoveryMeasurement>(recovered.Output);
                var deliveredBeforeCrash = File.Exists(prepared.DeliveredPath)
                    ? File.ReadLines(prepared.DeliveredPath).Select(long.Parse).ToArray()
                    : [];
                var recoveredValues = File.ReadLines(prepared.RecoveredPath).Select(long.Parse).ToArray();
                var combined = deliveredBeforeCrash.Concat(recoveredValues).ToArray();
                var expected = ExpectedOutput(operation);
                if (operation == "orderby")
                    Require(combined.SequenceEqual(expected), "Recovered ordering output was missing, duplicated, or reordered.");
                else
                    Require(combined.Order().SequenceEqual(expected.Order()),
                        "Combined pre-crash and recovered output contained missing, duplicate, or incorrect values.");
                Require(measurement.Count == recoveredValues.LongLength,
                    "The recovery measurement did not match the durable recovered-output ledger.");
                Require(measurement.Checksum == recoveredValues.Aggregate(0L, (sum, value) => unchecked(sum + value)),
                    "The recovery checksum did not match the durable recovered-output ledger.");
            }));
        }
    }

    results.Add(await CorruptionScenarioAsync("corrupt-block", root, sessionDirectory =>
    {
        var run = SingleOperationRun(sessionDirectory);
        using var stream = new FileStream(run, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        stream.Position = stream.Length - 1;
        var value = stream.ReadByte();
        stream.Position = stream.Length - 1;
        stream.WriteByte((byte)(value ^ 0x5A));
        stream.Flush(true);
    }, "checksum"));

    results.Add(await CorruptionScenarioAsync("missing-run", root, sessionDirectory =>
        File.Delete(SingleOperationRun(sessionDirectory)), "missing"));

    results.Add(await CorruptionScenarioAsync("truncated-checkpoint", root, sessionDirectory =>
    {
        var checkpoint = Path.Combine(sessionDirectory, $"checkpoint-orderby-{OperationId}.json");
        using var stream = new FileStream(checkpoint, FileMode.Open, FileAccess.Write, FileShare.None);
        stream.SetLength(1);
        stream.Flush(true);
    }, "JSON"));

    results.Add(await CorruptionScenarioAsync("malformed-manifest", root, sessionDirectory =>
    {
        var manifest = Path.Combine(sessionDirectory, "session.manifest.json");
        File.WriteAllText(manifest, "{not-valid-json");
    }, "JSON"));

    results.Add(await ExecuteScenarioAsync("disk-quota", async scenarioRoot =>
    {
        var child = await RunChildAsync("quota", scenarioRoot, "-");
        Require(child.ExitCode == 0,
            $"Disk-quota scenario failed with exit code {child.ExitCode}: {child.Error}{child.Output}");
        Require(child.Output.Contains("SpillResourceLimitException", StringComparison.Ordinal),
            "The worker did not report the expected disk-quota exception.");
    }));

    var report = new HarnessReport(
        started,
        DateTime.UtcNow,
        results.Count,
        results.Count(result => result.Passed),
        results.Count(result => !result.Passed),
        results);
    var reportPath = Path.Combine(root, "failure-harness-report.json");
    await File.WriteAllTextAsync(reportPath, JsonSerializer.Serialize(report,
        new JsonSerializerOptions { WriteIndented = true }));

    foreach (var result in results)
        Console.WriteLine($"{(result.Passed ? "PASS" : "FAIL"),-4} {result.Name,-24} {result.DurationMilliseconds,8:N0} ms {result.Detail}");
    Console.WriteLine($"Report: {reportPath}");
    return report.Failed == 0 ? 0 : 1;

    async Task<ScenarioResult> CorruptionScenarioAsync(
        string name,
        string outputRoot,
        Action<string> mutate,
        string expectedError)
    {
        return await ExecuteScenarioAsync(name, async scenarioRoot =>
        {
            var prepared = await PrepareKilledSessionAsync(scenarioRoot, "orderby", "output");
            mutate(prepared.SessionDirectory);
            var recovered = await RunChildAsync("recover-orderby", prepared.SessionDirectory, "-");
            Require(recovered.ExitCode != 0, "Recovery unexpectedly succeeded after durable state was damaged.");
            var diagnostic = recovered.Error + recovered.Output;
            Require(diagnostic.Contains(expectedError, StringComparison.OrdinalIgnoreCase),
                $"Expected a diagnostic containing '{expectedError}', received: {diagnostic}");
        });
    }

    async Task<ScenarioResult> ExecuteScenarioAsync(string name, Func<string, Task> execute)
    {
        var scenarioRoot = Path.Combine(root, name);
        Directory.CreateDirectory(scenarioRoot);
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await execute(scenarioRoot);
            return new ScenarioResult(name, true, stopwatch.Elapsed.TotalMilliseconds, "");
        }
        catch (Exception exception)
        {
            return new ScenarioResult(name, false, stopwatch.Elapsed.TotalMilliseconds, exception.Message);
        }
        finally
        {
            TryDelete(scenarioRoot);
        }
    }
}

static async Task<int> RunWorkerAsync(string[] workerArgs)
{
    var action = workerArgs.ElementAtOrDefault(0)
        ?? throw new ArgumentException("A worker action is required.");
    var path = workerArgs.ElementAtOrDefault(1)
        ?? throw new ArgumentException("A worker root or session path is required.");
    var markerPath = workerArgs.ElementAtOrDefault(2) ?? "-";

    try
    {
        if (action.StartsWith("prepare-", StringComparison.Ordinal))
        {
            var parts = action.Split('-', 3);
            if (parts.Length != 3) throw new ArgumentException($"Invalid prepare action '{action}'.");
            return await PrepareAndPauseAsync(path, markerPath, parts[1], parts[2]);
        }
        if (action.StartsWith("recover-", StringComparison.Ordinal))
            return await RecoverAsync(path, markerPath, action["recover-".Length..]);
        if (action == "quota")
            return await VerifyDiskQuotaAsync(path);
        throw new ArgumentException($"Unknown worker action '{action}'.");
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine(exception);
        return 2;
    }
}

static async Task<int> PrepareAndPauseAsync(
    string root,
    string markerPath,
    string operationName,
    string targetPhase)
{
    Directory.CreateDirectory(root);
    await using var session = new SpillSession(Options(root));
    session.PreserveForRecovery();

    void Checkpointed(SpillOperationPhase phase, long outputRecordsCheckpointed)
    {
        var normalized = phase.ToString().ToLowerInvariant();
        if (normalized != targetPhase || phase == SpillOperationPhase.Output && outputRecordsCheckpointed < 10)
            return;
        File.WriteAllText(markerPath, session.WorkingDirectory);
        Thread.Sleep(Timeout.Infinite);
    }

    var operation = RecoverableOutput(session, operationName, Checkpointed);
    await using var delivered = new StreamWriter(new FileStream(
        markerPath + ".delivered", FileMode.Create, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
    await foreach (var value in operation)
        await delivered.WriteLineAsync(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
    throw new InvalidOperationException(
        $"The {operationName} worker completed before reaching phase '{targetPhase}'.");
}

static async Task<int> RecoverAsync(
    string sessionDirectory,
    string recoveredOutputPath,
    string operationName)
{
    await using var session = SpillSession.Recover(sessionDirectory, Options(Path.GetDirectoryName(sessionDirectory)!));
    await using var recoveredOutput = recoveredOutputPath == "-" ? null : new StreamWriter(new FileStream(
        recoveredOutputPath, FileMode.Create, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
    long count = 0;
    long checksum = 0;
    await foreach (var value in RecoverableOutput(session, operationName, null))
    {
        count++;
        checksum = unchecked(checksum + value);
        if (recoveredOutput is not null)
            await recoveredOutput.WriteLineAsync(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    Console.WriteLine(JsonSerializer.Serialize(new RecoveryMeasurement(count, checksum)));
    return 0;
}

static IAsyncEnumerable<long> RecoverableOutput(
    SpillSession session,
    string operationName,
    Action<SpillOperationPhase, long>? checkpointed) => operationName switch
    {
        "orderby" => OrderByOutput(session, checkpointed),
        "distinct" => DistinctOutput(session, checkpointed),
        "exceptby" => ExceptByOutput(session, checkpointed),
        "aggregate" => AggregateOutput(session, checkpointed),
        "groupjoin" => GroupJoinOutput(session, checkpointed),
        _ => throw new ArgumentException($"Unknown recoverable operation '{operationName}'.")
    };

static async IAsyncEnumerable<long> OrderByOutput(
    SpillSession session,
    Action<SpillOperationPhase, long>? checkpointed)
{
    await foreach (var value in session.OrderByRecoverableSpill(
                       (offset, token) => DescendingSource(offset, RecordCount, token),
                       OperationId,
                       value => value,
                       outputCheckpointInterval: 10,
                       checkpointed: progress => checkpointed?.Invoke(
                           progress.Phase, progress.OutputRecordsCheckpointed)))
        yield return value;
}

static async IAsyncEnumerable<long> DistinctOutput(
    SpillSession session,
    Action<SpillOperationPhase, long>? checkpointed)
{
    await foreach (var value in session.DistinctRecoverableSpill(
                       (offset, token) => AscendingSource(offset, RecordCount, token),
                       OperationId,
                       value => value % 1_000,
                       outputCheckpointInterval: 10,
                       checkpointed: progress => checkpointed?.Invoke(
                           progress.Phase, progress.OutputRecordsCheckpointed)))
        yield return value;
}

static async IAsyncEnumerable<long> ExceptByOutput(
    SpillSession session,
    Action<SpillOperationPhase, long>? checkpointed)
{
    await foreach (var value in session.ExceptByRecoverableSpill(
                       (offset, token) => AscendingSource(offset, RecordCount, token),
                       (offset, token) => ExcludedKeys(offset, token),
                       OperationId,
                       value => value % 1_000,
                       outputCheckpointInterval: 10,
                       checkpointed: progress => checkpointed?.Invoke(
                           progress.Phase, progress.OutputRecordsCheckpointed)))
        yield return value;
}

static async IAsyncEnumerable<long> AggregateOutput(
    SpillSession session,
    Action<SpillOperationPhase, long>? checkpointed)
{
    await foreach (var value in session.GroupAggregateRecoverableSpill(
                       (offset, token) => AscendingSource(offset, RecordCount, token),
                       OperationId,
                       value => value % 1_000,
                       () => 0L,
                       (sum, value) => sum + value,
                       (key, sum) => key * 10_000_000L + sum,
                       outputCheckpointInterval: 10,
                       checkpointed: progress => checkpointed?.Invoke(
                           progress.Phase, progress.OutputRecordsCheckpointed)))
        yield return value;
}

static async IAsyncEnumerable<long> GroupJoinOutput(
    SpillSession session,
    Action<SpillOperationPhase, long>? checkpointed)
{
    await foreach (var value in session.GroupJoinStreamRecoverableSpill(
                       (offset, token) => AscendingSource(offset, RecordCount, token),
                       (offset, token) => ReferenceKeys(offset, token),
                       OperationId,
                       left => left % 1_000,
                       right => right,
                       async (left, matches, cancellationToken) =>
                       {
                           var count = 0;
                           await foreach (var _ in matches.WithCancellation(cancellationToken))
                               count++;
                           return left * 10L + count;
                       },
                       outputCheckpointInterval: 10,
                       checkpointed: progress => checkpointed?.Invoke(
                           progress.Phase, progress.OutputRecordsCheckpointed)))
        yield return value;
}

static async IAsyncEnumerable<int> ExcludedKeys(
    long offset,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    const int count = 334;
    for (var position = offset; position < count; position++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return (int)position * 3;
        await Task.Yield();
    }
}

static async IAsyncEnumerable<int> ReferenceKeys(
    long offset,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    for (var position = offset; position < 1_000; position++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return (int)position;
        await Task.Yield();
    }
}

static async IAsyncEnumerable<int> AscendingSource(
    long offset,
    int count,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    for (var position = offset; position < count; position++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return (int)position;
        if ((position & 1_023) == 0)
            await Task.Yield();
    }
}

/*
 * Keep all output adapters above deterministic: recovery can invoke selectors
 * again, and the harness relies on exact pre-crash plus post-recovery ledgers.
 */

static async Task<int> VerifyDiskQuotaAsync(string root)
{
    Directory.CreateDirectory(root);
    await using var session = new SpillSession(new SpillOptions
    {
        TemporaryDirectory = root,
        CleanupAbandonedSessions = false,
        ProtectTemporaryDirectory = false,
        MemoryLimitBytes = 64 * 1024,
        MaxDiskUsageBytes = 1_024,
        MergeFanIn = 2
    });
    try
    {
        await foreach (var _ in DescendingSource(0, RecordCount, CancellationToken.None)
                           .OrderBySpill(session, value => value)) { }
    }
    catch (SpillResourceLimitException exception)
    {
        Console.WriteLine($"{exception.GetType().Name}: {exception.Message}");
        return 0;
    }
    throw new InvalidOperationException("The operation exceeded its quota test size without failing.");
}

static async Task<PreparedSession> PrepareKilledSessionAsync(
    string root,
    string operationName,
    string phase)
{
    var marker = Path.Combine(root, "phase.marker");
    using var child = StartChild($"prepare-{operationName}-{phase}", root, marker);
    var deadline = DateTime.UtcNow.AddSeconds(60);
    while (!File.Exists(marker) && !child.HasExited && DateTime.UtcNow < deadline)
        await Task.Delay(25);
    if (!File.Exists(marker))
    {
        if (!child.HasExited)
        {
            child.Kill();
            child.WaitForExit(5_000);
        }
        var output = await child.StandardOutput.ReadToEndAsync();
        var error = await child.StandardError.ReadToEndAsync();
        throw new TimeoutException($"{operationName} worker did not reach phase '{phase}'. {error}{output}");
    }

    var sessionDirectory = (await File.ReadAllTextAsync(marker)).Trim();
    child.Kill();
    if (!child.WaitForExit(5_000))
        throw new TimeoutException($"Killed {operationName} worker did not exit within five seconds.");
    Require(Directory.Exists(sessionDirectory), "Killed worker did not leave a recoverable session directory.");
    return new PreparedSession(sessionDirectory, marker + ".delivered", marker + ".recovered");
}

static async Task<ChildResult> RunChildAsync(string action, string path, string marker)
{
    using var child = StartChild(action, path, marker);
    var outputTask = child.StandardOutput.ReadToEndAsync();
    var errorTask = child.StandardError.ReadToEndAsync();
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
    try
    {
        await child.WaitForExitAsync(timeout.Token);
    }
    catch (OperationCanceledException)
    {
        child.Kill(entireProcessTree: true);
        throw new TimeoutException($"Worker action '{action}' exceeded 90 seconds.");
    }
    return new ChildResult(child.ExitCode, await outputTask, await errorTask);
}

static Process StartChild(string action, string path, string marker)
{
    var executable = Environment.ProcessPath
        ?? throw new InvalidOperationException("Cannot locate the dotnet host executable.");
    var assembly = typeof(Program).Assembly.Location;
    var start = new ProcessStartInfo(executable)
    {
        UseShellExecute = false,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        CreateNoWindow = true
    };
    if (string.Equals(Path.GetFileNameWithoutExtension(executable), "dotnet", StringComparison.OrdinalIgnoreCase))
        start.ArgumentList.Add(assembly);
    start.ArgumentList.Add("worker");
    start.ArgumentList.Add(action);
    start.ArgumentList.Add(path);
    start.ArgumentList.Add(marker);
    return Process.Start(start) ?? throw new InvalidOperationException("Failed to launch failure-harness worker.");
}

static async IAsyncEnumerable<int> DescendingSource(
    long offset,
    int count,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    for (var position = offset; position < count; position++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return count - 1 - (int)position;
        if ((position & 1_023) == 0)
            await Task.Yield();
    }
}

static SpillOptions Options(string root) => new()
{
    TemporaryDirectory = root,
    CleanupAbandonedSessions = false,
    ProtectTemporaryDirectory = false,
    MemoryLimitBytes = 64 * 1024,
    MaxDiskUsageBytes = 256L * 1024 * 1024,
    MergeFanIn = 2,
    PartitionCount = 8,
    RecursivePartitionCount = 4
};

static string SingleOperationRun(string sessionDirectory) =>
    Directory.GetFiles(sessionDirectory, $"*.orderby-{OperationId}.run").Single();

static long[] ExpectedOutput(string operationName) => operationName switch
{
    "orderby" => Enumerable.Range(0, RecordCount).Select(value => (long)value).ToArray(),
    "distinct" => Enumerable.Range(0, 1_000).Select(value => (long)value).ToArray(),
    "exceptby" => Enumerable.Range(0, 1_000)
        .Where(value => value % 3 != 0).Select(value => (long)value).ToArray(),
    "aggregate" => Enumerable.Range(0, 1_000).Select(key =>
    {
        long sum = 0;
        for (var value = key; value < RecordCount; value += 1_000)
            sum += value;
        return key * 10_000_000L + sum;
    }).ToArray(),
    "groupjoin" => Enumerable.Range(0, RecordCount).Select(value => value * 10L + 1).ToArray(),
    _ => throw new ArgumentException($"Unknown recoverable operation '{operationName}'.")
};

static T ParseLastJson<T>(string output) where T : class
{
    var line = output.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries)
        .LastOrDefault(value => value.TrimStart().StartsWith('{'))
        ?? throw new InvalidDataException("Worker returned no JSON measurement.");
    return JsonSerializer.Deserialize<T>(line)
        ?? throw new InvalidDataException("Worker returned an invalid JSON measurement.");
}

static string? ReadOption(string[] values, string name)
{
    var index = Array.IndexOf(values, name);
    if (index < 0)
        return null;
    if (index + 1 >= values.Length)
        throw new ArgumentException($"Option '{name}' requires a value.");
    return values[index + 1];
}

static void Require(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}

static void TryDelete(string path)
{
    try
    {
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
    }
    catch (IOException) { }
    catch (UnauthorizedAccessException) { }
}

sealed record ChildResult(int ExitCode, string Output, string Error);
sealed record PreparedSession(string SessionDirectory, string DeliveredPath, string RecoveredPath);
sealed record RecoveryMeasurement(long Count, long Checksum);
sealed record ScenarioResult(string Name, bool Passed, double DurationMilliseconds, string Detail);
sealed record HarnessReport(
    DateTime StartedUtc,
    DateTime CompletedUtc,
    int Total,
    int Passed,
    int Failed,
    IReadOnlyList<ScenarioResult> Scenarios);
sealed class FixtureInterruptionException : Exception;
