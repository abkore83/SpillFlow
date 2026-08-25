using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using SpillFlow;

const int RecordCount = 50_000;
const string OperationId = "hard-crash";

if (args.ElementAtOrDefault(0) == "worker")
    return await RunWorkerAsync(args.Skip(1).ToArray());

return await RunHarnessAsync(args);

static async Task<int> RunHarnessAsync(string[] harnessArgs)
{
    var requestedOutput = ReadOption(harnessArgs, "--output");
    var root = Path.GetFullPath(requestedOutput ?? Path.Combine(
        "artifacts", "failure-harness", DateTime.UtcNow.ToString("yyyyMMdd-HHmmss")));
    Directory.CreateDirectory(root);
    var results = new List<ScenarioResult>();
    var started = DateTime.UtcNow;

    foreach (var phase in new[] { "reading", "merging", "output" })
    {
        var name = $"hard-crash-{phase}";
        results.Add(await ExecuteScenarioAsync(name, async scenarioRoot =>
        {
            var prepared = await PrepareKilledSessionAsync(scenarioRoot, phase);
            var recovered = await RunChildAsync("recover", prepared.SessionDirectory, prepared.RecoveredPath);
            Require(recovered.ExitCode == 0,
                $"Recovery failed with exit code {recovered.ExitCode}: {recovered.Error}{recovered.Output}");
            var measurement = ParseLastJson<RecoveryMeasurement>(recovered.Output);
            var deliveredBeforeCrash = File.Exists(prepared.DeliveredPath)
                ? File.ReadLines(prepared.DeliveredPath).Select(int.Parse).ToArray()
                : [];
            var recoveredValues = File.ReadLines(prepared.RecoveredPath).Select(int.Parse).ToArray();
            var combined = deliveredBeforeCrash.Concat(recoveredValues).ToArray();
            Require(combined.SequenceEqual(Enumerable.Range(0, RecordCount)),
                "The combined pre-crash and recovered output was missing, duplicated, or out of order.");
            Require(measurement.Count == recoveredValues.LongLength,
                "The recovery measurement did not match the durable recovered-output ledger.");
            Require(measurement.Checksum == recoveredValues.Sum(value => (long)value),
                "The recovery checksum did not match the durable recovered-output ledger.");
        }));
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
            var prepared = await PrepareKilledSessionAsync(scenarioRoot, "output");
            mutate(prepared.SessionDirectory);
            var recovered = await RunChildAsync("recover", prepared.SessionDirectory, "-");
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
        return action switch
        {
            "prepare-reading" => await PrepareAndPauseAsync(path, markerPath, "reading"),
            "prepare-merging" => await PrepareAndPauseAsync(path, markerPath, "merging"),
            "prepare-output" => await PrepareAndPauseAsync(path, markerPath, "output"),
            "recover" => await RecoverAsync(path, markerPath),
            "quota" => await VerifyDiskQuotaAsync(path),
            _ => throw new ArgumentException($"Unknown worker action '{action}'.")
        };
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine(exception);
        return 2;
    }
}

static async Task<int> PrepareAndPauseAsync(string root, string markerPath, string targetPhase)
{
    Directory.CreateDirectory(root);
    await using var session = new SpillSession(Options(root));
    session.PreserveForRecovery();

    bool IsTarget(SpillOperationProgress progress) => targetPhase switch
    {
        "reading" => progress.Phase == SpillOperationPhase.Reading && progress.RecordsConsumed > 0,
        "merging" => progress.Phase == SpillOperationPhase.Merging,
        "output" => progress.Phase == SpillOperationPhase.Output && progress.OutputRecordsCheckpointed >= 100,
        _ => false
    };

    var operation = session.OrderByRecoverableSpill(
        (offset, token) => DescendingSource(offset, RecordCount, token),
        OperationId,
        value => value,
        outputCheckpointInterval: 100,
        checkpointed: progress =>
        {
            if (!IsTarget(progress))
                return;
            File.WriteAllText(markerPath, session.WorkingDirectory);
            Thread.Sleep(Timeout.Infinite);
        });

    await using var delivered = new StreamWriter(new FileStream(
        markerPath + ".delivered", FileMode.Create, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
    await foreach (var value in operation)
        await delivered.WriteLineAsync(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
    throw new InvalidOperationException($"The worker completed before reaching phase '{targetPhase}'.");
}

static async Task<int> RecoverAsync(string sessionDirectory, string recoveredOutputPath)
{
    await using var session = SpillSession.Recover(sessionDirectory, Options(Path.GetDirectoryName(sessionDirectory)!));
    await using var recoveredOutput = recoveredOutputPath == "-" ? null : new StreamWriter(new FileStream(
        recoveredOutputPath, FileMode.Create, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
    long count = 0;
    long checksum = 0;
    await foreach (var value in session.OrderByRecoverableSpill(
                       (offset, token) => DescendingSource(offset, RecordCount, token),
                       OperationId,
                       value => value,
                       outputCheckpointInterval: 100))
    {
        count++;
        checksum = checked(checksum + value);
        if (recoveredOutput is not null)
            await recoveredOutput.WriteLineAsync(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    Console.WriteLine(JsonSerializer.Serialize(new RecoveryMeasurement(count, checksum)));
    return 0;
}

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

static async Task<PreparedSession> PrepareKilledSessionAsync(string root, string phase)
{
    var marker = Path.Combine(root, "phase.marker");
    using var child = StartChild($"prepare-{phase}", root, marker);
    var deadline = DateTime.UtcNow.AddSeconds(60);
    while (!File.Exists(marker) && !child.HasExited && DateTime.UtcNow < deadline)
        await Task.Delay(25);
    if (!File.Exists(marker))
    {
        var output = await child.StandardOutput.ReadToEndAsync();
        var error = await child.StandardError.ReadToEndAsync();
        if (!child.HasExited) child.Kill(entireProcessTree: true);
        throw new TimeoutException($"Worker did not reach phase '{phase}'. {error}{output}");
    }

    var sessionDirectory = (await File.ReadAllTextAsync(marker)).Trim();
    child.Kill(entireProcessTree: true);
    await child.WaitForExitAsync();
    Require(Directory.Exists(sessionDirectory), "Killed worker did not leave a recoverable session directory.");
    return new PreparedSession(sessionDirectory, marker + ".delivered", marker + ".recovered");
}

static async Task<ChildResult> RunChildAsync(string action, string path, string marker)
{
    using var child = StartChild(action, path, marker);
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
    return new ChildResult(
        child.ExitCode,
        await child.StandardOutput.ReadToEndAsync(),
        await child.StandardError.ReadToEndAsync());
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
    MergeFanIn = 2
};

static string SingleOperationRun(string sessionDirectory) =>
    Directory.GetFiles(sessionDirectory, $"*.orderby-{OperationId}.run").Single();

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
