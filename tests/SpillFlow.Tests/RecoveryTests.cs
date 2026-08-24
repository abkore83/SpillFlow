namespace SpillFlow.Tests;

public sealed class RecoveryTests
{
    [Fact]
    public async Task Preserved_session_restores_checkpoint_files_and_disk_accounting()
    {
        var root = Path.Combine(Path.GetTempPath(), $"spillflow-recovery-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        string workingDirectory;
        try
        {
            await using (var session = new SpillSession(new SpillOptions
            {
                TemporaryDirectory = root,
                CleanupAbandonedSessions = false
            }))
            {
                workingDirectory = session.WorkingDirectory;
                var path = session.NewFile();
                await using (var writer = new PooledRecordWriter<int>(path, session))
                {
                    await writer.WriteAsync(10, CancellationToken.None);
                    await writer.WriteAsync(20, CancellationToken.None);
                }
                await session.SaveCheckpointAsync("partition", new RecoveryState(2, "source-42"));
                session.PreserveForRecovery();
            }

            Assert.True(Directory.Exists(workingDirectory));
            await using var recovered = SpillSession.Recover(workingDirectory, new SpillOptions
            {
                TemporaryDirectory = root,
                CleanupAbandonedSessions = false
            });
            var checkpoint = await recovered.LoadCheckpointAsync<RecoveryState>("partition");
            var values = new List<int>();
            await foreach (var value in RecordFile.ReadAsync<int>(recovered.RecoveredFiles.Single(), recovered))
                values.Add(value);

            Assert.Equal(new[] { 10, 20 }, values);
            Assert.Equal(new RecoveryState(2, "source-42"), checkpoint);
            Assert.True(recovered.Statistics.CurrentDiskBytes > 0);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [Fact]
    public async Task Checkpoint_name_rejects_path_traversal()
    {
        await using var session = new SpillSession();
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await session.SaveCheckpointAsync("../outside", new { Value = 1 }));
    }

    private sealed record RecoveryState(int SourcePosition, string SourceToken);
}
