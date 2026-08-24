namespace SpillFlow;

internal sealed class PartitionWriter<T> : IAsyncDisposable
{
    private readonly SpillSession _session;
    private readonly string[] _paths;
    private readonly PooledRecordWriter<T>?[] _writers;

    public IReadOnlyList<string> Paths => _paths;

    public PartitionWriter(SpillSession session, int? partitionCount = null, string extension = ".spill")
    {
        _session = session;
        _paths = Enumerable.Range(0, partitionCount ?? session.Options.PartitionCount)
            .Select(_ => session.NewFile(extension)).ToArray();
        _writers = new PooledRecordWriter<T>[_paths.Length];
    }

    public async ValueTask WriteAsync(int partition, T item, CancellationToken cancellationToken)
    {
        var writer = _writers[partition] ??= new PooledRecordWriter<T>(
            _paths[partition], _session, _session.GetIoBlockSize(_writers.Length));
        await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var writer in _writers)
            if (writer is not null)
                await writer.DisposeAsync().ConfigureAwait(false);
    }
}
