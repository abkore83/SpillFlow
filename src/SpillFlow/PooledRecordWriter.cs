using System.Buffers;
using System.Security.Cryptography;
using System.Text.Json;

namespace SpillFlow;

internal sealed class PooledRecordWriter<T> : IAsyncDisposable
{
    private readonly SpillSession _session;
    private readonly FileStream _stream;
    private byte[]? _buffer;
    private int _position;
    private SpillMemoryLease? _bufferLease;

    public PooledRecordWriter(string path, SpillSession session, int? blockSize = null)
    {
        _session = session;
        _buffer = ArrayPool<byte>.Shared.Rent(blockSize ?? session.GetIoBlockSize());
        try
        {
            _bufferLease = session.AcquireMemoryLease(_buffer.Length, $"I/O buffer for '{Path.GetFileName(path)}'");
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null;
            throw;
        }
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None,
            _buffer.Length, FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    public async ValueTask WriteAsync(T value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (value is int intValue)
        {
            await EnsureAsync(8, cancellationToken).ConfigureAwait(false);
            BitConverter.TryWriteBytes(_buffer!.AsSpan(_position, 4), 4);
            BitConverter.TryWriteBytes(_buffer.AsSpan(_position + 4, 4), intValue);
            _position += 8;
            _session.WroteRecord(8);
            return;
        }
        if (value is long longValue)
        {
            await EnsureAsync(12, cancellationToken).ConfigureAwait(false);
            BitConverter.TryWriteBytes(_buffer!.AsSpan(_position, 4), 8);
            BitConverter.TryWriteBytes(_buffer.AsSpan(_position + 4, 8), longValue);
            _position += 12;
            _session.WroteRecord(12);
            return;
        }
        if (value is Guid guid)
        {
            await EnsureAsync(20, cancellationToken).ConfigureAwait(false);
            BitConverter.TryWriteBytes(_buffer!.AsSpan(_position, 4), 16);
            guid.TryWriteBytes(_buffer.AsSpan(_position + 4, 16));
            _position += 20;
            _session.WroteRecord(20);
            return;
        }

        var payload = _session.TryGetCodec<T>(out var codec)
            ? codec!.Encode(value)
            : JsonSerializer.SerializeToUtf8Bytes(value);
        var recordLength = checked(sizeof(int) + payload.Length);
        if (recordLength > _buffer!.Length)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
            var record = ArrayPool<byte>.Shared.Rent(recordLength);
            try
            {
                BitConverter.TryWriteBytes(record.AsSpan(0, sizeof(int)), payload.Length);
                payload.CopyTo(record.AsSpan(sizeof(int), payload.Length));
                await WriteBlockAsync(record.AsMemory(0, recordLength), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(record);
            }
            _session.WroteRecord(recordLength);
            return;
        }

        await EnsureAsync(recordLength, cancellationToken).ConfigureAwait(false);
        BitConverter.TryWriteBytes(_buffer.AsSpan(_position, sizeof(int)), payload.Length);
        payload.CopyTo(_buffer.AsSpan(_position + sizeof(int)));
        _position += recordLength;
        _session.WroteRecord(recordLength);
    }

    private async ValueTask EnsureAsync(int required, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_buffer is null, this);
        if (_position + required > _buffer.Length)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        if (_position == 0)
            return;
        await WriteBlockAsync(_buffer!.AsMemory(0, _position), cancellationToken).ConfigureAwait(false);
        _position = 0;
    }

    private async ValueTask WriteBlockAsync(ReadOnlyMemory<byte> plaintext, CancellationToken cancellationToken)
    {
        var checksum = SpillBlockFormat.ComputeCrc32(plaintext.Span);
        var key = _session.EncryptionKey;
        byte[]? ciphertext = null;
        byte[] nonce = [];
        byte[] tag = [];
        ReadOnlyMemory<byte> payload = plaintext;
        if (key is not null)
        {
            ciphertext = new byte[plaintext.Length];
            nonce = RandomNumberGenerator.GetBytes(SpillBlockFormat.NonceSize);
            tag = new byte[SpillBlockFormat.TagSize];
            using var aes = new AesGcm(key, SpillBlockFormat.TagSize);
            aes.Encrypt(nonce, plaintext.Span, ciphertext, tag);
            payload = ciphertext;
        }

        var header = SpillBlockFormat.CreateHeader(payload.Length, plaintext.Length, checksum, nonce, tag, key is not null);
        _session.ReserveDisk(checked(header.Length + payload.Length));
        await _stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        _session.WroteBlock();
    }

    public async ValueTask DisposeAsync()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is null)
            return;
        try
        {
            if (_position > 0)
            {
                await WriteBlockAsync(buffer.AsMemory(0, _position), CancellationToken.None).ConfigureAwait(false);
                _position = 0;
            }
        }
        finally
        {
            try
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                Interlocked.Exchange(ref _bufferLease, null)?.Dispose();
            }
        }
    }
}
