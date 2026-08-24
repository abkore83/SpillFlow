using System.Buffers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace SpillFlow;

internal static class RecordFile
{
    public static int EstimateSize<T>(T value, SpillSession session)
    {
        if (value is int) return 8;
        if (value is long) return 12;
        if (value is Guid) return 20;
        if (session.TryGetCodec<T>(out var codec)) return sizeof(int) + codec!.GetEncodedSize(value);
        session.GetSchemaFingerprint<T>();
        return sizeof(int) + JsonSerializer.SerializeToUtf8Bytes(value).Length;
    }

    public static async IAsyncEnumerable<T> ReadAsync<T>(
        string path,
        SpillSession session,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var expectedSchemaFingerprint = session.GetSchemaFingerprint<T>();
        session.ReaderOpened();
        try
        {
            await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var headerBytes = new byte[SpillBlockFormat.HeaderSize];
            while (stream.Position < stream.Length)
            {
                await stream.ReadExactlyAsync(headerBytes, cancellationToken).ConfigureAwait(false);
                var header = SpillBlockFormat.ParseHeader(headerBytes);
                if (!CryptographicOperations.FixedTimeEquals(header.SchemaFingerprint, expectedSchemaFingerprint))
                    throw new InvalidDataException(
                        $"Spill block schema fingerprint does not match the codec registered for '{typeof(T)}'.");
                var leaseBytes = checked((long)header.PayloadLength + (header.Encrypted ? header.PlainLength : 0));
                using var lease = session.AcquireMemoryLease(leaseBytes, $"read block from '{Path.GetFileName(path)}'");
                var payload = ArrayPool<byte>.Shared.Rent(header.PayloadLength);
                byte[]? decrypted = null;
                try
                {
                    await stream.ReadExactlyAsync(payload.AsMemory(0, header.PayloadLength), cancellationToken).ConfigureAwait(false);
                    ReadOnlyMemory<byte> plaintext;
                    if (header.Encrypted)
                    {
                        var key = session.EncryptionKey ?? throw new CryptographicException(
                            "This spill block is encrypted, but the session has no encryption key.");
                        decrypted = ArrayPool<byte>.Shared.Rent(header.PlainLength);
                        using var aes = new AesGcm(key, SpillBlockFormat.TagSize);
                        aes.Decrypt(header.Nonce, payload.AsSpan(0, header.PayloadLength), header.Tag,
                            decrypted.AsSpan(0, header.PlainLength));
                        plaintext = decrypted.AsMemory(0, header.PlainLength);
                    }
                    else
                    {
                        if (header.PayloadLength != header.PlainLength)
                            throw new InvalidDataException("Unencrypted block length mismatch.");
                        plaintext = payload.AsMemory(0, header.PlainLength);
                    }

                    if (SpillBlockFormat.ComputeCrc32(plaintext.Span) != header.Checksum)
                        throw new InvalidDataException("Spill block checksum mismatch.");

                    var offset = 0;
                    while (offset < plaintext.Length)
                    {
                        if (plaintext.Length - offset < sizeof(int))
                            throw new InvalidDataException("Truncated spill record header.");
                        var length = BitConverter.ToInt32(plaintext.Span.Slice(offset, sizeof(int)));
                        offset += sizeof(int);
                        if (length < 0 || length > plaintext.Length - offset)
                            throw new InvalidDataException($"Invalid spill record length: {length}.");
                        var record = plaintext.Slice(offset, length);
                        offset += length;
                        yield return DeserializeRecord<T>(record, session);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(payload);
                    if (decrypted is not null)
                        ArrayPool<byte>.Shared.Return(decrypted);
                }
            }
        }
        finally
        {
            session.ReaderClosed();
        }
    }

    private static T DeserializeRecord<T>(ReadOnlyMemory<byte> record, SpillSession session)
    {
        if (typeof(T) == typeof(int) && record.Length == 4)
            return (T)(object)BitConverter.ToInt32(record.Span);
        if (typeof(T) == typeof(long) && record.Length == 8)
            return (T)(object)BitConverter.ToInt64(record.Span);
        if (typeof(T) == typeof(Guid) && record.Length == 16)
            return (T)(object)new Guid(record.Span);
        if (session.TryGetCodec<T>(out var codec))
            return codec!.Decode(record.Span);
        return JsonSerializer.Deserialize<T>(record.Span)
            ?? throw new InvalidDataException("A spill record deserialized to null.");
    }
}
