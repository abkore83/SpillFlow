namespace SpillFlow;

/// <summary>Identifies one logical output independently of retries and process restarts.</summary>
/// <param name="OperationId">The stable SpillFlow operation identifier.</param>
/// <param name="DeliveryKey">A caller-provided key that is stable for the logical output.</param>
public readonly record struct SpillDeliveryId(string OperationId, string DeliveryKey);

/// <summary>An output value and the stable identifier used to deduplicate its side effect.</summary>
/// <typeparam name="T">The output value type.</typeparam>
/// <param name="Id">The stable delivery identifier.</param>
/// <param name="Value">The output value.</param>
public sealed record SpillDelivery<T>(SpillDeliveryId Id, T Value);

/// <summary>
/// A transactional destination for recoverable SpillFlow output. Implementations must write the value and its
/// acknowledgement atomically in the same destination transaction.
/// </summary>
public interface ISpillTransactionalSink<T>
{
    /// <summary>Returns whether the destination has already committed this delivery.</summary>
    ValueTask<bool> IsAcknowledgedAsync(SpillDeliveryId deliveryId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically applies the output and records its acknowledgement. A duplicate delivery must have no effect.
    /// </summary>
    ValueTask CommitAndAcknowledgeAsync(SpillDelivery<T> delivery, CancellationToken cancellationToken = default);
}

/// <summary>Transactional output integration for recoverable SpillFlow operators.</summary>
public static class TransactionalOutputExtensions
{
    /// <summary>
    /// Sends output to a sink that atomically commits each effect with its acknowledgement. A recovered operator may
    /// replay output, but an acknowledged delivery is not applied again.
    /// </summary>
    /// <remarks>
    /// The key selector must return the same unique key for the same logical output on every retry. The sink must
    /// enforce uniqueness for <see cref="SpillDeliveryId"/> inside the same transaction as its business write.
    /// </remarks>
    public static async ValueTask ProcessTransactionallyAsync<T>(
        this IAsyncEnumerable<T> source,
        string operationId,
        Func<T, string> deliveryKeySelector,
        ISpillTransactionalSink<T> sink,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ValidateOperationId(operationId);
        ArgumentNullException.ThrowIfNull(deliveryKeySelector);
        ArgumentNullException.ThrowIfNull(sink);

        await foreach (var value in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var deliveryKey = deliveryKeySelector(value);
            if (string.IsNullOrWhiteSpace(deliveryKey))
                throw new InvalidOperationException("The delivery key selector returned an empty key.");

            var id = new SpillDeliveryId(operationId, deliveryKey);
            if (await sink.IsAcknowledgedAsync(id, cancellationToken).ConfigureAwait(false))
                continue;

            await sink.CommitAndAcknowledgeAsync(new SpillDelivery<T>(id, value), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static void ValidateOperationId(string operationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        if (operationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException(
                "Operation IDs may contain only ASCII letters, digits, '-' and '_'.", nameof(operationId));
    }
}
