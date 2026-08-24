namespace SpillFlow;

/// <summary>Requests a compile-time <see cref="ISpillCodec{T}"/> for the annotated type.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class GenerateSpillCodecAttribute : Attribute;
