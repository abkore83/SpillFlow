# Contributing

Thanks for helping improve SpillFlow.

## Development

Requirements: .NET 8 SDK and enough temporary disk space for the tests you run.

```powershell
dotnet restore SpillFlow.sln
dotnet build SpillFlow.sln -c Release --no-restore
dotnet test SpillFlow.sln -c Release --no-build --no-restore
```

Keep changes focused. Add regression tests for behavior changes, preserve bounded
resource behavior, and do not commit `bin`, `obj`, benchmark output, packages, or
temporary spill files. Performance claims should include the command, input
shape, memory cap, output count, checksum, and machine/storage context.

## Pull Requests

- Explain the problem and the chosen behavior.
- Include tests for correctness and relevant resource limits.
- Update README, CHANGELOG, or DRAWBACKS when public behavior changes.
- Confirm the Release build and test suite pass.
- Avoid unrelated formatting or API changes.

By contributing, you agree that your contribution is licensed under the MIT
License used by this repository.
