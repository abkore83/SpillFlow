using System.Security.AccessControl;
using System.Runtime.Versioning;
using System.Security.Principal;

namespace SpillFlow;

internal static class SpillDirectorySecurity
{
    private const UnixFileMode OwnerOnlyMode =
        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;

    public static void CreatePrivate(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            CreatePrivateWindowsDirectory(path);
            return;
        }

        Directory.CreateDirectory(path, OwnerOnlyMode);
        File.SetUnixFileMode(path, OwnerOnlyMode);
    }

    public static void ProtectExisting(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            ApplyWindowsAcl(path);
            return;
        }

        File.SetUnixFileMode(path, OwnerOnlyMode);
    }

    [SupportedOSPlatform("windows")]
    private static void CreatePrivateWindowsDirectory(string path)
    {
        FileSystemAclExtensions.CreateDirectory(BuildWindowsAcl(), path);
    }

    [SupportedOSPlatform("windows")]
    private static void ApplyWindowsAcl(string path)
    {
        new DirectoryInfo(path).SetAccessControl(BuildWindowsAcl());
    }

    [SupportedOSPlatform("windows")]
    private static DirectorySecurity BuildWindowsAcl()
    {
        using var identity = WindowsIdentity.GetCurrent();
        var user = identity.User ?? throw new InvalidOperationException(
            "The current Windows identity does not have a security identifier.");
        var security = new DirectorySecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            user,
            FileSystemRights.FullControl,
            InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.None,
            AccessControlType.Allow));
        return security;
    }
}
