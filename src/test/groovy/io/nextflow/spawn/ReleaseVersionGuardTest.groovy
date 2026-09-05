package io.nextflow.spawn

import spock.lang.Specification

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

// Companion tests for scripts/check-release-version.sh, the release-time guard
// added for #91 (the v0.10.0 release zip's MANIFEST.MF said Plugin-Version:
// 0.8.0 while the tag was v0.10.0 — build.gradle's `version` line wasn't
// bumped before the tag, and nothing checked what the *built artifact*
// actually reported).
//
// Two failure modes matter here, mirroring lagotto's release_version_guard_test.go:
//   - the guard script's own version-comparison logic could be wrong/vacuous
//   - the release workflow could stop calling the guard script entirely
// Neither is loud on its own: a broken comparison just always "passes", and a
// deleted workflow step means the guard silently never runs again.
class ReleaseVersionGuardTest extends Specification {

    private static File repoRoot() {
        File dir = new File('.').canonicalFile
        while (dir != null && !new File(dir, 'settings.gradle').isFile()) {
            dir = dir.parentFile
        }
        assert dir != null: 'could not locate the repo root (no settings.gradle found walking up from CWD)'
        return dir
    }

    private static File guardScript() {
        def f = new File(repoRoot(), 'scripts/check-release-version.sh')
        assert f.isFile(): "no ${f} — the release-time manifest-version guard is missing"
        return f
    }

    /** Builds a minimal plugin-shaped zip with the given Plugin-Version, no Gradle involved. */
    private static File fakePluginZip(File dir, String pluginVersion) {
        def zip = new File(dir, "nf-spawn-fake.zip")
        zip.withOutputStream { out ->
            def zos = new ZipOutputStream(out)
            zos.putNextEntry(new ZipEntry('classes/META-INF/MANIFEST.MF'))
            zos.write(("""Manifest-Version: 1.0
Plugin-Id: nf-spawn
Plugin-Version: ${pluginVersion}
Plugin-Requires: >=26.04.3
Plugin-Class: io.nextflow.spawn.SpawnPlugin
Plugin-Provider: scttfrdmn
""").getBytes('UTF-8'))
            zos.closeEntry()
            zos.close()
        }
        return zip
    }

    private static ProcessResult runGuard(String tag, File zip) {
        def pb = new ProcessBuilder('bash', guardScript().absolutePath, tag, zip.absolutePath)
        pb.redirectErrorStream(true)
        def proc = pb.start()
        def output = proc.inputStream.text
        def exit = proc.waitFor()
        return new ProcessResult(exit, output)
    }

    @groovy.transform.Immutable(knownImmutableClasses = [String])
    static class ProcessResult {
        int exitCode
        String output
    }

    def 'the guard fails a manifest whose Plugin-Version does not match the tag'() {
        given: 'a fake zip reproducing the exact #91 shape — manifest says 0.8.0, tag is v0.10.0'
        def dir = File.createTempDir()
        def zip = fakePluginZip(dir, '0.8.0')

        when:
        def result = runGuard('v0.10.0', zip)

        then: 'the guard must not pass a mismatched manifest'
        result.exitCode != 0
        result.output.contains('0.8.0')
        result.output.contains('v0.10.0')

        cleanup:
        dir.deleteDir()
    }

    def 'the guard passes a manifest whose Plugin-Version matches the tag'() {
        given:
        def dir = File.createTempDir()
        def zip = fakePluginZip(dir, '0.10.0')

        when:
        def result = runGuard('v0.10.0', zip)

        then: 'a correct manifest must not be rejected — anti-vacuous check on the guard itself'
        result.exitCode == 0
        result.output.contains('Release version check passed')

        cleanup:
        dir.deleteDir()
    }

    def 'the guard fails when the zip has no manifest at all'() {
        given: 'a zip with none of the expected plugin contents'
        def dir = File.createTempDir()
        def zip = new File(dir, 'empty.zip')
        zip.withOutputStream { out ->
            def zos = new ZipOutputStream(out)
            zos.putNextEntry(new ZipEntry('classes/'))
            zos.closeEntry()
            zos.close()
        }

        when:
        def result = runGuard('v0.10.0', zip)

        then:
        result.exitCode != 0

        cleanup:
        dir.deleteDir()
    }

    def 'the release workflow still invokes the release version guard script'() {
        // Guards against the step being deleted or renamed away from calling
        // scripts/check-release-version.sh — the guard script above is only
        // useful if the release workflow actually runs it on every tag push.
        given:
        def workflow = new File(repoRoot(), '.github/workflows/release.yml')
        assert workflow.isFile(): "no ${workflow}"

        expect:
        workflow.text.contains('scripts/check-release-version.sh')
    }

    def 'the release workflow no longer relies solely on the old source-line version check'() {
        // The pre-#91 check only grepped build.gradle's `version = '...'` line,
        // which was already correct when the built zip's manifest was wrong —
        // that's exactly how #91 shipped. Guards against silently reverting to
        // (or leaving stacked alongside) that check as if it were sufficient.
        given:
        def workflow = new File(repoRoot(), '.github/workflows/release.yml')

        expect:
        !(workflow.text =~ /grep -E ["']\^version["'] build\.gradle/)
    }
}
