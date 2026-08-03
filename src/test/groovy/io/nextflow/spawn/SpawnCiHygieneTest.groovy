package io.nextflow.spawn

import groovy.transform.CompileStatic
import spock.lang.Specification

import java.util.regex.Pattern

// Asserts on repo wiring rather than on plugin code (#80).
//
// Wiring is what rots: reverting a pin to `@v6` or deleting a Dependabot entry is
// a one-line change whose absence is completely silent — nothing fails, CI's
// supply chain just quietly goes back to being mutable (or, without Dependabot,
// silently freezes). This makes both fail.
//
// A Spock spec rather than a shell script because this repo already runs the Spock
// suite in CI (`./gradlew test`, #59), so the check needs no new job and no new
// tool. It reads files under the repo root and touches nothing else — no AWS, no
// instance, in keeping with the rest of the suite.
class SpawnCiHygieneTest extends Specification {

    // Gradle runs tests with the project directory as CWD; fall back to walking up
    // so the spec also works if that ever stops being true.
    private static File repoRoot() {
        File dir = new File('.').canonicalFile
        while (dir != null && !new File(dir, 'settings.gradle').isFile()) {
            dir = dir.parentFile
        }
        assert dir != null: 'could not locate the repo root (no settings.gradle found walking up from CWD)'
        return dir
    }

    private static List<File> workflowFiles() {
        def dir = new File(repoRoot(), '.github/workflows')
        assert dir.isDirectory(): "no .github/workflows directory at ${dir}"
        def files = (dir.listFiles() ?: [] as File[])
                .findAll { it.isFile() && (it.name.endsWith('.yml') || it.name.endsWith('.yaml')) }
                .sort { it.name }
        assert !files.isEmpty(): "no workflow files under ${dir}"
        return files
    }

    /** Every `uses:` ref in the workflows, as [file, lineNo, ref]. Local `./` paths excluded. */
    private static List<List> usesRefs() {
        def out = []
        workflowFiles().each { File f ->
            f.readLines().eachWithIndex { String line, int i ->
                def trimmed = line.trim()
                if (trimmed.startsWith('- ')) {
                    trimmed = trimmed.substring(2).trim()
                }
                if (!trimmed.startsWith('uses:')) {
                    return
                }
                def ref = trimmed.substring('uses:'.length()).trim()
                if (ref && !ref.startsWith('./')) {
                    out << [f.name, i + 1, ref]
                }
            }
        }
        return out
    }

    def 'every action is pinned to a full commit SHA with a version comment'() {
        // A tag is mutable: `@v6` means "whatever v6 points at when the job runs",
        // so the code executing in CI can change with no commit here.
        // actions/checkout@v6 really did move (df4cb1c 2026-06-02 -> d23441a
        // 2026-07-16), silently, exactly as tags are designed to.
        //
        // The trailing `# vX.Y.Z` comment is required too: a bare SHA is
        // unreadable, and the version is what makes a bump reviewable.
        given:
        def pinned = Pattern.compile(/^[^@\s]+@[0-9a-f]{40}\s+#\s*v?[0-9].*$/)
        def refs = usesRefs()

        expect: 'anti-vacuous — if the parser stops matching, this would pass forever'
        !refs.isEmpty()

        and:
        def unpinned = refs.findAll { !pinned.matcher(it[2] as String).matches() }
        assert unpinned.isEmpty(), 'these actions are not pinned to a commit SHA with a ' +
                'version comment, so the code CI runs can change with no commit here:\n' +
                unpinned.collect { "    ${it[0]}:${it[1]}  ${it[2]}" }.join('\n') +
                '\nUse: uses: owner/action@<40-hex-sha> # vX.Y.Z'
    }

    def 'Dependabot has a github-actions entry whose group patterns cover every action'() {
        // The other half of pinning. A SHA never moves, including past a security
        // fix, so pinning without Dependabot trades a mutable-tag hole for a
        // staleness one — which had already happened here: five checkout refs sat
        // on a SHA `@v6` moved off of six weeks earlier, and nothing said so.
        //
        // Coverage is the property that matters: an entry whose group patterns
        // don't match an action leaves it outside the grouped PR, silently. This
        // repo is exactly where `actions/*` would bite — gradle/actions,
        // aquasecurity/trivy-action and softprops/action-gh-release would all fall
        // outside it.
        given:
        def entry = dependabotEntry('github-actions')

        expect:
        entry.directory == '/'
        !entry.patterns.isEmpty()

        and:
        def actions = usesRefs().collect { (it[2] as String).split('@', 2)[0] }.unique()
        assert !actions.isEmpty()
        def uncovered = actions.findAll { action -> !entry.patterns.any { globMatches(it, action) } }
        assert uncovered.isEmpty(), "these actions match no Dependabot group pattern " +
                "${entry.patterns}, so they fall outside the grouped PR and stop being " +
                "bumped:\n" + uncovered.collect { "    $it" }.join('\n') + '\nWiden to "*".'
    }

    def 'Dependabot has a gradle entry, because the plugin versions are the whole dependency surface'() {
        // build.gradle has no `dependencies { }` block — compile/test deps come
        // from the io.nextflow.nextflow-plugin toolchain — so the two plugin
        // versions ARE the dependency surface, and nothing was watching them.
        // There is no govulncheck equivalent for Gradle here either; security.yml
        // runs gitleaks/Trivy/Semgrep, none of which bump a plugin version.
        expect:
        dependabotEntry('gradle').directory == '/'
    }

    def 'the pinned Gradle plugin versions are the ones Dependabot will be watching'() {
        // Guards the claim the gradle entry rests on: if these move to a version
        // catalog or a `dependencies { }` block, the entry above may silently stop
        // covering the real surface. Asserts the versions are declared as literals
        // where Dependabot's gradle ecosystem looks, not that they are current.
        given:
        def build = new File(repoRoot(), 'build.gradle').text
        def settings = new File(repoRoot(), 'settings.gradle').text

        expect: 'the Nextflow plugin toolchain version is a literal in build.gradle'
        build =~ /id\s+'io\.nextflow\.nextflow-plugin'\s+version\s+'[^']+'/

        and: 'the toolchain resolver version is a literal in settings.gradle'
        settings =~ /id\s+'org\.gradle\.toolchains\.foojay-resolver-convention'\s+version\s+'[^']+'/

        and: 'still no dependencies block — if one appears, re-check what the gradle entry covers'
        !(build =~ /(?m)^\s*dependencies\s*\{/)
    }

    // --- helpers -----------------------------------------------------------
    //
    // A deliberately narrow reader for the one file shape this repo has:
    // dependabot.yml as a `version:` scalar plus a flat `updates:` list of block
    // mappings. Not a YAML parser and not trying to be — adding a YAML library to
    // the build for one config file isn't worth it. It fails loudly (a missing
    // entry is an assertion failure, never a silent pass) rather than mis-parsing.

    @CompileStatic
    static class Entry {
        String directory = ''
        List<String> patterns = []
    }

    private static Entry dependabotEntry(String ecosystem) {
        def f = new File(repoRoot(), '.github/dependabot.yml')
        assert f.isFile(): "no ${f}: CI's actions are pinned to SHAs, so without Dependabot " +
                'nothing ever bumps them and they just freeze'

        def lines = []
        f.readLines().eachWithIndex { String raw, int i ->
            def line = raw
            def h = line.indexOf('#')
            if (h >= 0) {
                // Strip comments — safe only because no VALUE here contains '#'
                // (the `# vX.Y.Z` comments live in the workflows). Asserted rather
                // than assumed: naive stripping would truncate such a value and the
                // key would read as empty, a quiet mis-parse.
                def before = line.substring(0, h)
                def colon = before.indexOf(':')
                assert !(colon >= 0 && before.substring(colon + 1).trim()): "${f.name}:${i + 1}: " +
                        "a value on this line is followed by '#'; this reader strips comments " +
                        'naively and would truncate it. Move the comment to its own line.'
                line = before
            }
            if (line.trim()) {
                lines << line
            }
        }

        def version = lines.find { it.startsWith('version:') }?.replaceFirst(/^version:/, '')?.trim()
        assert version == '2': "dependabot.yml version = ${version}, want 2 (v1 is unsupported and ignored)"

        Entry found = null
        Entry current = null
        String currentName = null
        boolean inPatterns = false

        for (String line : lines) {
            def trimmed = line.trim()
            if (trimmed.startsWith('- package-ecosystem:')) {
                if (currentName == ecosystem) {
                    found = current
                    break
                }
                current = new Entry()
                currentName = unquote(trimmed.substring('- package-ecosystem:'.length()))
                inPatterns = false
                continue
            }
            if (current == null) {
                continue
            }
            if (trimmed.startsWith('directory:')) {
                current.directory = unquote(trimmed.substring('directory:'.length()))
                inPatterns = false
            } else if (trimmed == 'patterns:') {
                inPatterns = true
            } else if (inPatterns && trimmed.startsWith('- ')) {
                current.patterns << unquote(trimmed.substring(2))
            } else if (trimmed.endsWith(':')) {
                // Any other key (schedule:, groups:, a group name, …) ends a patterns list.
                inPatterns = false
            }
        }
        if (found == null && currentName == ecosystem) {
            found = current
        }

        assert found != null: "dependabot.yml has no `${ecosystem}` entry — see the comments in " +
                'that file for why both entries are load-bearing'
        return found
    }

    private static String unquote(String s) {
        def t = s.trim()
        if (t.length() >= 2 && (t.startsWith('"') || t.startsWith("'")) && t[-1] == t[0]) {
            return t.substring(1, t.length() - 1)
        }
        return t
    }

    /** Dependabot's only wildcard is `*`, matching any run of characters including `/`. */
    private static boolean globMatches(String pattern, String value) {
        def quoted = pattern.split(/\*/, -1).collect { Pattern.quote(it) }
        return value ==~ quoted.join('.*')
    }
}
