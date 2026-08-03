#!/usr/bin/env bash
# Verify every action pin's `# vX.Y.Z` comment actually names the tag its SHA
# carries.
#
# This is the half of pin hygiene that needs the network, so it is NOT in the
# Spock suite (which must stay offline and hermetic). SpawnCiHygieneTest enforces
# the offline half: every pin is a 40-hex SHA with an exact vX.Y.Z comment.
#
# Why it exists: a pin's comment can be silently false. Dependabot bumped
# actions/checkout to 3d3c42e — which is v7.0.1 — while leaving `# v6` on all
# five refs, and a stale `# v4` on gradle/actions/setup-gradle (really v4.4.3)
# had sat on main for months. Both read as fine. A wrong label is worse than no
# label: it makes a major-version jump look like a routine same-line bump.
#
# Read-only: git ls-remote only. No auth, no writes, no side effects.
#
# Usage: scripts/verify-pins.sh
set -euo pipefail

cd "$(dirname "$0")/.."

fail=0
found=0

while IFS= read -r line; do
    # file:lineno:  ... uses: owner/action@<sha> # vX.Y.Z
    loc=${line%%:*}
    rest=${line#*:}
    lineno=${rest%%:*}

    ref=$(printf '%s\n' "$line" | sed -nE 's/.*uses:[[:space:]]*([^[:space:]]+@[0-9a-f]{40}).*/\1/p')
    ver=$(printf '%s\n' "$line" | sed -nE 's/.*@[0-9a-f]{40}[[:space:]]*#[[:space:]]*(v[0-9][^[:space:]]*).*/\1/p')
    [ -n "$ref" ] && [ -n "$ver" ] || continue

    action=${ref%@*}
    sha=${ref#*@}
    # gradle/actions/setup-gradle is a subdirectory action: the repo is the first
    # two path segments, so trim anything deeper before querying.
    repo=$(printf '%s\n' "$action" | cut -d/ -f1,2)

    found=$((found + 1))

    # Resolve the claimed tag to BOTH acceptable SHAs and accept either.
    #
    # For an annotated tag, `refs/tags/vX` is the tag OBJECT and `refs/tags/vX^{}`
    # the commit it points at, and the two differ. Both are legitimate pins —
    # gradle/actions/setup-gradle@48b5f21 is v4.4.3's tag object, actions/checkout
    # pins the commit — and GitHub resolves either. Comparing against just one form
    # reports a correct pin as mislabeled (which this script did on its first run,
    # absurdly printing "comment says v4.4.3 ... SHA is actually v4.4.3").
    claimed=$(git ls-remote "https://github.com/$repo" \
        "refs/tags/$ver" "refs/tags/$ver^{}" 2>/dev/null | awk '{print $1}')

    if [ -z "$claimed" ]; then
        printf '  MISSING TAG  %s:%s  %s @ %.12s  — tag %s does not exist in %s\n' \
            "$loc" "$lineno" "$action" "$sha" "$ver" "$repo"
        fail=1
    elif ! printf '%s\n' "$claimed" | grep -qFx "$sha"; then
        # Name what the SHA really is, so the fix is obvious rather than a puzzle.
        actual=$(git ls-remote --tags "https://github.com/$repo" 2>/dev/null \
            | awk -v s="$sha" '$1==s {gsub("refs/tags/",""); gsub(/\^\{\}$/,"",$2); print $2}' \
            | sort -u | paste -sd, -)
        printf '  MISLABELED   %s:%s  %s @ %.12s  — comment says %s (that tag is %.12s); SHA is actually %s\n' \
            "$loc" "$lineno" "$action" "$sha" "$ver" "$(printf '%s' "$claimed" | head -1)" \
            "${actual:-untagged}"
        fail=1
    else
        printf '  ok           %s:%s  %s %s\n' "$loc" "$lineno" "$action" "$ver"
    fi
done < <(grep -rn 'uses:.*@[0-9a-f]\{40\}' .github/workflows/)

# Anti-vacuous: a grep that stops matching would otherwise report success forever.
if [ "$found" -eq 0 ]; then
    echo "verify-pins: found no SHA-pinned actions under .github/workflows — the check" >&2
    echo "matched nothing, which is a parser problem, not a clean bill of health." >&2
    exit 1
fi

if [ "$fail" -ne 0 ]; then
    echo
    echo "verify-pins: FAIL — a pin's version comment does not match its SHA." >&2
    echo "Fix the comment to name the tag the SHA really carries (or move the SHA)." >&2
    exit 1
fi

echo
echo "verify-pins: OK — all $found pins' comments match their SHAs."
