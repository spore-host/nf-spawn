package io.nextflow.spawn

import spock.lang.Specification

import java.nio.file.Path

class SpawnTaskHandlerTest extends Specification {

    def 'passes the task script via --user-data-file, not a bare --user-data path (#13)'() {
        when:
        def cmd = SpawnTaskHandler.buildLaunchCommand(
            'nf-abc123', 't3.medium', 'us-east-1', '2h', false, '/tmp/nf-spawn-abc.sh', '', 0)

        then:
        // The script path must be carried by --user-data-file so spawn reads
        // the file's contents. A bare --user-data <path> would be treated as
        // inline user-data and the script would never run on the instance.
        cmd.contains('--user-data-file')
        cmd[cmd.indexOf('--user-data-file') + 1] == '/tmp/nf-spawn-abc.sh'

        and: 'the broken form must not reappear'
        !cmd.contains('--user-data')

        and: 'launch does not block on running/ssh'
        cmd.contains('--wait-for-running=false')
        cmd.contains('--wait-for-ssh=false')

        and: 'auto-approves so a non-TTY pipe invocation never blocks on a prompt (#18)'
        cmd.contains('-y')
    }

    def 'adds --spot only when requested'() {
        expect:
        SpawnTaskHandler.buildLaunchCommand('n', 't3.medium', 'us-east-1', '2h', spot, '/s.sh', '', 0)
            .contains('--spot') == expected

        where:
        spot  | expected
        true  | true
        false | false
    }

    def 'passes --ami only when ext.ami is set, to avoid SSM auto-detect (#18 / spawn#38)'() {
        when:
        def withAmi = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', 'ami-0123456789abcdef0', 0)
        def withoutAmi = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0)

        then: 'an explicit AMI is forwarded as --ami <id>'
        withAmi.contains('--ami')
        withAmi[withAmi.indexOf('--ami') + 1] == 'ami-0123456789abcdef0'

        and: 'no --ami when unset, so spawn keeps its own auto-detect behavior'
        !withoutAmi.contains('--ami')
    }

    def 'passes --volume-size only when ext.volumeSize > 0 (#21)'() {
        when:
        def withSize = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 100)
        def withoutSize = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0)

        then: 'an explicit volume size is forwarded as --volume-size <gib>'
        withSize.contains('--volume-size')
        withSize[withSize.indexOf('--volume-size') + 1] == '100'

        and: 'no --volume-size when unset, so spawn auto-floors at the AMI minimum (spawn#25)'
        !withoutSize.contains('--volume-size')
    }

    def 'staging script syncs the S3 work dir down, runs the task, and syncs results back (#14)'() {
        given:
        def workDir = 's3://my-bucket/work/ab/cdef0123456789'

        when:
        def script = SpawnTaskHandler.buildStagingScript(workDir, 'us-west-2', 'echo hello > out.txt', '')

        then: 'inputs are staged down from the S3 work dir before the task runs'
        def downIdx = script.indexOf('aws s3 sync "${WORKDIR_S3}" "${LOCAL_DIR}/"')
        downIdx >= 0

        and: 'the task script is materialized and run, capturing the real exit code'
        script.contains('echo hello > out.txt')
        script.contains('bash .command.sh 1>.command.out 2>.command.err')
        script.contains('TASK_RC=$?')
        script.contains('echo "${TASK_RC}" > .exitcode')

        and: 'outputs sync back up (excluding .exitcode), then .exitcode is uploaded last'
        def upOutputsIdx = script.indexOf('aws s3 sync "${LOCAL_DIR}/" "${WORKDIR_S3}" --region "${AWS_REGION}" --exclude ".exitcode"')
        def upExitIdx    = script.indexOf('aws s3 cp .exitcode')
        upOutputsIdx >= 0
        upExitIdx > upOutputsIdx

        and: 'ordering: stage-down precedes run precedes stage-up'
        downIdx < script.indexOf('bash .command.sh')
        script.indexOf('echo "${TASK_RC}" > .exitcode') < upOutputsIdx

        and: 'the work dir URI and region are injected, single-quoted'
        script.contains("WORKDIR_S3='s3://my-bucket/work/ab/cdef0123456789'")
        script.contains("AWS_REGION='us-west-2'")

        and: 'LOCAL_DIR is on the EBS root volume, not the /tmp tmpfs RAM disk (#27)'
        script.contains('LOCAL_DIR=/var/lib/nf-work')
        !script.contains('LOCAL_DIR=/tmp')
    }

    def 'run line is bare bash when no container is set'() {
        expect:
        SpawnTaskHandler.buildRunLine(container) == 'bash .command.sh 1>.command.out 2>.command.err\n'

        where:
        container << [null, '', '   ']
    }

    def 'run line wraps the task in docker run when a container directive is set (#30)'() {
        when:
        def line = SpawnTaskHandler.buildRunLine('biocontainers/fastqc:0.12.1--hdfd78af_0')

        then: 'runs inside the image via docker, with the work dir bind-mounted and set as cwd'
        line.contains('docker run --rm -v "${LOCAL_DIR}":"${LOCAL_DIR}" -w "${LOCAL_DIR}"')
        line.contains("'biocontainers/fastqc:0.12.1--hdfd78af_0'")
        line.contains('bash .command.sh 1>.command.out 2>.command.err')

        and: 'it does NOT run the script on the bare OS'
        !line.startsWith('bash .command.sh')
    }

    def 'staging script honors the container directive end-to-end (#30)'() {
        when:
        def script = SpawnTaskHandler.buildStagingScript(
            's3://b/work/aa/bb', 'us-east-1', 'fastqc reads.fq', 'biocontainers/fastqc:0.12.1--hdfd78af_0')

        then: 'the task runs inside the container, not on the OS'
        script.contains('docker run --rm')
        script.contains("'biocontainers/fastqc:0.12.1--hdfd78af_0' bash .command.sh")

        and: 'the real exit code (from docker run) is still captured'
        script.contains('TASK_RC=$?')
        script.contains('echo "${TASK_RC}" > .exitcode')
    }

    def 'run line splices docker.runOptions before the image (#39)'() {
        when:
        def line = SpawnTaskHandler.buildRunLine('biocontainers/fastqc:0.12.1--hdfd78af_0', '--user root')

        then: 'the run options appear between the docker run flags and the image'
        line.contains('-w "${LOCAL_DIR}" --user root \'biocontainers/fastqc:0.12.1--hdfd78af_0\'')

        and: 'still runs the task script inside the container'
        line.contains('bash .command.sh 1>.command.out 2>.command.err')
    }

    def 'run line omits run options when none are given (#39)'() {
        when:
        def line = SpawnTaskHandler.buildRunLine('biocontainers/fastqc:0.12.1--hdfd78af_0')

        then: 'no stray separator between the docker flags and the image'
        line.contains('-w "${LOCAL_DIR}" \'biocontainers/fastqc:0.12.1--hdfd78af_0\'')
    }

    def 'staging script makes the work dir world-writable and honors run options (#39)'() {
        when:
        def script = SpawnTaskHandler.buildStagingScript(
            's3://b/work/aa/bb', 'us-east-1', 'fastqc reads.fq',
            'biocontainers/fastqc:0.12.1--hdfd78af_0', [:], '--user root')

        then: 'the work dir is made writable so a non-root container user can write outputs'
        script.contains('chmod 0777 "${LOCAL_DIR}"')

        and: 'the resolved run options reach the docker run line'
        script.contains('--user root \'biocontainers/fastqc:0.12.1--hdfd78af_0\' bash .command.sh')
    }

    def 'dedentTaskScript strips the common leading indentation, flush-left like Nextflow (#43)'() {
        given: 'an nf-core-style indented script with a `<<-` versions heredoc'
        def src = [
            '    fastqc reads.fq',
            '',
            '    cat <<-END_VERSIONS > versions.yml',
            '    "FASTQC":',
            '        fastqc: 0.12.1',
            '    END_VERSIONS',
        ].join('\n')

        when:
        def out = SpawnTaskHandler.dedentTaskScript(src)

        then: 'the common 4-space indent is removed so the heredoc terminator lands at column 0'
        out.readLines().contains('END_VERSIONS')
        out.startsWith('fastqc reads.fq')

        and: 'relative indentation INSIDE the script is preserved'
        out.contains('\n    fastqc: 0.12.1\n')

        and: 'the blank line is untouched (not used to compute the common prefix)'
        out.contains('\n\ncat <<-END_VERSIONS')
    }

    def 'dedentTaskScript leaves an already flush-left script unchanged (#43)'() {
        expect:
        SpawnTaskHandler.dedentTaskScript('echo hi\nls -l') == 'echo hi\nls -l'
        SpawnTaskHandler.dedentTaskScript('') == ''
        SpawnTaskHandler.dedentTaskScript(null) == ''
    }

    def 'staging script writes the task body flush-left so `<<-` heredoc terminators match (#43)'() {
        given:
        def taskScript = '    cat <<-END_VERSIONS > versions.yml\n    "FASTQC":\n    END_VERSIONS'

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', taskScript, '')

        then: 'the embedded terminator is at column 0, not space-indented'
        script.contains('\nEND_VERSIONS\nNF_SPAWN_TASK_EOF')
        !script.contains('    END_VERSIONS')
    }

    def 'completion is signaled only after the task, reflecting its real exit code (#24)'() {
        when:
        def script = SpawnTaskHandler.buildStagingScript(
            's3://b/work/aa/bb', 'us-east-1', 'run_task', '')

        then: 'status is derived from the task exit code, not hard-coded success'
        script.contains('if [ "${TASK_RC}" -eq 0 ]; then COMPLETE_STATUS=success; else COMPLETE_STATUS=failed; fi')
        script.contains('spored complete --status "${COMPLETE_STATUS}"')

        and: 'the unconditional success signal must not reappear (the #24 bug)'
        !script.contains('spored complete --status success')

        and: 'completion is the LAST step — strictly after the task runs and outputs sync'
        script.indexOf('spored complete') > script.indexOf('bash .command.sh')
        script.indexOf('spored complete') > script.indexOf('aws s3 cp .exitcode')
    }

    // ── #37: declared input localization ──────────────────────────────────────

    // A java.nio.file.Path stub whose toUri() returns the given URI — lets us
    // test stage-in generation without registering the S3 NIO provider.
    private Path s3Path(String uri) {
        Stub(Path) { toUri() >> URI.create(uri) }
    }

    def 'staging localizes each declared s3:// input by its source URI to its stage name (#37)'() {
        given: 'inputs that live OUTSIDE this task work dir (upstream/samplesheet)'
        def inputs = [
            'reads_1.fastq.gz': s3Path('s3://data/sra/SRR059374_1.fastq.gz'),
            'reads_2.fastq.gz': s3Path('s3://data/sra/SRR059374_2.fastq.gz'),
        ] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'fastqc reads_1.fastq.gz reads_2.fastq.gz', '', inputs)

        then: 'each input is copied from its real source to ${LOCAL_DIR}/<stageName>, with ${LOCAL_DIR} OUTSIDE the quotes so it expands (#41)'
        script.contains('''aws s3 cp 's3://data/sra/SRR059374_1.fastq.gz' "${LOCAL_DIR}/"'reads_1.fastq.gz' --region "${AWS_REGION}"''')
        script.contains('''aws s3 cp 's3://data/sra/SRR059374_2.fastq.gz' "${LOCAL_DIR}/"'reads_2.fastq.gz' --region "${AWS_REGION}"''')

        and: 'the destination does not pass an unexpanded literal ${LOCAL_DIR} to aws (#41 bug 2)'
        !script.contains('''cp 's3://data/sra/SRR059374_1.fastq.gz' '${LOCAL_DIR}/''')

        and: 'a failed input copy fails loud instead of being silently ignored (#41)'
        script.contains('|| { echo "nf-spawn: failed to stage input reads_1.fastq.gz')

        and: 'inputs are staged before the task runs'
        script.indexOf('aws s3 cp ') < script.indexOf('bash .command.sh')
    }

    def 'directory inputs (trailing slash) are copied recursively (#37)'() {
        given:
        def inputs = ['refdir': s3Path('s3://data/genome/')] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'use refdir', '', inputs)

        then:
        script.contains('''aws s3 cp 's3://data/genome/' "${LOCAL_DIR}/"'refdir' --region "${AWS_REGION}" --recursive''')
    }

    def 'a stage name with a subdir is mkdir -p before the copy (#37)'() {
        given:
        def inputs = ['db/blast.fa': s3Path('s3://data/db/blast.fa')] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'blast', '', inputs)

        then: 'mkdir and cp both keep ${LOCAL_DIR} outside the quotes so it expands (#41)'
        def mkdirIdx = script.indexOf('mkdir -p "${LOCAL_DIR}/"\'db\'')
        def cpIdx = script.indexOf('''aws s3 cp 's3://data/db/blast.fa' "${LOCAL_DIR}/"'db/blast.fa''')
        mkdirIdx >= 0
        cpIdx > mkdirIdx
    }

    def 'non-s3 inputs are not given a copy command (left to the work-dir sync) (#37)'() {
        given:
        def inputs = ['local.txt': s3Path('file:///some/local/path.txt')] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'cat local.txt', '', inputs)

        then: 'no aws s3 cp for a non-s3 source'
        !script.contains("aws s3 cp 'file://")
    }

    def 'a malformed s3:/// source (empty authority) is repaired to s3:// (#41 bug 1)'() {
        given: 'the nf-amazon Path renders toUri() with an empty authority — bucket lands in the path'
        def inputs = ['reads.fq.gz': s3Path('s3:///my-bucket/work/fetch/SRR059375_1.fastq.gz')] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'fastqc reads.fq.gz', '', inputs)

        then: 'the copy source has a proper bucket authority (two slashes), not three'
        script.contains('''aws s3 cp 's3://my-bucket/work/fetch/SRR059375_1.fastq.gz' "${LOCAL_DIR}/"\'reads.fq.gz\'''')
        !script.contains("aws s3 cp 's3:///")
    }

    def 'normalizeS3Uri collapses an empty authority and leaves a well-formed URI alone (#41)'() {
        expect:
        SpawnTaskHandler.normalizeS3Uri('s3:///bucket/key') == 's3://bucket/key'
        SpawnTaskHandler.normalizeS3Uri('s3://bucket/key') == 's3://bucket/key'
        SpawnTaskHandler.normalizeS3Uri('file:///x') == 'file:///x'
    }

    def 'no input-staging block when there are no declared inputs'() {
        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'echo hi', '')

        then: 'the work-dir sync still runs, but no per-input copies'
        script.contains('aws s3 sync "${WORKDIR_S3}"')
        !script.contains('Localize declared inputs')
    }

    def 'staging script single-quotes values and escapes embedded quotes'() {
        expect:
        SpawnTaskHandler.shellQuote("plain") == "'plain'"
        SpawnTaskHandler.shellQuote("a'b")   == "'a'\\''b'"
        SpawnTaskHandler.shellQuote(null)    == "''"
    }

    def 'completion probe reads <workDir>/.exitcode from S3, not the instance (#34)'() {
        when:
        def cmd = SpawnTaskHandler.buildExitcodeProbeCommand('s3://b/work/ab/cd', 'us-west-2')

        then: 'streams the .exitcode object to stdout via aws s3 cp <uri> -'
        cmd == ['aws', 's3', 'cp', 's3://b/work/ab/cd/.exitcode', '-', '--region', 'us-west-2']
    }

    def 'completion probe normalizes a trailing slash on the work dir URI'() {
        expect:
        SpawnTaskHandler.buildExitcodeProbeCommand('s3://b/work/ab/cd/', 'us-east-1')[3] == 's3://b/work/ab/cd/.exitcode'
        SpawnTaskHandler.buildExitcodeProbeCommand('s3://b/work/ab/cd', 'us-east-1')[3]  == 's3://b/work/ab/cd/.exitcode'
    }

    def 'completion probe defaults the region when unset'() {
        expect:
        SpawnTaskHandler.buildExitcodeProbeCommand('s3://b/x', null).last() == 'us-east-1'
    }

    def 'parseExitCode reads the integer status, tolerating whitespace'() {
        expect:
        SpawnTaskHandler.parseExitCode(content) == expected

        where:
        content     | expected
        '0'         | 0
        '0\n'       | 0
        '  1 '      | 1
        '137\n'     | 137
        '0\nnoise'  | 0
        ''          | null
        '   '       | null
        null        | null
        'notanint'  | null
    }
}
