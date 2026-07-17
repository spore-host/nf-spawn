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

    def 'forwards ext.volumes as repeated --attach-volume args (#45)'() {
        when:
        def cmd = SpawnTaskHandler.buildLaunchCommand(
            'n', 'r7g.2xlarge', 'us-east-1', '2h', false, '/s.sh', '', 0,
            ['snap-aaa:/opt/databases/kraken2:ro', 'snap-bbb:/data:rw'])

        then: 'each volume becomes its own --attach-volume <spec>'
        cmd.count { it == '--attach-volume' } == 2
        def i = cmd.indexOf('--attach-volume')
        cmd[i + 1] == 'snap-aaa:/opt/databases/kraken2:ro'

        and: 'no --attach-volume when none given (back-compat default)'
        !SpawnTaskHandler.buildLaunchCommand('n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0).contains('--attach-volume')
    }

    def 'passes --az only when ext.az is set, to pin FSR placement (#62)'() {
        when:
        def withAz = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0, [], 'us-east-1a')
        def withoutAz = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0)

        then: 'an explicit AZ is forwarded as --az <zone>'
        withAz.contains('--az')
        withAz[withAz.indexOf('--az') + 1] == 'us-east-1a'

        and: 'no --az when unset, so spawn keeps its own placement (back-compat default)'
        !withoutAz.contains('--az')
    }

    def 'forwards ext.fsx by id as --fsx-id (+ mount point) only when set (#67)'() {
        when:
        def withFsx = SpawnTaskHandler.buildLaunchCommand(
            'n', 'r7g.2xlarge', 'us-east-1', '2h', false, '/s.sh', '', 0, [], '',
            [id: 'fs-0abc', mount: '/fsx', paths: []])
        def withoutFsx = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0)

        then: 'the filesystem id and mount point are forwarded'
        withFsx.contains('--fsx-id')
        withFsx[withFsx.indexOf('--fsx-id') + 1] == 'fs-0abc'
        withFsx.contains('--fsx-mount-point')
        withFsx[withFsx.indexOf('--fsx-mount-point') + 1] == '/fsx'

        and: 'no --fsx-id when unset, so spawn launch is unchanged (back-compat default)'
        !withoutFsx.contains('--fsx-id')
        !withoutFsx.contains('--fsx-mount-point')
    }

    def 'forwards ext.efs by id as --efs-id (+ mount point) only when set (#67)'() {
        when:
        def withEfs = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '', 0, [], '',
            [:], [id: 'fs-0def', mount: '/efs', paths: []])

        then:
        withEfs.contains('--efs-id')
        withEfs[withEfs.indexOf('--efs-id') + 1] == 'fs-0def'
        withEfs.contains('--efs-mount-point')
        withEfs[withEfs.indexOf('--efs-mount-point') + 1] == '/efs'

        and: 'fsx and efs do not interfere — neither leaks when only the other is set'
        !withEfs.contains('--fsx-id')
    }

    def 'parseSharedFs accepts a bare id string and a map, defaulting the mount (#67)'() {
        expect: 'a bare filesystem-id string → id + default mount'
        SpawnTaskHandler.parseSharedFs('fs-0abc', '/fsx') == [id: 'fs-0abc', mount: '/fsx', paths: []]

        and: 'a map can override the mount and declare symlink paths'
        SpawnTaskHandler.parseSharedFs([id: 'fs-0abc', mount: '/data', paths: ['kraken2', 'metaphlan']], '/fsx') ==
            [id: 'fs-0abc', mount: '/data', paths: ['kraken2', 'metaphlan']]

        and: 'a single string path is normalized to a one-element list'
        SpawnTaskHandler.parseSharedFs([id: 'fs-0abc', path: 'kraken2'], '/fsx') ==
            [id: 'fs-0abc', mount: '/fsx', paths: ['kraken2']]

        and: 'efsId / fsxId key aliases for id are accepted'
        SpawnTaskHandler.parseSharedFs([efsId: 'fs-0def'], '/efs') == [id: 'fs-0def', mount: '/efs', paths: []]

        and: 'null / empty → no shared FS'
        SpawnTaskHandler.parseSharedFs(null, '/fsx') == [:]
        SpawnTaskHandler.parseSharedFs('', '/fsx') == [:]
    }

    def 'parseSharedFs rejects the create form with a pointer to pre-creating by id (#67)'() {
        when:
        SpawnTaskHandler.parseSharedFs([create: true, lifecycle: 'ephemeral', s3Bucket: 'b'], '/fsx')

        then: 'per-task create across a fan-out is a footgun — rejected'
        def e = thrown(nextflow.exception.AbortOperationException)
        e.message.contains('create form is not supported')
    }

    def 'parseSharedFs requires an id in the map form (#67)'() {
        when:
        SpawnTaskHandler.parseSharedFs([mount: '/fsx'], '/fsx')

        then:
        thrown(nextflow.exception.AbortOperationException)
    }

    def 'sharedFsBindMounts bind-mounts the shared FS read-only into the container (#67)'() {
        expect:
        SpawnTaskHandler.sharedFsBindMounts([[id: 'fs-0abc', mount: '/fsx', paths: []]]) ==
            "-v '/fsx:/fsx:ro'"

        and: 'fsx and efs together'
        SpawnTaskHandler.sharedFsBindMounts([
            [id: 'fs-0abc', mount: '/fsx', paths: []],
            [id: 'fs-0def', mount: '/efs', paths: []],
        ]) == "-v '/fsx:/fsx:ro' -v '/efs:/efs:ro'"

        and: 'empty maps contribute nothing'
        SpawnTaskHandler.sharedFsBindMounts([[:], [:]]) == ''
    }

    def 'sharedFsMountPaths exposes <mount>/<name> per declared path, or the bare mount (#67)'() {
        expect: 'declared paths become <mount>/<name> so a stage-name input symlinks zero-copy'
        SpawnTaskHandler.sharedFsMountPaths([[id: 'fs-0abc', mount: '/fsx', paths: ['kraken2', 'metaphlan']]]) ==
            ['/fsx/kraken2', '/fsx/metaphlan']

        and: 'no declared paths → the bare mount (a DB mounted at the root, or single-DB FS)'
        SpawnTaskHandler.sharedFsMountPaths([[id: 'fs-0abc', mount: '/fsx', paths: []]]) == ['/fsx']

        and: 'a trailing slash on the mount is normalized'
        SpawnTaskHandler.sharedFsMountPaths([[id: 'fs-0abc', mount: '/fsx/', paths: ['kraken2']]]) == ['/fsx/kraken2']

        and: 'empty maps contribute nothing'
        SpawnTaskHandler.sharedFsMountPaths([[:]]) == []
    }

    def 'a path input matching a shared-FS db dir is symlinked, not copied — even when Nextflow staged it to s3 (#67/#55)'() {
        given: 'taxprofiler stages the Kraken2 db_path into the S3 work area; the FS holds it at /fsx/kraken2'
        def inputs = ['kraken2': s3Path('s3://bucket/work/classify/stage-abc/70/x/kraken2/')] as Map<String, java.nio.file.Path>
        def mounts = SpawnTaskHandler.sharedFsMountPaths([[id: 'fs-0abc', mount: '/fsx', paths: ['kraken2']]])

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'kraken2', 'img:1', inputs, '', '', mounts)

        then: 'the stage name symlinks to the shared-FS db dir (zero-copy), guarded by existence'
        script.contains('if [ -e \'/fsx/kraken2\' ]; then ln -sfn \'/fsx/kraken2\' "${LOCAL_DIR}/"\'kraken2\';')

        and: 'the huge DB is NOT aws-s3-cp\'d despite the s3:// source'
        !script.contains("aws s3 cp 's3://bucket/work/classify/stage-abc")
        !script.contains('kraken2 --region')
    }

    def 'parseVolumeSpecs maps ext.volumes maps to snap:mount:mode, read-only by default (#45)'() {
        expect:
        SpawnTaskHandler.parseVolumeSpecs([[snapshot: 'snap-aaa', mount: '/ref']]) == ['snap-aaa:/ref:ro']
        SpawnTaskHandler.parseVolumeSpecs([[snapshot: 'snap-aaa', mount: '/ref', readOnly: false]]) == ['snap-aaa:/ref:rw']
        SpawnTaskHandler.parseVolumeSpecs([[snapshot: 'snap-aaa', mount: '/ref', readOnly: true]]) == ['snap-aaa:/ref:ro']

        and: 'a single map (not a list) is accepted'
        SpawnTaskHandler.parseVolumeSpecs([snapshot: 'snap-xyz', mount: '/db']) == ['snap-xyz:/db:ro']

        and: 'multiple volumes preserve order'
        SpawnTaskHandler.parseVolumeSpecs([
            [snapshot: 'snap-1', mount: '/a'],
            [snapshot: 'snap-2', mount: '/b', readOnly: false],
        ]) == ['snap-1:/a:ro', 'snap-2:/b:rw']

        and: 'null / empty → no volumes'
        SpawnTaskHandler.parseVolumeSpecs(null) == []
        SpawnTaskHandler.parseVolumeSpecs([]) == []
    }

    def 'parseVolumeSpecs rejects entries missing snapshot or mount (#45)'() {
        when:
        SpawnTaskHandler.parseVolumeSpecs([[snapshot: 'snap-aaa']])

        then:
        thrown(nextflow.exception.AbortOperationException)
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

        and: 'a failed output sync flips a success exit code so it is not a silent partial success (#59)'
        script.contains('if [ "${TASK_RC}" -eq 0 ]; then TASK_RC=75; fi')

        and: 'ordering: stage-down precedes run; .exitcode is written AFTER the output sync (so a sync failure can flip it) and uploaded last'
        downIdx < script.indexOf('bash .command.sh')
        script.indexOf('echo "${TASK_RC}" > .exitcode') > upOutputsIdx
        script.indexOf('echo "${TASK_RC}" > .exitcode') < upExitIdx

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

    def 'auto-ensures Docker when a container is set, idempotently (#47)'() {
        when:
        def setup = SpawnTaskHandler.buildSetupScript([:], 'biocontainers/fastqc:0.12.1--hdfd78af_0')

        then: 'installs docker only if absent, then starts it'
        setup.contains('command -v docker')
        setup.contains('dnf install -y docker')
        setup.contains('systemctl')
    }

    def 'does not install Docker when there is no container (#47)'() {
        expect:
        SpawnTaskHandler.buildSetupScript([:], '') == ''
        SpawnTaskHandler.buildSetupScript([:], null) == ''
    }

    def 'ext.ensureDocker=false opts out of the Docker install (#47)'() {
        when:
        def setup = SpawnTaskHandler.buildSetupScript([ensureDocker: false], 'some/image:1')

        then: 'no docker install even though a container is set (pre-baked tools AMI)'
        !setup.contains('dnf install -y docker')
    }

    def 'ext.packages installs host tools via dnf (#47)'() {
        when:
        def setup = SpawnTaskHandler.buildSetupScript([packages: ['pigz', 'ethtool']], '')

        then:
        setup.contains("dnf install -y 'pigz' 'ethtool'")
    }

    def 'ext.setup runs an arbitrary bootstrap last (#47)'() {
        when:
        def setup = SpawnTaskHandler.buildSetupScript([setup: 'echo hi && do_thing'], 'img:1')

        then: 'the arbitrary command is included, after the docker ensure'
        setup.contains('echo hi && do_thing')
        setup.indexOf('docker') < setup.indexOf('echo hi && do_thing')
    }

    def 'parsePackages accepts a list or a delimited string (#47)'() {
        expect:
        SpawnTaskHandler.parsePackages(['pigz', 'ethtool']) == ['pigz', 'ethtool']
        SpawnTaskHandler.parsePackages('pigz ethtool') == ['pigz', 'ethtool']
        SpawnTaskHandler.parsePackages('pigz, ethtool') == ['pigz', 'ethtool']
        SpawnTaskHandler.parsePackages(null) == []
        SpawnTaskHandler.parsePackages('') == []
    }

    def 'staging script runs setup before the work-dir sync and the task (#47)'() {
        when:
        def setup = SpawnTaskHandler.buildSetupScript([:], 'img:1')
        def script = SpawnTaskHandler.buildStagingScript(
            's3://b/work/aa/bb', 'us-east-1', 'run', 'img:1', [:], '', setup)

        then: 'docker is ensured before inputs are synced and before the task runs'
        script.contains('dnf install -y docker')
        script.indexOf('dnf install -y docker') < script.indexOf('aws s3 sync "${WORKDIR_S3}"')
        script.indexOf('dnf install -y docker') < script.indexOf('bash .command.sh')
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

    def 'non-s3 inputs are never aws-s3-copied (#37)'() {
        given:
        def inputs = ['local.txt': s3Path('file:///some/local/path.txt')] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'cat local.txt', '', inputs)

        then: 'no aws s3 cp for a non-s3 source'
        !script.contains("aws s3 cp 'file://")
    }

    def 'a local path input is symlinked into the work dir when present on the task (#51)'() {
        given: 'a path input whose source is a local absolute path — e.g. an ext.volumes DB mount'
        def inputs = ['metaphlan_db': s3Path('file:///opt/databases/metaphlan')] as Map<String, java.nio.file.Path>

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'metaphlan', '', inputs)

        then: 'the stage name is symlinked to the mount path, guarded by an existence test — no copy'
        script.contains('if [ -e \'/opt/databases/metaphlan\' ]; then ln -sfn \'/opt/databases/metaphlan\' "${LOCAL_DIR}/"\'metaphlan_db\'; fi')

        and: 'the input itself is not aws-s3-copied (only the .exitcode upload uses aws s3 cp)'
        !script.contains("aws s3 cp 'file://")
        !script.contains('metaphlan_db --region')
    }

    def 'localAbsolutePath extracts a usable local path or returns empty (#51)'() {
        expect:
        SpawnTaskHandler.localAbsolutePath('file:///opt/databases/metaphlan') == '/opt/databases/metaphlan'
        SpawnTaskHandler.localAbsolutePath('/opt/databases/kraken2') == '/opt/databases/kraken2'
        SpawnTaskHandler.localAbsolutePath('file:///x') == '/x'

        and: 'relative / empty / non-absolute → empty (caller skips)'
        SpawnTaskHandler.localAbsolutePath('relative/path') == ''
        SpawnTaskHandler.localAbsolutePath('') == ''
        SpawnTaskHandler.localAbsolutePath(null) == ''
    }

    def 'ext.volumes mount paths are bind-mounted into the task container (#51)'() {
        expect:
        SpawnTaskHandler.volumeBindMounts([[snapshot: 'snap-a', mount: '/opt/databases/metaphlan']]) ==
            "-v '/opt/databases/metaphlan:/opt/databases/metaphlan:ro'"

        and: 'read-write volume drops the :ro suffix'
        SpawnTaskHandler.volumeBindMounts([[snapshot: 'snap-a', mount: '/data', readOnly: false]]) ==
            "-v '/data:/data'"

        and: 'multiple volumes; null → empty'
        SpawnTaskHandler.volumeBindMounts([
            [snapshot: 'snap-a', mount: '/a'],
            [snapshot: 'snap-b', mount: '/b', readOnly: false],
        ]) == "-v '/a:/a:ro' -v '/b:/b'"
        SpawnTaskHandler.volumeBindMounts(null) == ''
    }

    def 'a path input whose stage name matches an ext.volumes mount is symlinked, NOT copied — even when Nextflow staged it to s3 (#55)'() {
        given: 'taxprofiler stages db_path into the S3 work area, so the source is s3://… not the mount'
        def inputs = ['metaphlan': s3Path('s3://bucket/work/classify/stage-abc/70/x/metaphlan/')] as Map<String, java.nio.file.Path>
        def mounts = ['/opt/databases/metaphlan']

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'metaphlan', 'img:1', inputs, '', '', mounts)

        then: 'the stage name is symlinked to the mount (zero-copy), guarded by existence'
        script.contains('if [ -e \'/opt/databases/metaphlan\' ]; then ln -sfn \'/opt/databases/metaphlan\' "${LOCAL_DIR}/"\'metaphlan\';')

        and: 'the huge DB is NOT aws-s3-cp\'d despite the s3:// source'
        !script.contains("aws s3 cp 's3://bucket/work/classify/stage-abc")
        !script.contains('metaphlan --region')
    }

    def 'a non-matching s3 input is still copied when ext.volumes is present (#55)'() {
        given: 'reads are a normal staged input; only the DB matches a mount'
        def inputs = [
            'reads_1.fq.gz': s3Path('s3://data/sra/SRR_1.fq.gz'),
            'metaphlan'    : s3Path('s3://bucket/work/stage/metaphlan/'),
        ] as Map<String, java.nio.file.Path>
        def mounts = ['/opt/databases/metaphlan']

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'run', 'img:1', inputs, '', '', mounts)

        then: 'reads are copied as usual'
        script.contains('''aws s3 cp 's3://data/sra/SRR_1.fq.gz' "${LOCAL_DIR}/"'reads_1.fq.gz''')

        and: 'the DB is symlinked, not copied'
        script.contains('ln -sfn \'/opt/databases/metaphlan\'')
        !script.contains("aws s3 cp 's3://bucket/work/stage/metaphlan")
    }

    def 'volumeMountPaths extracts mount paths; baseName takes the last segment (#55)'() {
        expect:
        SpawnTaskHandler.volumeMountPaths([[snapshot: 'snap-a', mount: '/opt/databases/metaphlan'], [snapshot: 'snap-b', mount: '/data']]) ==
            ['/opt/databases/metaphlan', '/data']
        SpawnTaskHandler.volumeMountPaths(null) == []

        and:
        SpawnTaskHandler.baseName('/opt/databases/metaphlan') == 'metaphlan'
        SpawnTaskHandler.baseName('/opt/databases/metaphlan/') == 'metaphlan'
        SpawnTaskHandler.baseName('kraken2') == 'kraken2'
        SpawnTaskHandler.baseName('') == ''
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

    def 'killTask uses spawn terminate (not cancel) with -y (#58)'() {
        when:
        def cmd = SpawnTaskHandler.buildTerminateCommand('nf-abc123')

        then: 'terminate destroys the instance; cancel only cancels a sweep'
        cmd == ['spawn', 'terminate', 'nf-abc123', '-y']
        cmd[1] == 'terminate'
        cmd.contains('-y')
        !cmd.contains('cancel')
        // region is intentionally NOT a flag — terminate reads AWS_REGION, which
        // killTask sets on the subprocess environment.
        !cmd.contains('--region')
    }

    def 'validateExtDirectives accepts well-formed values (#59)'() {
        when:
        SpawnTaskHandler.validateExtDirectives('c7i.2xlarge', 'us-east-1', 'us-east-1a', '2h', 'ami-0abc123def')

        then:
        noExceptionThrown()
    }

    def 'validateExtDirectives accepts empty/defaulted az and ami (#59)'() {
        when: 'empty ami/az are the unset case and must not error'
        SpawnTaskHandler.validateExtDirectives('t3.medium', 'us-west-2', '', '90m', '')

        then:
        noExceptionThrown()
    }

    def 'validateExtDirectives rejects malformed #field (#59)'() {
        when:
        SpawnTaskHandler.validateExtDirectives(instanceType, region, az, ttl, ami)

        then:
        def e = thrown(nextflow.exception.AbortOperationException)
        e.message.contains(field)

        where:
        field              | instanceType | region      | az          | ttl   | ami
        'ext.instanceType' | 'c7i;rm -rf' | 'us-east-1' | ''          | '2h'  | ''
        'ext.instanceType' | 'notatype'   | 'us-east-1' | ''          | '2h'  | ''
        'ext.region'       | 't3.medium'  | 'useast1'   | ''          | '2h'  | ''
        'ext.az'           | 't3.medium'  | 'us-east-1' | 'us-east-1' | '2h'  | ''
        'ext.ttl'          | 't3.medium'  | 'us-east-1' | ''          | '2 h' | ''
        'ext.ttl'          | 't3.medium'  | 'us-east-1' | ''          | 'foo' | ''
        'ext.ami'          | 't3.medium'  | 'us-east-1' | ''          | '2h'  | 'ami-XYZ'
        'ext.ami'          | 't3.medium'  | 'us-east-1' | ''          | '2h'  | 'notanami'
    }

    def 'staging script only world-writes the work dir for a container task (#59)'() {
        when: 'bare-OS task (no container) runs as the dir owner — no chmod 0777'
        def bare = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'echo hi', '')

        then:
        !bare.contains('chmod 0777')

        when: 'containerized task needs the dir world-writable for a non-root image user'
        def ctr = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'echo hi', 'img:1')

        then:
        ctr.contains('chmod 0777 "${LOCAL_DIR}"')
    }

    def 'buildInputStaging logs the basename-match substitution so a wrong match is visible (#59)'() {
        given:
        Map<String, Path> inputs = ['kraken2': java.nio.file.Paths.get('/tmp/kraken2')]
        def mounts = ['/opt/databases/kraken2']

        when:
        def script = SpawnTaskHandler.buildStagingScript('s3://b/work/aa/bb', 'us-east-1', 'kraken2', 'img:1', inputs, '', '', mounts)

        then: 'the symlink substitution is announced to stderr'
        script.contains("input 'kraken2'")
        script.contains('basename match')
    }
}
