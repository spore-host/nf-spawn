package io.nextflow.spawn

import spock.lang.Specification

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
