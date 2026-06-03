package io.nextflow.spawn

import spock.lang.Specification

class SpawnTaskHandlerTest extends Specification {

    def 'passes the task script via --user-data-file, not a bare --user-data path (#13)'() {
        when:
        def cmd = SpawnTaskHandler.buildLaunchCommand(
            'nf-abc123', 't3.medium', 'us-east-1', '2h', false, '/tmp/nf-spawn-abc.sh', '')

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
        SpawnTaskHandler.buildLaunchCommand('n', 't3.medium', 'us-east-1', '2h', spot, '/s.sh', '')
            .contains('--spot') == expected

        where:
        spot  | expected
        true  | true
        false | false
    }

    def 'passes --ami only when ext.ami is set, to avoid SSM auto-detect (#18 / spawn#38)'() {
        when:
        def withAmi = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', 'ami-0123456789abcdef0')
        def withoutAmi = SpawnTaskHandler.buildLaunchCommand(
            'n', 't3.medium', 'us-east-1', '2h', false, '/s.sh', '')

        then: 'an explicit AMI is forwarded as --ami <id>'
        withAmi.contains('--ami')
        withAmi[withAmi.indexOf('--ami') + 1] == 'ami-0123456789abcdef0'

        and: 'no --ami when unset, so spawn keeps its own auto-detect behavior'
        !withoutAmi.contains('--ami')
    }

    def 'staging script syncs the S3 work dir down, runs the task, and syncs results back (#14)'() {
        given:
        def workDir = 's3://my-bucket/work/ab/cdef0123456789'

        when:
        def script = SpawnTaskHandler.buildStagingScript(workDir, 'us-west-2', 'echo hello > out.txt')

        then: 'inputs are staged down from the S3 work dir before the task runs'
        def downIdx = script.indexOf('aws s3 sync "${WORKDIR_S3}" "${LOCAL_DIR}/"')
        downIdx >= 0

        and: 'the task script is materialized and run, capturing the real exit code'
        script.contains('echo hello > out.txt')
        script.contains('bash .command.sh 1>.command.out 2>.command.err')
        script.contains('echo $? > .exitcode')

        and: 'outputs sync back up (excluding .exitcode), then .exitcode is uploaded last'
        def upOutputsIdx = script.indexOf('aws s3 sync "${LOCAL_DIR}/" "${WORKDIR_S3}" --region "${AWS_REGION}" --exclude ".exitcode"')
        def upExitIdx    = script.indexOf('aws s3 cp .exitcode')
        upOutputsIdx >= 0
        upExitIdx > upOutputsIdx

        and: 'ordering: stage-down precedes run precedes stage-up'
        downIdx < script.indexOf('bash .command.sh')
        script.indexOf('echo $? > .exitcode') < upOutputsIdx

        and: 'the work dir URI and region are injected, single-quoted'
        script.contains("WORKDIR_S3='s3://my-bucket/work/ab/cdef0123456789'")
        script.contains("AWS_REGION='us-west-2'")
    }

    def 'staging script single-quotes values and escapes embedded quotes'() {
        expect:
        SpawnTaskHandler.shellQuote("plain") == "'plain'"
        SpawnTaskHandler.shellQuote("a'b")   == "'a'\\''b'"
        SpawnTaskHandler.shellQuote(null)    == "''"
    }
}
